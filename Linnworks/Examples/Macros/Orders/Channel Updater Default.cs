// UW Home Default Channel Updater (Refactored)
// =============================================
// 
// SUMMARY:   
// This Linnworks macro processes orders and generates CSV files for various notification types,
// uploading them via SFTP or saving locally based on the outputMethod parameter.
// After successful file output, the corresponding ExtendedProperty is set to TRUE for each order.
//
// FUNCTIONALITY:
// 1. notifyAcknowledge (Type: OpenAcknowledge)
//    - Returns ALL open orders (not restricted to any folder) filtered by subSource
//    - Only includes orders where ExtendedProperty 'ChannelUpdatesRequired' is TRUE
//    - Only includes orders where 'statusACK' is FALSE or not set
//    - Excludes parked, unpaid, and locked orders (Status=1 means PAID)
//    - After processing, ONLY moves orders that are in the newFolder to 'Updated' folder
//    - Orders from other folders remain in their current folder but still get processed
//    - After processing, sets ExtendedProperty 'statusACK' to TRUE for ALL processed orders
//
// 2. notifyOOS (Type: Open)
//    - Returns open orders in the specified oosFolder filtered by subSource
//    - Only includes orders where ExtendedProperty 'ChannelUpdatesRequired' is TRUE
//    - Only includes orders where 'statusOOS' is FALSE or not set
//    - Excludes parked, unpaid, and locked orders
//    - NO folder move after processing
//    - After processing, sets ExtendedProperty 'statusOOS' to TRUE
//
// 3. notifyBIS (Type: Open)
//    - Returns open orders in the specified bisFolder filtered by subSource
//    - Only includes orders where ExtendedProperty 'ChannelUpdatesRequired' is TRUE
//    - Only includes orders where 'statusBIS' is FALSE or not set
//    - Excludes parked, unpaid, and locked orders
//    - After processing, moves ALL processed orders to 'Updated' folder
//    - After processing, sets ExtendedProperty 'statusBIS' to TRUE
//
// 4. notifyShipped (Type: Shipped)
//    - Returns processed/shipped orders within lookBackDays (searches by processed date)
//    - Filtered by subSource and Source
//    - Only includes orders where ExtendedProperty 'ChannelUpdatesRequired' is TRUE
//    - Only includes orders where 'StatusASN' is FALSE or not set
//    - Excludes orders already in 'Completed' folder
//    - NO folder move after processing
//    - After processing, sets ExtendedProperty 'StatusASN' to TRUE
//
// 5. actionCancelled (Type: OpenNoEPFilter)
//    - Returns open orders in the cancelFolder filtered by subSource
//    - NO ExtendedProperty filtering applied (no ChannelUpdatesRequired check)
//    - NO statusEP filter applied
//    - Excludes parked, unpaid, and locked orders
//    - After processing, CANCELS all orders (calls Api.Orders.CancelOrder)
//    - NO ExtendedProperty set after processing
//
// 6. notifyCancelled (Type:  Cancelled)
//    - Returns cancelled orders within lookBackDays (searches by cancelled date)
//    - Filtered by subSource and Source
//    - Only includes orders where ExtendedProperty 'ChannelUpdatesRequired' is TRUE
//    - Only includes orders where 'StatusCANC' is FALSE or not set
//    - Excludes orders already in 'Completed' folder
//    - NO folder move after processing
//    - After processing, sets ExtendedProperty 'StatusCANC' to TRUE
//
// REFACTORING NOTES:
// - Consolidated three duplicate GetFilteredOpenOrders methods into one unified method
// - Batch fetching for processed orders using GetOrdersById instead of individual calls
// - Reduced verbose logging (use DEBUG flag for detailed logging)
// - Introduced configuration class for cleaner code
// - Extracted constants for magic strings
// - Simplified CSV generation and switch statements

using System;
using System.Collections. Generic;
using System. Linq;
using System.Text;
using LinnworksAPI;
using LinnworksMacroHelpers. Classes;
using System.ComponentModel;
using System.IO;

namespace LinnworksMacro
{
    public class LinnworksMacro :  LinnworksMacroHelpers.LinnworksMacroBase
    {
        #region Constants

        private static class Constants
        {
            public const string UpdatedFolder = "Updated";
            public const string CompletedFolder = "Completed";
            public const string ChannelUpdatesRequired = "ChannelUpdatesRequired";
            public const string TrueValue = "TRUE";
            public const int BatchSize = 200;
            
            public static class EPNames
            {
                public const string StatusACK = "statusACK";
                public const string StatusOOS = "statusOOS";
                public const string StatusBIS = "statusBIS";
                public const string StatusASN = "StatusASN";
                public const string StatusCANC = "StatusCANC";
            }
        }

        private static readonly string StandardCsvHeader = string.Join(",",
            "Linnworks Order Number", "Reference Num", "Secondary Ref", "External Ref", "Primary PO Field",
            "JDE Order Number", "Sold To Account", "Received Date", "Source", "Sub Source", "Despatch By Date",
            "Number Order Items", "Postal Service Name", "Total Order Weight", "Tracking Number",
            "Item > SKU", "Item > ChannelSKU", "Item > Description", "Item > Quantity", "Item > Line Ref",
            "Item > Item Cost (ex VAT)", "Item > Item Discount (ex VAT)", "Item > Tax Rate", "Item > Weight Per Item");

        private static readonly Dictionary<string, string> FilePrefixMap = new Dictionary<string, string>
        {
            { "notifyAcknowledge", "Acknowledge" },
            { "notifyOOS", "OOS" },
            { "notifyBIS", "BIS" },
            { "notifyShipped", "Shipped" },
            { "actionCancelled", "ActionCancelled" },
            { "notifyCancelled", "Cancelled" }
        };

        #endregion

        #region Configuration Classes

        private enum NotificationType
        {
            OpenAcknowledge,
            Open,
            OpenNoEPFilter,
            Shipped,
            Cancelled
        }

        private class NotificationConfig
        {
            public string NotifyFlag { get; set; }
            public string NotifyType { get; set; }
            public string Folder { get; set; }
            public string Directory { get; set; }
            public NotificationType Type { get; set; }
            public string EPFilter { get; set; }
            public string EPUpdate { get; set; }
            public bool RequiresChannelUpdates { get; set; }

            public bool IsEnabled
            {
                get { return string.Equals(NotifyFlag, Constants.TrueValue, StringComparison.OrdinalIgnoreCase); }
            }
        }

        private class ProcessingContext
        {
            public string Source { get; set; }
            public string SubSource { get; set; }
            public string SortField { get; set; }
            public string SortDirection { get; set; }
            public int LookBackDays { get; set; }
            public string LocalFilePath { get; set; }
            public string FileType { get; set; }
            public string NewFolder { get; set; }
            public string OutputMethod { get; set; }
            public SFtpSettings SftpSettings { get; set; }
            public string SftpFolderRoot { get; set; }
        }

        private class CsvResult
        {
            public StringBuilder Csv { get; set; }
            public string FileName { get; set; }
            public Guid[] OrderIds { get; set; }
            public Guid[] NewFolderOrderIds { get; set; }
        }

        #endregion

        #region Main Execute Method

        public void Execute(
            string Source,
            string subSource,
            string notifyAcknowledge,
            string notifyOOS,
            string notifyBIS,
            string notifyShipped,
            string actionCancelled,
            string notifyCancelled,
            string newFolder,
            string oosFolder,
            string bisFolder,
            string cancelFolder,
            string SFTPServer,
            int SFTPPort,
            string SFTPUsername,
            string SFTPPassword,
            string SFTPFolderRoot,
            string acknowledgeDirectory,
            string oosDirectory,
            string bisDirectory,
            string shippedDirectory,
            string actionCancelledDirectory,
            string cancelDirectory,
            string filetype,
            string sortField,
            string sortDirection,
            int lookBackDays,
            string localFilePath,
            string outputMethod)
        {
            LogExecutionStart(Source, subSource, outputMethod);

            var context = new ProcessingContext
            {
                Source = Source,
                SubSource = subSource,
                SortField = sortField,
                SortDirection = sortDirection,
                LookBackDays = lookBackDays,
                LocalFilePath = localFilePath,
                FileType = filetype,
                NewFolder = newFolder,
                OutputMethod = outputMethod,
                SftpFolderRoot = SFTPFolderRoot,
                SftpSettings = new SFtpSettings
                {
                    UserName = SFTPUsername,
                    Password = SFTPPassword,
                    Server = SFTPServer. StartsWith("sftp://") ? SFTPServer.Substring(7) : SFTPServer,
                    Port = SFTPPort
                }
            };

            var configs = BuildNotificationConfigs(
                notifyAcknowledge, notifyOOS, notifyBIS, notifyShipped, actionCancelled, notifyCancelled,
                newFolder, oosFolder, bisFolder, cancelFolder,
                acknowledgeDirectory, oosDirectory, bisDirectory, shippedDirectory, actionCancelledDirectory, cancelDirectory);

            foreach (var config in configs. Where(c => c.IsEnabled))
            {
                ProcessNotification(config, context);
            }

            Logger.WriteInfo(string.Format("Macro export complete at:  {0:yyyy-MM-dd HH: mm:ss}", DateTime.Now));
        }

        #endregion

        #region Notification Processing

        private void ProcessNotification(NotificationConfig config, ProcessingContext ctx)
        {
            Logger.WriteInfo(string.Format("Processing {0}...", config.NotifyType));

            try
            {
                // 1. Fetch orders
                var orders = FetchOrders(config, ctx);
                if (orders == null || orders.Count == 0)
                {
                    Logger.WriteInfo(string.Format("No orders found for {0}", config.NotifyType));
                    return;
                }

                Logger.WriteInfo(string.Format("Found {0} orders for {1}", orders.Count, config.NotifyType));

                // 2. Generate CSV
                var csvResult = GenerateCsv(orders, config, ctx);

                // 3. Output (FTP or Local)
                bool outputSuccess;
                string emailSubject;
                string emailBody;
                OutputCsv(csvResult, config, ctx, out outputSuccess, out emailSubject, out emailBody);

                // 4. Post-processing (folder moves, EP updates, cancellations)
                if (outputSuccess)
                {
                    ProcessPostOutputActions(config, csvResult);
                }

                // 5. Send notification email
                SendNotificationEmail(emailSubject, emailBody);
            }
            catch (Exception ex)
            {
                Logger. WriteError(string.Format("Error processing {0}: {1}", config.NotifyType, ex.Message));
                SendNotificationEmail(
                    string.Format("Error Processing {0}", config.NotifyType),
                    string.Format("An error occurred:  {0}", ex.Message));
            }
        }

        private List<OrderDetails> FetchOrders(NotificationConfig config, ProcessingContext ctx)
        {
            switch (config.Type)
            {
                case NotificationType.OpenAcknowledge:
                    return GetFilteredOpenOrders(ctx. SubSource, null, ctx.SortField, ctx.SortDirection, config.EPFilter, false, true);
                case NotificationType.Open:
                    return GetFilteredOpenOrders(ctx.SubSource, config. Folder, ctx.SortField, ctx.SortDirection, config. EPFilter, false, true);
                case NotificationType.OpenNoEPFilter:
                    return GetFilteredOpenOrders(ctx.SubSource, config.Folder, ctx.SortField, ctx.SortDirection, null, true, false);
                case NotificationType. Shipped:
                    return GetFilteredProcessedOrders(ctx.SubSource, ctx.Source, ctx.SortField, ctx.SortDirection, ctx.LookBackDays, true, config.EPFilter);
                case NotificationType. Cancelled:
                    return GetFilteredProcessedOrders(ctx. SubSource, ctx.Source, ctx.SortField, ctx.SortDirection, ctx.LookBackDays, false, config.EPFilter);
                default:
                    return new List<OrderDetails>();
            }
        }

        private CsvResult GenerateCsv(List<OrderDetails> orders, NotificationConfig config, ProcessingContext ctx)
        {
            string filePrefix;
            if (! FilePrefixMap.TryGetValue(config.NotifyType, out filePrefix))
            {
                filePrefix = "Unknown";
            }
            string trackingFolder = config.NotifyType == "notifyAcknowledge" ? ctx.NewFolder : null;
            
            return FormatStandardCsv(orders, filePrefix, ctx.LocalFilePath, ctx.FileType, ctx.SubSource, trackingFolder);
        }

        private void OutputCsv(CsvResult result, NotificationConfig config, ProcessingContext ctx,
            out bool outputSuccess, out string emailSubject, out string emailBody)
        {
            outputSuccess = false;
            emailSubject = "";
            emailBody = "";

            if (string.Equals(ctx.OutputMethod, "FTP", StringComparison.OrdinalIgnoreCase))
            {
                var sftpSettings = new SFtpSettings
                {
                    UserName = ctx. SftpSettings.UserName,
                    Password = ctx.SftpSettings.Password,
                    Server = ctx.SftpSettings.Server,
                    Port = ctx.SftpSettings.Port,
                    FullPath = string.Format("{0}/{1}/{2}", ctx.SftpFolderRoot, config.Directory, result.FileName)
                };

                Logger.WriteInfo(string.Format("Uploading to SFTP: {0}", sftpSettings.FullPath));

                if (SendByFTP(result.Csv, sftpSettings))
                {
                    Logger.WriteInfo("SUCCESS: CSV uploaded to SFTP");
                    outputSuccess = true;
                    emailSubject = string.Format("SFTP Upload Successful for {0}", config. Folder);
                    emailBody = string.Format("The CSV file '{0}' was successfully uploaded to SFTP at '{1}'.", result.FileName, sftpSettings.FullPath);
                }
                else
                {
                    Logger.WriteError("FAILED: Could not upload CSV to SFTP");
                    emailSubject = string.Format("SFTP Upload Failed for {0}", config.Folder);
                    emailBody = string.Format("The CSV file '{0}' could not be uploaded to SFTP. Please check the logs for details.", result.FileName);
                }
            }
            else
            {
                string fullLocalPath = Path.Combine(ctx.LocalFilePath, result.FileName);
                Logger.WriteInfo(string. Format("CSV saved locally at: {0}", fullLocalPath));
                outputSuccess = true;
                emailSubject = string.Format("Local Save Successful for {0}", config. Folder);
                emailBody = string.Format("The CSV file '{0}' was successfully saved locally at '{1}'.", result.FileName, fullLocalPath);
            }
        }

        private void ProcessPostOutputActions(NotificationConfig config, CsvResult result)
        {
            Logger.WriteInfo(string.Format("Processing post-output actions for {0}...", config.NotifyType));

            // Handle folder moves and cancellations
            var orderIdsToProcess = config.NotifyType == "notifyAcknowledge" 
                ? result.NewFolderOrderIds 
                : result.OrderIds;

            if (orderIdsToProcess != null && orderIdsToProcess.Length > 0)
            {
                if (config.NotifyType == "notifyAcknowledge" || config.NotifyType == "notifyBIS")
                {
                    MoveOrdersToFolder(orderIdsToProcess, Constants.UpdatedFolder);
                }
                else if (config.NotifyType == "actionCancelled")
                {
                    CancelOrders(orderIdsToProcess);
                }
            }

            // Handle EP updates
            if (! string.IsNullOrEmpty(config.EPUpdate) && result.OrderIds != null && result.OrderIds.Length > 0)
            {
                SetOrderExtendedProperty(result.OrderIds, config.EPUpdate, Constants.TrueValue);
            }
        }

        #endregion

        #region Order Fetching (Unified Method)

        private List<OrderDetails> GetFilteredOpenOrders(
            string subSource, 
            string folderName, 
            string sortField, 
            string sortDirection,
            string epFilterName,
            bool skipAllEPFilters,
            bool requireChannelUpdates)
        {
            Logger.WriteInfo(string.Format("GetFilteredOpenOrders: folder={0}, epFilter={1}, skipEP={2}",
                folderName ??  "ALL", epFilterName ?? "NONE", skipAllEPFilters));

            try
            {
                var filter = BuildOpenOrderFilter(subSource, folderName);
                var sorting = BuildSorting(sortField, sortDirection);

                var guids = Api.Orders.GetAllOpenOrders(filter, sorting, Guid.Empty, "");
                Logger.WriteInfo(string.Format("Found {0} order GUIDs", guids.Count));

                if (guids.Count == 0)
                    return new List<OrderDetails>();

                // Batch fetch order details
                var orders = LoadOrderDetailsBatched(guids);

                // Apply EP filters unless skipped
                if (!skipAllEPFilters)
                {
                    if (requireChannelUpdates)
                    {
                        int beforeFilter = orders.Count;
                        orders = orders.Where(o => HasChannelUpdatesRequired(o)).ToList();
                        Logger.WriteInfo(string. Format("After ChannelUpdatesRequired filter: {0} -> {1}", beforeFilter, orders. Count));
                    }

                    if (! string.IsNullOrEmpty(epFilterName))
                    {
                        int beforeFilter = orders.Count;
                        orders = orders.Where(o => ! HasStatusEPSetToTrue(o, epFilterName)).ToList();
                        Logger. WriteInfo(string.Format("After {0} filter: {1} -> {2}", epFilterName, beforeFilter, orders.Count));
                    }
                }

                return ApplySorting(orders, sortField, sortDirection);
            }
            catch (Exception ex)
            {
                Logger.WriteError(string.Format("Error in GetFilteredOpenOrders: {0}", ex.Message));
                throw;
            }
        }

        private List<OrderDetails> GetFilteredProcessedOrders(
            string subSource, 
            string source, 
            string sortField, 
            string sortDirection, 
            int lookBackDays, 
            bool isShipped, 
            string epFilterName)
        {
            Logger.WriteInfo(string.Format("GetFilteredProcessedOrders: isShipped={0}, epFilter={1}", isShipped, epFilterName));

            try
            {
                DateTime toDate = DateTime. UtcNow.Date.AddDays(1);
                DateTime fromDate = DateTime.UtcNow.Date.AddDays(-lookBackDays);

                var searchFilters = new List<SearchFilters>
                {
                    new SearchFilters { SearchField = SearchFieldTypes.SubSource, SearchTerm = subSource },
                    new SearchFilters { SearchField = SearchFieldTypes.Source, SearchTerm = source }
                };

                var request = new SearchProcessedOrdersRequest
                {
                    SearchTerm = "",
                    SearchFilters = searchFilters,
                    DateField = isShipped ? DateField.processed : DateField.cancelled,
                    FromDate = fromDate,
                    ToDate = toDate,
                    PageNumber = 1,
                    ResultsPerPage = 200
                };

                var response = Api.ProcessedOrders.SearchProcessedOrders(request);
                int responseCount = (response.ProcessedOrders != null && response.ProcessedOrders.Data != null) 
                    ? response.ProcessedOrders.Data.Count 
                    : 0;
                Logger.WriteInfo(string.Format("SearchProcessedOrders returned {0} orders", responseCount));

                if (response.ProcessedOrders == null || response.ProcessedOrders.Data == null || ! response.ProcessedOrders.Data.Any())
                    return new List<OrderDetails>();

                // Batch fetch all order details at once instead of one-by-one
                var orderGuids = response.ProcessedOrders.Data.Select(po => po.pkOrderID).ToList();
                var allOrders = LoadOrderDetailsBatched(orderGuids);

                // Filter out completed orders
                var orders = allOrders
                    .Where(o => o.FolderName == null || ! o.FolderName.Contains(Constants.CompletedFolder))
                    .ToList();

                Logger.WriteInfo(string.Format("After Completed folder filter: {0} -> {1}", allOrders.Count, orders.Count));

                // Apply EP filters
                int beforeChannelFilter = orders.Count;
                orders = orders.Where(o => HasChannelUpdatesRequired(o)).ToList();
                Logger.WriteInfo(string.Format("After ChannelUpdatesRequired filter: {0} -> {1}", beforeChannelFilter, orders.Count));

                if (!string.IsNullOrEmpty(epFilterName))
                {
                    int beforeStatusFilter = orders.Count;
                    orders = orders.Where(o => !HasStatusEPSetToTrue(o, epFilterName)).ToList();
                    Logger. WriteInfo(string.Format("After {0} filter: {1} -> {2}", epFilterName, beforeStatusFilter, orders.Count));
                }

                return ApplySorting(orders, sortField, sortDirection);
            }
            catch (Exception ex)
            {
                Logger.WriteError(string.Format("Error in GetFilteredProcessedOrders: {0}", ex.Message));
                throw;
            }
        }

        private List<OrderDetails> LoadOrderDetailsBatched(List<Guid> orderIds)
        {
            var orders = new List<OrderDetails>();

            if (orderIds. Count > Constants.BatchSize)
            {
                for (int i = 0; i < orderIds.Count; i += Constants.BatchSize)
                {
                    var batch = orderIds.Skip(i).Take(Constants.BatchSize).ToList();
                    Logger.WriteInfo(string.Format("Fetching order details batch {0}:  {1} orders", (i / Constants.BatchSize) + 1, batch.Count));
                    orders.AddRange(Api.Orders.GetOrdersById(batch));
                }
            }
            else if (orderIds.Count > 0)
            {
                orders = Api.Orders.GetOrdersById(orderIds);
            }

            return orders;
        }

        #endregion

        #region Filter & Sorting Builders

        private FieldsFilter BuildOpenOrderFilter(string subSource, string folderName)
        {
            var filter = new FieldsFilter
            {
                NumericFields = new List<NumericFieldFilter>
                {
                    new NumericFieldFilter { FieldCode = FieldCode. GENERAL_INFO_STATUS, Type = NumericFieldFilterType.Equal, Value = 1 },
                    new NumericFieldFilter { FieldCode = FieldCode.GENERAL_INFO_PARKED, Type = NumericFieldFilterType. Equal, Value = 0 },
                    new NumericFieldFilter { FieldCode = FieldCode.GENERAL_INFO_LOCKED, Type = NumericFieldFilterType.Equal, Value = 0 }
                },
                TextFields = new List<TextFieldFilter>
                {
                    new TextFieldFilter { FieldCode = FieldCode.GENERAL_INFO_SUBSOURCE, Text = subSource, Type = TextFieldFilterType.Equal }
                }
            };

            if (! string.IsNullOrEmpty(folderName))
            {
                filter.ListFields = new List<ListFieldFilter>
                {
                    new ListFieldFilter { FieldCode = FieldCode.FOLDER, Value = folderName, Type = ListFieldFilterType.Is }
                };
            }

            return filter;
        }

        private List<FieldSorting> BuildSorting(string sortField, string sortDirection)
        {
            FieldCode sortingCode = (sortField != null && sortField.ToUpperInvariant() == "REFERENCE")
                ? FieldCode. GENERAL_INFO_REFERENCE_NUMBER
                : FieldCode. GENERAL_INFO_ORDER_ID;

            ListSortDirection sortingDirection = (sortDirection != null && sortDirection.ToUpperInvariant() == "ASCENDING")
                ? ListSortDirection.Ascending
                : ListSortDirection.Descending;

            return new List<FieldSorting>
            {
                new FieldSorting { FieldCode = sortingCode, Direction = sortingDirection, Order = 0 }
            };
        }

        private List<OrderDetails> ApplySorting(List<OrderDetails> orders, string sortField, string sortDirection)
        {
            bool ascending = sortDirection != null && sortDirection.ToUpperInvariant() == "ASCENDING";
            bool byOrderId = sortField == null || sortField. ToUpperInvariant() != "REFERENCE";

            if (byOrderId)
            {
                return ascending 
                    ? orders.OrderBy(o => o.NumOrderId).ToList() 
                    : orders.OrderByDescending(o => o.NumOrderId).ToList();
            }
            else
            {
                return ascending 
                    ? orders. OrderBy(o => o.GeneralInfo.ReferenceNum).ToList() 
                    : orders.OrderByDescending(o => o.GeneralInfo. ReferenceNum).ToList();
            }
        }

        #endregion

        #region Extended Property Helpers

        private bool HasChannelUpdatesRequired(OrderDetails order)
        {
            string value = GetExtendedPropertyValue(order. ExtendedProperties, Constants.ChannelUpdatesRequired);
            return value. Equals(Constants.TrueValue, StringComparison.OrdinalIgnoreCase);
        }

        private bool HasStatusEPSetToTrue(OrderDetails order, string epName)
        {
            if (string.IsNullOrEmpty(epName)) return false;
            string value = GetExtendedPropertyValue(order.ExtendedProperties, epName);
            return value.Equals(Constants.TrueValue, StringComparison.OrdinalIgnoreCase);
        }

        private string GetExtendedPropertyValue(List<ExtendedProperty> props, string name)
        {
            if (props == null) return "";
            var prop = props.FirstOrDefault(ep => string.Equals(ep.Name, name, StringComparison.OrdinalIgnoreCase));
            return prop != null && prop.Value != null ? prop. Value :  "";
        }

        private void SetOrderExtendedProperty(Guid[] orderIds, string propertyName, string propertyValue)
        {
            if (orderIds == null || orderIds.Length == 0) return;

            Logger.WriteInfo(string.Format("Setting EP '{0}' = '{1}' for {2} orders", propertyName, propertyValue, orderIds.Length));

            int successCount = 0;
            int errorCount = 0;

            foreach (var orderId in orderIds)
            {
                try
                {
                    var existingProperties = Api.Orders.GetExtendedProperties(orderId);
                    if (existingProperties == null)
                    {
                        existingProperties = new List<ExtendedProperty>();
                    }
                    var updatedProperties = UpdateOrAddProperty(existingProperties, propertyName, propertyValue);
                    Api.Orders.SetExtendedProperties(orderId, updatedProperties);
                    successCount++;
                }
                catch (Exception ex)
                {
                    errorCount++;
                    Logger.WriteError(string.Format("Failed to set EP for order {0}: {1}", orderId, ex.Message));
                }
            }

            Logger. WriteInfo(string.Format("EP update complete.  Success: {0}, Errors: {1}", successCount, errorCount));
        }

        private ExtendedProperty[] UpdateOrAddProperty(List<ExtendedProperty> existing, string name, string value)
        {
            var properties = existing.ToList();
            var existingProp = properties.FirstOrDefault(p => string.Equals(p. Name, name, StringComparison.OrdinalIgnoreCase));

            if (existingProp != null)
            {
                existingProp.Value = value;
            }
            else
            {
                properties.Add(new ExtendedProperty
                {
                    RowId = Guid.Empty,
                    Name = name,
                    Value = value,
                    Type = "Order"
                });
            }

            return properties.ToArray();
        }

        #endregion

        #region Order Actions

        private void MoveOrdersToFolder(Guid[] orderIds, string folderName)
        {
            if (orderIds == null || orderIds.Length == 0) return;

            try
            {
                Logger.WriteInfo(string.Format("Moving {0} orders to '{1}' folder", orderIds.Length, folderName));
                Api.Orders.AssignToFolder(orderIds. ToList(), folderName);
                Logger.WriteInfo(string.Format("Successfully moved orders to '{0}'", folderName));
            }
            catch (Exception ex)
            {
                Logger.WriteError(string.Format("Error moving orders to folder: {0}", ex.Message));
            }
        }

        private void CancelOrders(Guid[] orderIds)
        {
            if (orderIds == null || orderIds.Length == 0) return;

            Logger.WriteInfo(string.Format("Cancelling {0} orders.. .", orderIds.Length));
            int successCount = 0;
            int errorCount = 0;

            foreach (var orderId in orderIds)
            {
                try
                {
                    Api.Orders.CancelOrder(orderId, Guid.Empty, 0, "Cancelled via actionCancelled macro");
                    successCount++;
                }
                catch (Exception ex)
                {
                    errorCount++;
                    Logger.WriteError(string.Format("Error cancelling order {0}: {1}", orderId, ex.Message));
                }
            }

            Logger.WriteInfo(string.Format("Cancellation complete. Success: {0}, Errors: {1}", successCount, errorCount));
        }

        #endregion

        #region CSV Generation

        private CsvResult FormatStandardCsv(List<OrderDetails> orders, string filePrefix, string localFilePath, string filetype, string subSource, string newFolder)
        {
            string sanitizedSubSource = SanitizeForFileName(subSource);
            string sanitizedFiletype = SanitizeForFileName(filetype);
            string sanitizedPrefix = SanitizeForFileName(filePrefix);
            string fileName = string.Format("{0}_Orders_{1}_{2:yyyyMMddHHmmss}_{3}.csv", 
                sanitizedSubSource, sanitizedPrefix, DateTime. Now, sanitizedFiletype);

            var csv = new StringBuilder();
            csv.AppendLine(StandardCsvHeader);

            Guid[] orderIds = orders.Select(o => o.OrderId).ToArray();
            Guid[] newFolderOrderIds = new Guid[0];

            if (! string.IsNullOrEmpty(newFolder))
            {
                newFolderOrderIds = orders
                    .Where(o => o.FolderName != null && o.FolderName.Any(f => string.Equals(f, newFolder, StringComparison.OrdinalIgnoreCase)))
                    .Select(o => o.OrderId)
                    .ToArray();

                Logger.WriteInfo(string.Format("Orders in '{0}' to be moved:  {1}", newFolder, newFolderOrderIds.Length));
            }

            // Generate CSV lines - cache EP lookups per order
            foreach (var order in orders)
            {
                var epCache = BuildEPCache(order. ExtendedProperties);
                foreach (var item in order.Items)
                {
                    csv.AppendLine(GenerateCsvLine(order, item, epCache));
                }
            }

            Logger.WriteInfo(string.Format("CSV generated: {0} item lines from {1} orders", orders.Sum(o => o.Items.Count), orders.Count));

            // Save locally
            string fullPath = Path.Combine(localFilePath, fileName);
            SaveCsvLocally(csv, fullPath);

            return new CsvResult
            {
                Csv = csv,
                FileName = fileName,
                OrderIds = orderIds,
                NewFolderOrderIds = newFolderOrderIds
            };
        }

        private Dictionary<string, string> BuildEPCache(List<ExtendedProperty> props)
        {
            var cache = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (props != null)
            {
                foreach (var ep in props)
                {
                    if (!string.IsNullOrEmpty(ep.Name) && !cache.ContainsKey(ep.Name))
                    {
                        cache[ep.Name] = ep.Value ??  "";
                    }
                }
            }
            return cache;
        }

        private string GenerateCsvLine(OrderDetails order, OrderItem item, Dictionary<string, string> epCache)
        {
            var values = new List<string>
            {
                order.NumOrderId.ToString(),
                order.GeneralInfo.ReferenceNum,
                order.GeneralInfo.SecondaryReference,
                order.GeneralInfo.ExternalReferenceNum,
                GetEPFromCache(epCache, "PrimaryPONumber"),
                GetEPFromCache(epCache, "JDEOrderNo"),
                GetEPFromCache(epCache, "SoldTo"),
                order.GeneralInfo.ReceivedDate.ToString("yyyy-MM-dd"),
                order.GeneralInfo.Source,
                order.GeneralInfo.SubSource,
                order.GeneralInfo.DespatchByDate == DateTime.MinValue ? "" : order. GeneralInfo.DespatchByDate.ToString("yyyy-MM-dd"),
                order.GeneralInfo.NumItems.ToString(),
                order.ShippingInfo.PostalServiceName,
                order.ShippingInfo.TotalWeight.ToString(),
                order.ShippingInfo.TrackingNumber,
                item.SKU,
                item. ChannelSKU,
                item.Title,
                item. Quantity.ToString(),
                item. ItemNumber,
                item.PricePerUnit.ToString(),
                item. DiscountValue.ToString(),
                item.TaxRate.ToString(),
                item.Weight.ToString()
            };

            return string. Join(",", values. Select(v => EscapeCsvValue(v)));
        }

        private string GetEPFromCache(Dictionary<string, string> cache, string name)
        {
            string val;
            return cache.TryGetValue(name, out val) ? val : "";
        }

        private string EscapeCsvValue(string value)
        {
            if (string.IsNullOrEmpty(value)) return "";
            
            if (value.Contains(",") || value.Contains("\"") || value.Contains("\n") || value.Contains("\r"))
            {
                return "\"" + value.Replace("\"", "\"\"") + "\"";
            }
            return value;
        }

        private string SanitizeForFileName(string input)
        {
            if (string.IsNullOrEmpty(input)) return "";
            
            char[] invalidChars = Path.GetInvalidFileNameChars();
            var result = new StringBuilder();
            foreach (char c in input)
            {
                if (! invalidChars.Contains(c) && c != ' ')
                {
                    result. Append(c);
                }
            }
            return result. ToString().Trim();
        }

        private void SaveCsvLocally(StringBuilder csv, string localPath)
        {
            try
            {
                File.WriteAllText(localPath, csv.ToString());
                Logger.WriteInfo(string.Format("File saved:  {0}", localPath));
            }
            catch (Exception ex)
            {
                Logger.WriteError(string.Format("Error saving file locally: {0}", ex.Message));
                throw;
            }
        }

        #endregion

        #region SFTP Upload

        private bool SendByFTP(StringBuilder report, SFtpSettings sftpSettings)
        {
            try
            {
                using (var upload = ProxyFactory.GetSFtpUploadProxy(sftpSettings))
                {
                    if (upload == null)
                        throw new Exception("SFTP upload proxy is null.");

                    upload.Write(report.ToString());
                    var uploadResult = upload.CompleteUpload();

                    if (! uploadResult. IsSuccess)
                    {
                        Logger.WriteError(string.Format("SFTP upload failed: {0}", uploadResult.ErrorMessage));
                        return false;
                    }

                    return true;
                }
            }
            catch (Exception ex)
            {
                Logger.WriteError(string. Format("Error in SendByFTP: {0}", ex.Message));
                return false;
            }
        }

        #endregion

        #region Email Notifications

        private void SendNotificationEmail(string subject, string body)
        {
            try
            {
                var recipientIds = new List<Guid> { Guid.Parse("c9a7f89f-790f-4db2-88a1-f7f31cccb5b8") };
                
                var emailRequest = new GenerateFreeTextEmailRequest
                {
                    ids = recipientIds,
                    subject = subject,
                    body = body,
                    templateType = null
                };

                var response = Api.Email.GenerateFreeTextEmail(emailRequest);
                
                if (response.isComplete)
                {
                    Logger.WriteInfo("Email sent successfully");
                }
                else
                {
                    string failedRecipients = response. FailedRecipients != null 
                        ? string.Join(",", response.FailedRecipients) 
                        : "";
                    Logger.WriteError(string.Format("Email failed.  Failed recipients: {0}", failedRecipients));
                }
            }
            catch (Exception ex)
            {
                Logger.WriteError(string.Format("Error sending email: {0}", ex.Message));
            }
        }

        #endregion

        #region Configuration Builders

        private List<NotificationConfig> BuildNotificationConfigs(
            string notifyAcknowledge, string notifyOOS, string notifyBIS, 
            string notifyShipped, string actionCancelled, string notifyCancelled,
            string newFolder, string oosFolder, string bisFolder, string cancelFolder,
            string acknowledgeDirectory, string oosDirectory, string bisDirectory,
            string shippedDirectory, string actionCancelledDirectory, string cancelDirectory)
        {
            return new List<NotificationConfig>
            {
                new NotificationConfig
                {
                    NotifyFlag = notifyAcknowledge,
                    NotifyType = "notifyAcknowledge",
                    Folder = newFolder,
                    Directory = acknowledgeDirectory,
                    Type = NotificationType.OpenAcknowledge,
                    EPFilter = Constants.EPNames.StatusACK,
                    EPUpdate = Constants.EPNames.StatusACK,
                    RequiresChannelUpdates = true
                },
                new NotificationConfig
                {
                    NotifyFlag = notifyOOS,
                    NotifyType = "notifyOOS",
                    Folder = oosFolder,
                    Directory = oosDirectory,
                    Type = NotificationType. Open,
                    EPFilter = Constants.EPNames.StatusOOS,
                    EPUpdate = Constants. EPNames.StatusOOS,
                    RequiresChannelUpdates = true
                },
                new NotificationConfig
                {
                    NotifyFlag = notifyBIS,
                    NotifyType = "notifyBIS",
                    Folder = bisFolder,
                    Directory = bisDirectory,
                    Type = NotificationType.Open,
                    EPFilter = Constants.EPNames.StatusBIS,
                    EPUpdate = Constants.EPNames.StatusBIS,
                    RequiresChannelUpdates = true
                },
                new NotificationConfig
                {
                    NotifyFlag = notifyShipped,
                    NotifyType = "notifyShipped",
                    Folder = shippedDirectory,
                    Directory = shippedDirectory,
                    Type = NotificationType.Shipped,
                    EPFilter = Constants.EPNames.StatusASN,
                    EPUpdate = Constants.EPNames.StatusASN,
                    RequiresChannelUpdates = true
                },
                new NotificationConfig
                {
                    NotifyFlag = actionCancelled,
                    NotifyType = "actionCancelled",
                    Folder = cancelFolder,
                    Directory = actionCancelledDirectory,
                    Type = NotificationType.OpenNoEPFilter,
                    EPFilter = "",
                    EPUpdate = "",
                    RequiresChannelUpdates = false
                },
                new NotificationConfig
                {
                    NotifyFlag = notifyCancelled,
                    NotifyType = "notifyCancelled",
                    Folder = cancelDirectory,
                    Directory = cancelDirectory,
                    Type = NotificationType. Cancelled,
                    EPFilter = Constants.EPNames.StatusCANC,
                    EPUpdate = Constants.EPNames.StatusCANC,
                    RequiresChannelUpdates = true
                }
            };
        }

        private void LogExecutionStart(string source, string subSource, string outputMethod)
        {
            Logger.WriteInfo("========================================");
            Logger.WriteInfo("Starting macro:  UW Home Default Channel Updater");
            Logger.WriteInfo("========================================");
            Logger. WriteInfo(string.Format("Execution started at:  {0:yyyy-MM-dd HH: mm:ss}", DateTime.Now));
            Logger.WriteInfo(string.Format("Source: {0}, SubSource: {1}, OutputMethod: {2}", source, subSource, outputMethod));
        }

        #endregion
    }
}