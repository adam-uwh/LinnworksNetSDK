// UW Home Default Channel Updater
// ================================
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
// 4. notifyShipped (Type:  Shipped)
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
// 6. notifyCancelled (Type: Cancelled)
//    - Returns cancelled orders within lookBackDays (searches by cancelled date)
//    - Filtered by subSource and Source
//    - Only includes orders where ExtendedProperty 'ChannelUpdatesRequired' is TRUE
//    - Only includes orders where 'StatusCANC' is FALSE or not set
//    - Excludes orders already in 'Completed' folder
//    - NO folder move after processing
//    - After processing, sets ExtendedProperty 'StatusCANC' to TRUE
//
// OUTPUT: 
// - CSV files are saved locally first, then optionally uploaded via SFTP
// - Filename format: {subSource}_Orders_{type}_{timestamp}_{filetype}.csv
//   Examples: MultiVery_Orders_Shipped_20260115141132_Direct.csv
// - Email notifications are sent after each file operation
// - ALL CSV files use the same standardized format with identical headers
//
// FILTERS APPLIED TO ALL OPEN ORDERS (Types: OpenAcknowledge, Open, OpenNoEPFilter):
// - Status = 1 (Paid - Note: In Linnworks, Status 0=UNPAID, 1=PAID)
// - Parked = 0 (Not parked)
// - Locked = 0 (Not locked)
//
// FILTERS APPLIED TO PROCESSED ORDERS (Types:  Shipped, Cancelled):
// - Searches within lookBackDays date range
// - Filters by Source and SubSource
// - Excludes orders in 'Completed' folder
//
// EXTENDED PROPERTY FILTERS & UPDATES:
// | Notification Type  | ChannelUpdatesRequired | EP Status Filter    | EP Set After Processing | Folder Action              |
// |--------------------|------------------------|---------------------|-------------------------|----------------------------|
// | notifyAcknowledge  | Must be TRUE           | statusACK != TRUE   | statusACK = TRUE        | Move from newFolder to Updated |
// | notifyOOS          | Must be TRUE           | statusOOS != TRUE   | statusOOS = TRUE        | None                       |
// | notifyBIS          | Must be TRUE           | statusBIS != TRUE   | statusBIS = TRUE        | Move to Updated            |
// | notifyShipped      | Must be TRUE           | StatusASN != TRUE   | StatusASN = TRUE        | None                       |
// | actionCancelled    | No filter              | No EP filter        | No EP update            | Cancel orders              |
// | notifyCancelled    | Must be TRUE           | StatusCANC != TRUE  | StatusCANC = TRUE       | None                       |
//
// CSV HEADER COLUMNS:
// Linnworks Order Number, Reference Num, Secondary Ref, External Ref, Primary PO Field,
// JDE Order Number, Sold To Account, Received Date, Source, Sub Source, Despatch By Date,
// Number Order Items, Postal Service Name, Total Order Weight, Tracking Number,
// Item > SKU, Item > ChannelSKU, Item > Description, Item > Quantity, Item > Line Ref,
// Item > Item Cost (ex VAT), Item > Item Discount (ex VAT), Item > Tax Rate, Item > Weight Per Item
//
// PARAMETERS:
// - Source:  The order source channel (used for Shipped/Cancelled searches)
// - subSource: The order sub-source for filtering all order types
// - notifyAcknowledge, notifyOOS, notifyBIS, notifyShipped, actionCancelled, notifyCancelled:  TRUE/FALSE flags to enable each step
// - newFolder:  Folder name for notifyAcknowledge (orders in this folder get moved to Updated)
// - oosFolder: Folder name for notifyOOS
// - bisFolder: Folder name for notifyBIS
// - cancelFolder: Folder name for actionCancelled
// - outputMethod: "Local" or "FTP" - determines where CSV files are saved/uploaded
// - SFTP*:  SFTP connection parameters (Server, Port, Username, Password, FolderRoot)
// - *Directory:  SFTP directory paths for each notification type
// - filetype: File type identifier included in filename
// - sortField: Field to sort orders by ("ORDERID" or "REFERENCE")
// - sortDirection: Sort direction ("ASCENDING" or "DESCENDING")
// - lookBackDays:  Number of days to look back for processed/cancelled orders
// - localFilePath: Local directory path for saving CSV files

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
        private const string StandardCsvHeader = "Linnworks Order Number,Reference Num,Secondary Ref,External Ref,Primary PO Field,JDE Order Number,Sold To Account,Received Date,Source,Sub Source,Despatch By Date,Number Order Items,Postal Service Name,Total Order Weight,Tracking Number,Item > SKU,Item > ChannelSKU,Item > Description,Item > Quantity,Item > Line Ref,Item > Item Cost (ex VAT),Item > Item Discount (ex VAT),Item > Tax Rate,Item > Weight Per Item";

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
            string outputMethod
        )
        {
            this.Logger.WriteInfo("========================================");
            this.Logger. WriteInfo("Starting macro:  UW Home Default Channel Updater");
            this.Logger.WriteInfo("========================================");
            this.Logger.WriteInfo($"Execution started at:  {DateTime.Now:yyyy-MM-dd HH: mm:ss}");
            
            this.Logger.WriteInfo("--- Input Parameters ---");
            this.Logger.WriteInfo($"Source: {Source}");
            this.Logger.WriteInfo($"subSource: {subSource}");
            this.Logger.WriteInfo($"notifyAcknowledge: {notifyAcknowledge}");
            this.Logger.WriteInfo($"notifyOOS: {notifyOOS}");
            this.Logger.WriteInfo($"notifyBIS: {notifyBIS}");
            this.Logger.WriteInfo($"notifyShipped: {notifyShipped}");
            this.Logger.WriteInfo($"actionCancelled:  {actionCancelled}");
            this.Logger.WriteInfo($"notifyCancelled: {notifyCancelled}");
            this.Logger.WriteInfo($"newFolder: {newFolder}");
            this.Logger.WriteInfo($"oosFolder: {oosFolder}");
            this.Logger.WriteInfo($"bisFolder: {bisFolder}");
            this.Logger.WriteInfo($"cancelFolder: {cancelFolder}");
            this.Logger.WriteInfo($"outputMethod: {outputMethod}");
            this.Logger.WriteInfo($"localFilePath: {localFilePath}");
            this.Logger.WriteInfo($"sortField: {sortField}");
            this.Logger.WriteInfo($"sortDirection: {sortDirection}");
            this.Logger.WriteInfo($"lookBackDays:  {lookBackDays}");
            this.Logger.WriteInfo($"SFTPServer: {SFTPServer}");
            this.Logger.WriteInfo($"SFTPPort: {SFTPPort}");
            this.Logger.WriteInfo($"SFTPFolderRoot: {SFTPFolderRoot}");
            this.Logger.WriteInfo("------------------------");

            var notificationConfigs = new[]
            {
                new { Notify = notifyAcknowledge, NotifyType = "notifyAcknowledge", Folder = newFolder, Directory = acknowledgeDirectory, Type = "OpenAcknowledge", EPFilter = "statusACK", EPUpdate = "statusACK", RequiresChannelUpdates = true },
                new { Notify = notifyOOS, NotifyType = "notifyOOS", Folder = oosFolder, Directory = oosDirectory, Type = "Open", EPFilter = "statusOOS", EPUpdate = "statusOOS", RequiresChannelUpdates = true },
                new { Notify = notifyBIS, NotifyType = "notifyBIS", Folder = bisFolder, Directory = bisDirectory, Type = "Open", EPFilter = "statusBIS", EPUpdate = "statusBIS", RequiresChannelUpdates = true },
                new { Notify = notifyShipped, NotifyType = "notifyShipped", Folder = shippedDirectory, Directory = shippedDirectory, Type = "Shipped", EPFilter = "StatusASN", EPUpdate = "StatusASN", RequiresChannelUpdates = true },
                new { Notify = actionCancelled, NotifyType = "actionCancelled", Folder = cancelFolder, Directory = actionCancelledDirectory, Type = "OpenNoEPFilter", EPFilter = "", EPUpdate = "", RequiresChannelUpdates = false },
                new { Notify = notifyCancelled, NotifyType = "notifyCancelled", Folder = cancelDirectory, Directory = cancelDirectory, Type = "Cancelled", EPFilter = "StatusCANC", EPUpdate = "StatusCANC", RequiresChannelUpdates = true }
            };

            this.Logger.WriteInfo($"Total notification configs to process: {notificationConfigs. Length}");

            int configIndex = 0;
            foreach (var config in notificationConfigs)
            {
                configIndex++;
                this.Logger. WriteInfo("----------------------------------------");
                this.Logger.WriteInfo($"Processing config {configIndex}/{notificationConfigs.Length}:  {config.NotifyType}");
                this. Logger.WriteInfo($"  Notify flag value: {config.Notify}");
                this.Logger.WriteInfo($"  Folder:  {config.Folder}");
                this.Logger.WriteInfo($"  Directory: {config.Directory}");
                this.Logger. WriteInfo($"  Type: {config.Type}");
                this.Logger.WriteInfo($"  EP Filter: {(string.IsNullOrEmpty(config.EPFilter) ? "NONE" : config.EPFilter)}");
                this.Logger.WriteInfo($"  EP Update: {(string.IsNullOrEmpty(config.EPUpdate) ? "NONE" : config.EPUpdate)}");
                this.Logger. WriteInfo($"  Requires ChannelUpdatesRequired=TRUE: {config.RequiresChannelUpdates}");

                if (! string. Equals(config. Notify, "TRUE", StringComparison.OrdinalIgnoreCase))
                {
                    this.Logger.WriteInfo($"  SKIPPED:  {config.NotifyType} is not set to TRUE");
                    continue;
                }

                this.Logger.WriteInfo($"  Processing {config.NotifyType}.. .");

                List<OrderDetails> orders = new List<OrderDetails>();

                try
                {
                    if (config.Type == "OpenAcknowledge")
                    {
                        this.Logger.WriteInfo($"  Fetching ALL open orders for Acknowledge (filtered by ChannelUpdatesRequired=TRUE and statusACK EP)...");
                        orders = GetFilteredOpenOrdersForAcknowledge(subSource, sortField, sortDirection);
                    }
                    else if (config.Type == "Open")
                    {
                        this.Logger.WriteInfo($"  Fetching open orders from folder '{config.Folder}' with ChannelUpdatesRequired=TRUE and EP filter '{config.EPFilter}'...");
                        orders = GetFilteredOpenOrders(subSource, config.Folder, sortField, sortDirection, config.EPFilter);
                    }
                    else if (config.Type == "OpenNoEPFilter")
                    {
                        this.Logger. WriteInfo($"  Fetching open orders from folder '{config. Folder}' WITHOUT EP filter (actionCancelled)...");
                        orders = GetFilteredOpenOrdersNoEPFilter(subSource, config.Folder, sortField, sortDirection);
                    }
                    else
                    {
                        bool isShipped = config.Type == "Shipped";
                        this.Logger.WriteInfo($"  Fetching processed orders (isShipped:  {isShipped}) with ChannelUpdatesRequired=TRUE and EP filter '{config.EPFilter}'...");
                        orders = GetFilteredProcessedOrders(subSource, Source, sortField, sortDirection, lookBackDays, isShipped, config.EPFilter);
                    }

                    this.Logger.WriteInfo($"  Orders retrieved: {orders.Count}");

                    if (orders.Count == 0)
                    {
                        this.Logger.WriteInfo($"  No orders found for {config.NotifyType} - folder: {config.Folder}.  Skipping to next config.");
                        continue;
                    }

                    this.Logger.WriteInfo($"  Order IDs found: {string.Join(", ", orders.Select(o => o. NumOrderId))}");
                }
                catch (Exception ex)
                {
                    this.Logger.WriteError($"  ERROR fetching orders for {config.NotifyType}: {ex.Message}");
                    this.Logger.WriteError($"  Stack trace: {ex.StackTrace}");
                    continue;
                }

                (StringBuilder Csv, string FileName, Guid[] OrderIds, Guid[] NewFolderOrderIds) result;

                try
                {
                    this.Logger.WriteInfo($"  Formatting CSV for {config.NotifyType}...");

                    switch (config.NotifyType)
                    {
                        case "notifyAcknowledge":
                            result = FormatStandardCsv(orders, "Acknowledge", localFilePath, filetype, subSource, newFolder);
                            break;
                        case "notifyOOS": 
                            var oosResult = FormatStandardCsv(orders, "OOS", localFilePath, filetype, subSource, null);
                            result = (oosResult.Csv, oosResult.FileName, oosResult.OrderIds, new Guid[0]);
                            break;
                        case "notifyBIS":
                            var bisResult = FormatStandardCsv(orders, "BIS", localFilePath, filetype, subSource, null);
                            result = (bisResult.Csv, bisResult.FileName, bisResult.OrderIds, new Guid[0]);
                            break;
                        case "notifyShipped":
                            var shippedResult = FormatStandardCsv(orders, "Shipped", localFilePath, filetype, subSource, null);
                            result = (shippedResult.Csv, shippedResult.FileName, shippedResult.OrderIds, new Guid[0]);
                            break;
                        case "actionCancelled":
                            var actionResult = FormatStandardCsv(orders, "ActionCancelled", localFilePath, filetype, subSource, null);
                            result = (actionResult.Csv, actionResult.FileName, actionResult.OrderIds, new Guid[0]);
                            break;
                        case "notifyCancelled":
                            var cancelResult = FormatStandardCsv(orders, "Cancelled", localFilePath, filetype, subSource, null);
                            result = (cancelResult.Csv, cancelResult.FileName, cancelResult.OrderIds, new Guid[0]);
                            break;
                        default:
                            this.Logger.WriteError($"  Unknown NotifyType: {config.NotifyType}.  Using default CSV format.");
                            var defaultResult = FormatStandardCsv(orders, "FAILED", localFilePath, filetype, subSource, null);
                            result = (defaultResult.Csv, defaultResult.FileName, defaultResult.OrderIds, new Guid[0]);
                            break;
                    }

                    this.Logger.WriteInfo($"  CSV formatted successfully. FileName: {result.FileName}");
                    this.Logger.WriteInfo($"  OrderIds to process after output: {result.OrderIds?. Length ??  0}");
                    if (config.NotifyType == "notifyAcknowledge")
                    {
                        this.Logger.WriteInfo($"  NewFolderOrderIds to move to Updated:  {result.NewFolderOrderIds?.Length ?? 0}");
                    }
                }
                catch (Exception ex)
                {
                    this.Logger. WriteError($"  ERROR formatting CSV for {config.NotifyType}:  {ex.Message}");
                    this.Logger.WriteError($"  Stack trace: {ex. StackTrace}");
                    continue;
                }

                string emailSubject = "";
                string emailBody = "";
                bool outputSuccess = false;

                try
                {
                    if (string.Equals(outputMethod, "FTP", StringComparison.OrdinalIgnoreCase))
                    {
                        this.Logger.WriteInfo($"  Output method: FTP");

                        var sftpSettings = new SFtpSettings
                        {
                            UserName = SFTPUsername,
                            Password = SFTPPassword,
                            Server = SFTPServer. StartsWith("sftp://") ? SFTPServer.Substring(7) : SFTPServer,
                            Port = SFTPPort,
                            FullPath = $"{SFTPFolderRoot}/{config.Directory}/{result.FileName}"
                        };

                        this.Logger.WriteInfo($"  SFTP Settings configured:");
                        this.Logger.WriteInfo($"    Server: {sftpSettings.Server}");
                        this.Logger.WriteInfo($"    Port: {sftpSettings.Port}");
                        this.Logger. WriteInfo($"    User: {sftpSettings.UserName}");
                        this. Logger.WriteInfo($"    FullPath: {sftpSettings.FullPath}");

                        this.Logger.WriteInfo($"  Attempting SFTP upload...");

                        if (SendByFTP(result.Csv, sftpSettings))
                        {
                            this.Logger.WriteInfo($"  SUCCESS: CSV uploaded to SFTP at '{sftpSettings.FullPath}'");
                            outputSuccess = true;
                            emailSubject = $"SFTP Upload Successful for {config. Folder}";
                            emailBody = $"The CSV file '{result.FileName}' was successfully uploaded to SFTP at '{sftpSettings.FullPath}'. ";
                        }
                        else
                        {
                            this.Logger.WriteError($"  FAILED: Could not upload CSV to SFTP at '{sftpSettings.FullPath}'");
                            emailSubject = $"SFTP Upload Failed for {config.Folder}";
                            emailBody = $"The CSV file '{result. FileName}' could not be uploaded to SFTP at '{sftpSettings.FullPath}'. Please check the logs for details.";
                        }
                    }
                    else
                    {
                        this.Logger.WriteInfo($"  Output method: Local");
                        string fullLocalPath = Path.Combine(localFilePath, result.FileName);
                        this.Logger.WriteInfo($"  CSV already saved locally at: {fullLocalPath}");
                        outputSuccess = true;
                        emailSubject = $"Local Save Successful for {config.Folder}";
                        emailBody = $"The CSV file '{result. FileName}' was successfully saved locally at '{fullLocalPath}'. ";
                    }

                    if (outputSuccess)
                    {
                        this. Logger.WriteInfo($"  Output successful - processing order updates for {config.NotifyType}...");
                        
                        if (config.NotifyType == "notifyAcknowledge")
                        {
                            ProcessOrderFolderUpdates(config.NotifyType, result.NewFolderOrderIds);
                        }
                        else
                        {
                            ProcessOrderFolderUpdates(config. NotifyType, result.OrderIds);
                        }
                        
                        if (! string.IsNullOrEmpty(config.EPUpdate))
                        {
                            SetOrderExtendedProperty(result.OrderIds, config. EPUpdate, "TRUE");
                        }
                        else
                        {
                            this.Logger.WriteInfo($"  No ExtendedProperty update configured for {config.NotifyType}");
                        }
                    }
                    else
                    {
                        this.Logger. WriteInfo($"  Output failed - skipping order updates for {config.NotifyType}");
                    }
                }
                catch (Exception ex)
                {
                    this.Logger.WriteError($"  ERROR during output for {config.NotifyType}: {ex.Message}");
                    this.Logger.WriteError($"  Stack trace: {ex. StackTrace}");
                    emailSubject = $"Error Processing {config. Folder}";
                    emailBody = $"An error occurred while processing {config.NotifyType}:  {ex.Message}";
                }

                try
                {
                    this. Logger.WriteInfo($"  Sending notification email...");
                    SendMacroEmail(
                        new List<Guid> { Guid.Parse("6665d96a-ef96-46bc-a172-291b29785fbe") },
                        emailSubject,
                        emailBody
                    );
                    this.Logger.WriteInfo($"  Email sent successfully.");
                }
                catch (Exception ex)
                {
                    this.Logger.WriteError($"  ERROR sending email for {config.NotifyType}: {ex.Message}");
                    this.Logger.WriteError($"  Stack trace: {ex. StackTrace}");
                }

                this.Logger.WriteInfo($"  Completed processing {config.NotifyType}");
            }

            this.Logger.WriteInfo("========================================");
            this.Logger.WriteInfo($"Macro export complete at:  {DateTime.Now:yyyy-MM-dd HH: mm:ss}");
            this.Logger.WriteInfo("========================================");
        }

        private void ProcessOrderFolderUpdates(string notifyType, Guid[] orderIds)
        {
            this.Logger.WriteInfo($"    ProcessOrderFolderUpdates called for {notifyType}");

            if (orderIds == null || orderIds.Length == 0)
            {
                this.Logger.WriteInfo($"    No order IDs to process - skipping folder updates");
                return;
            }

            this.Logger.WriteInfo($"    Order IDs to update: {string.Join(", ", orderIds)}");

            var orderIdList = orderIds.ToList();

            try
            {
                if (notifyType == "notifyAcknowledge" || notifyType == "notifyBIS")
                {
                    this.Logger.WriteInfo($"    Moving {orderIdList.Count} orders to 'Updated' folder.. .");
                    Api.Orders.AssignToFolder(orderIdList, "Updated");
                    this.Logger.WriteInfo($"    Successfully moved orders to 'Updated' folder");
                }
                else if (notifyType == "actionCancelled")
                {
                    this.Logger.WriteInfo($"    Cancelling {orderIdList.Count} orders...");
                    foreach (var orderId in orderIdList)
                    {
                        try
                        {
                            Api.Orders.CancelOrder(orderId, Guid.Empty, 0, "Cancelled via actionCancelled macro");
                            this.Logger. WriteInfo($"      Successfully cancelled order {orderId}");
                        }
                        catch (Exception ex)
                        {
                            this.Logger.WriteError($"      ERROR cancelling order {orderId}: {ex.Message}");
                        }
                    }
                    this.Logger.WriteInfo($"    Order cancellation complete");
                }
                else
                {
                    this.Logger. WriteInfo($"    No folder move required for {notifyType}");
                }
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"    ERROR in ProcessOrderFolderUpdates for {notifyType}: {ex. Message}");
                this.Logger.WriteError($"    Stack trace: {ex.StackTrace}");
            }
        }

        private void SetOrderExtendedProperty(Guid[] orderIds, string propertyName, string propertyValue)
        {
            this.Logger.WriteInfo($"    SetOrderExtendedProperty called");
            this.Logger.WriteInfo($"    PropertyName: {propertyName}, PropertyValue: {propertyValue}");

            if (orderIds == null || orderIds.Length == 0)
            {
                this.Logger.WriteInfo($"    No order IDs provided - skipping ExtendedProperty update");
                return;
            }

            this.Logger.WriteInfo($"    Processing {orderIds.Length} orders for ExtendedProperty update");

            int successCount = 0;
            int errorCount = 0;

            foreach (var orderId in orderIds)
            {
                try
                {
                    this.Logger.WriteInfo($"      Setting EP '{propertyName}' = '{propertyValue}' for order {orderId}.. .");

                    var existingProperties = Api.Orders.GetExtendedProperties(orderId);
                    this.Logger.WriteInfo($"      Retrieved {existingProperties?. Count ??  0} existing properties for order {orderId}");

                    if (existingProperties != null && existingProperties.Count > 0)
                    {
                        this.Logger.WriteInfo($"      Existing properties:  {string.Join(", ", existingProperties.Select(p => $"{p.Name}={p.Value}"))}");
                    }

                    var allProperties = new List<ExtendedProperty>();
                    bool propertyFound = false;

                    if (existingProperties != null)
                    {
                        foreach (var prop in existingProperties)
                        {
                            if (string.Equals(prop.Name, propertyName, StringComparison.OrdinalIgnoreCase))
                            {
                                this.Logger.WriteInfo($"      Property '{propertyName}' exists (RowId: {prop.RowId}). Updating value from '{prop.Value}' to '{propertyValue}'.. .");
                                allProperties.Add(new ExtendedProperty
                                {
                                    RowId = prop.RowId,
                                    Name = prop.Name,
                                    Value = propertyValue,
                                    Type = prop.Type ??  "Order"
                                });
                                propertyFound = true;
                            }
                            else
                            {
                                allProperties.Add(new ExtendedProperty
                                {
                                    RowId = prop. RowId,
                                    Name = prop.Name,
                                    Value = prop.Value,
                                    Type = prop.Type ?? "Order"
                                });
                            }
                        }
                    }

                    if (! propertyFound)
                    {
                        this.Logger.WriteInfo($"      Property '{propertyName}' does not exist. Adding new property...");
                        allProperties.Add(new ExtendedProperty
                        {
                            RowId = Guid.Empty,
                            Name = propertyName,
                            Value = propertyValue,
                            Type = "Order"
                        });
                    }

                    this.Logger.WriteInfo($"      Saving {allProperties.Count} properties back to order {orderId}.. .");
                    Api.Orders. SetExtendedProperties(orderId, allProperties. ToArray());

                    this.Logger.WriteInfo($"      Successfully updated EP '{propertyName}' for order {orderId} (preserved {allProperties.Count - 1} other properties)");
                    successCount++;
                }
                catch (Exception ex)
                {
                    errorCount++;
                    this.Logger.WriteError($"      ERROR setting EP for order {orderId}: {ex. Message}");
                    this.Logger.WriteError($"      Stack trace: {ex. StackTrace}");
                }
            }

            this.Logger. WriteInfo($"    SetOrderExtendedProperty completed.  Success:  {successCount}, Errors: {errorCount}");
        }

        private bool HasChannelUpdatesRequired(OrderDetails order)
        {
            if (order. ExtendedProperties == null)
                return false;

            var channelUpdatesProp = order.ExtendedProperties. FirstOrDefault(ep =>
                string.Equals(ep.Name, "ChannelUpdatesRequired", StringComparison.OrdinalIgnoreCase));

            if (channelUpdatesProp == null)
                return false;

            return string.Equals(channelUpdatesProp.Value, "TRUE", StringComparison.OrdinalIgnoreCase);
        }

        private bool HasStatusEPSetToTrue(OrderDetails order, string epName)
        {
            if (string.IsNullOrEmpty(epName) || order.ExtendedProperties == null)
                return false;

            var statusProp = order.ExtendedProperties.FirstOrDefault(ep =>
                string.Equals(ep. Name, epName, StringComparison.OrdinalIgnoreCase));

            if (statusProp == null)
                return false;

            return string.Equals(statusProp.Value, "TRUE", StringComparison.OrdinalIgnoreCase);
        }

        private List<OrderDetails> GetFilteredOpenOrdersForAcknowledge(string subSource, string sortField, string sortDirection)
        {
            this.Logger.WriteInfo($"      GetFilteredOpenOrdersForAcknowledge started");
            this.Logger.WriteInfo($"      Parameters - subSource: {subSource}, sortField: {sortField}, sortDirection:  {sortDirection}");

            try
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

                this.Logger.WriteInfo($"      Filter configured:  Status=1 (PAID), Parked=0, Locked=0, SubSource={subSource}");

                FieldCode sortingCode = sortField.ToUpper() switch
                {
                    "ORDERID" => FieldCode. GENERAL_INFO_ORDER_ID,
                    "REFERENCE" => FieldCode.GENERAL_INFO_REFERENCE_NUMBER,
                    _ => FieldCode.GENERAL_INFO_ORDER_ID
                };

                ListSortDirection sortingDirection = sortDirection.ToUpper() == "ASCENDING"
                    ? ListSortDirection.Ascending
                    : ListSortDirection.Descending;

                this.Logger.WriteInfo($"      Sorting by:  {sortingCode}, Direction: {sortingDirection}");

                this.Logger.WriteInfo($"      Calling Api.Orders.GetAllOpenOrders...");
                var guids = Api.Orders.GetAllOpenOrders(filter, new List<FieldSorting>
                {
                    new FieldSorting { FieldCode = sortingCode, Direction = sortingDirection, Order = 0 }
                }, Guid.Empty, "");

                this.Logger.WriteInfo($"      GetAllOpenOrders returned {guids.Count} order GUIDs");

                var orders = new List<OrderDetails>();
                if (guids. Count > 0)
                {
                    this.Logger.WriteInfo($"      Calling Api.Orders.GetOrdersById for {guids.Count} orders.. .");
                    orders = Api. Orders.GetOrdersById(guids);
                    this.Logger.WriteInfo($"      GetOrdersById returned {orders.Count} order details");
                }

                int beforeChannelFilter = orders.Count;
                orders = orders.Where(order => HasChannelUpdatesRequired(order)).ToList();
                this.Logger.WriteInfo($"      EP filter 'ChannelUpdatesRequired = TRUE':  {beforeChannelFilter} -> {orders.Count} orders");

                int beforeStatusFilter = orders.Count;
                orders = orders.Where(order => ! HasStatusEPSetToTrue(order, "statusACK")).ToList();
                this. Logger.WriteInfo($"      EP filter 'statusACK != TRUE': {beforeStatusFilter} -> {orders.Count} orders");

                orders = sortingCode == FieldCode.GENERAL_INFO_ORDER_ID
                    ?  (sortingDirection == ListSortDirection. Ascending
                        ? orders.OrderBy(o => o.NumOrderId).ToList()
                        : orders.OrderByDescending(o => o.NumOrderId).ToList())
                    : (sortingDirection == ListSortDirection.Ascending
                        ? orders.OrderBy(o => o. GeneralInfo.ReferenceNum).ToList()
                        : orders.OrderByDescending(o => o.GeneralInfo.ReferenceNum).ToList());

                this.Logger. WriteInfo($"      GetFilteredOpenOrdersForAcknowledge completed.  Returning {orders.Count} orders");
                return orders;
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"      ERROR in GetFilteredOpenOrdersForAcknowledge: {ex. Message}");
                this.Logger.WriteError($"      Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        private List<OrderDetails> GetFilteredOpenOrders(string subSource, string folderName, string sortField, string sortDirection, string epFilterName)
        {
            this.Logger.WriteInfo($"      GetFilteredOpenOrders started");
            this.Logger.WriteInfo($"      Parameters - subSource:  {subSource}, folderName:  {folderName}, sortField:  {sortField}, sortDirection: {sortDirection}, epFilterName: {epFilterName}");

            try
            {
                var filter = new FieldsFilter
                {
                    NumericFields = new List<NumericFieldFilter>
                    {
                        new NumericFieldFilter { FieldCode = FieldCode.GENERAL_INFO_STATUS, Type = NumericFieldFilterType.Equal, Value = 1 },
                        new NumericFieldFilter { FieldCode = FieldCode.GENERAL_INFO_PARKED, Type = NumericFieldFilterType.Equal, Value = 0 },
                        new NumericFieldFilter { FieldCode = FieldCode.GENERAL_INFO_LOCKED, Type = NumericFieldFilterType.Equal, Value = 0 }
                    },
                    ListFields = new List<ListFieldFilter>
                    {
                        new ListFieldFilter { FieldCode = FieldCode.FOLDER, Value = folderName, Type = ListFieldFilterType.Is }
                    },
                    TextFields = new List<TextFieldFilter>
                    {
                        new TextFieldFilter { FieldCode = FieldCode.GENERAL_INFO_SUBSOURCE, Text = subSource, Type = TextFieldFilterType.Equal }
                    }
                };

                this.Logger.WriteInfo($"      Filter configured: Status=1 (PAID), Parked=0, Locked=0, Folder={folderName}, SubSource={subSource}");

                FieldCode sortingCode = sortField.ToUpper() switch
                {
                    "ORDERID" => FieldCode. GENERAL_INFO_ORDER_ID,
                    "REFERENCE" => FieldCode.GENERAL_INFO_REFERENCE_NUMBER,
                    _ => FieldCode. GENERAL_INFO_ORDER_ID
                };

                ListSortDirection sortingDirection = sortDirection. ToUpper() == "ASCENDING"
                    ? ListSortDirection.Ascending
                    :  ListSortDirection.Descending;

                this.Logger.WriteInfo($"      Sorting by:  {sortingCode}, Direction: {sortingDirection}");

                this.Logger.WriteInfo($"      Calling Api.Orders.GetAllOpenOrders...");
                var guids = Api.Orders.GetAllOpenOrders(filter, new List<FieldSorting>
                {
                    new FieldSorting { FieldCode = sortingCode, Direction = sortingDirection, Order = 0 }
                }, Guid.Empty, "");

                this.Logger.WriteInfo($"      GetAllOpenOrders returned {guids. Count} order GUIDs");

                var orders = new List<OrderDetails>();
                if (guids.Count > 0)
                {
                    this.Logger. WriteInfo($"      Calling Api.Orders.GetOrdersById for {guids.Count} orders.. .");
                    orders = Api. Orders.GetOrdersById(guids);
                    this.Logger.WriteInfo($"      GetOrdersById returned {orders.Count} order details");
                }

                int beforeChannelFilter = orders. Count;
                orders = orders. Where(order => HasChannelUpdatesRequired(order)).ToList();
                this.Logger.WriteInfo($"      EP filter 'ChannelUpdatesRequired = TRUE':  {beforeChannelFilter} -> {orders.Count} orders");

                if (! string.IsNullOrEmpty(epFilterName))
                {
                    int beforeStatusFilter = orders.Count;
                    orders = orders.Where(order => !HasStatusEPSetToTrue(order, epFilterName)).ToList();
                    this.Logger.WriteInfo($"      EP filter '{epFilterName} != TRUE': {beforeStatusFilter} -> {orders.Count} orders");
                }

                orders = sortingCode == FieldCode. GENERAL_INFO_ORDER_ID
                    ? (sortingDirection == ListSortDirection.Ascending
                        ? orders.OrderBy(o => o.NumOrderId).ToList()
                        : orders.OrderByDescending(o => o.NumOrderId).ToList())
                    : (sortingDirection == ListSortDirection. Ascending
                        ? orders.OrderBy(o => o.GeneralInfo.ReferenceNum).ToList()
                        : orders. OrderByDescending(o => o.GeneralInfo.ReferenceNum).ToList());

                this.Logger.WriteInfo($"      GetFilteredOpenOrders completed.  Returning {orders.Count} orders");
                return orders;
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"      ERROR in GetFilteredOpenOrders: {ex.Message}");
                this.Logger.WriteError($"      Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        private List<OrderDetails> GetFilteredOpenOrdersNoEPFilter(string subSource, string folderName, string sortField, string sortDirection)
        {
            this.Logger.WriteInfo($"      GetFilteredOpenOrdersNoEPFilter started");
            this.Logger.WriteInfo($"      Parameters - subSource: {subSource}, folderName: {folderName}, sortField: {sortField}, sortDirection: {sortDirection}");

            try
            {
                var filter = new FieldsFilter
                {
                    NumericFields = new List<NumericFieldFilter>
                    {
                        new NumericFieldFilter { FieldCode = FieldCode. GENERAL_INFO_STATUS, Type = NumericFieldFilterType.Equal, Value = 1 },
                        new NumericFieldFilter { FieldCode = FieldCode. GENERAL_INFO_PARKED, Type = NumericFieldFilterType. Equal, Value = 0 },
                        new NumericFieldFilter { FieldCode = FieldCode.GENERAL_INFO_LOCKED, Type = NumericFieldFilterType.Equal, Value = 0 }
                    },
                    ListFields = new List<ListFieldFilter>
                    {
                        new ListFieldFilter { FieldCode = FieldCode.FOLDER, Value = folderName, Type = ListFieldFilterType.Is }
                    },
                    TextFields = new List<TextFieldFilter>
                    {
                        new TextFieldFilter { FieldCode = FieldCode.GENERAL_INFO_SUBSOURCE, Text = subSource, Type = TextFieldFilterType.Equal }
                    }
                };

                this.Logger.WriteInfo($"      Filter configured: Status=1 (PAID), Parked=0, Locked=0, Folder={folderName}, SubSource={subSource}");
                this.Logger.WriteInfo($"      NO ExtendedProperty filter applied for actionCancelled (no ChannelUpdatesRequired check)");

                FieldCode sortingCode = sortField.ToUpper() switch
                {
                    "ORDERID" => FieldCode.GENERAL_INFO_ORDER_ID,
                    "REFERENCE" => FieldCode. GENERAL_INFO_REFERENCE_NUMBER,
                    _ => FieldCode.GENERAL_INFO_ORDER_ID
                };

                ListSortDirection sortingDirection = sortDirection.ToUpper() == "ASCENDING"
                    ?  ListSortDirection.Ascending
                    : ListSortDirection. Descending;

                this. Logger.WriteInfo($"      Sorting by: {sortingCode}, Direction: {sortingDirection}");

                this.Logger.WriteInfo($"      Calling Api.Orders.GetAllOpenOrders...");
                var guids = Api. Orders.GetAllOpenOrders(filter, new List<FieldSorting>
                {
                    new FieldSorting { FieldCode = sortingCode, Direction = sortingDirection, Order = 0 }
                }, Guid.Empty, "");

                this.Logger.WriteInfo($"      GetAllOpenOrders returned {guids.Count} order GUIDs");

                var orders = new List<OrderDetails>();
                if (guids.Count > 0)
                {
                    this.Logger.WriteInfo($"      Calling Api.Orders.GetOrdersById for {guids.Count} orders...");
                    orders = Api.Orders.GetOrdersById(guids);
                    this.Logger.WriteInfo($"      GetOrdersById returned {orders.Count} order details");
                }

                orders = sortingCode == FieldCode.GENERAL_INFO_ORDER_ID
                    ? (sortingDirection == ListSortDirection. Ascending
                        ? orders.OrderBy(o => o.NumOrderId).ToList()
                        : orders.OrderByDescending(o => o.NumOrderId).ToList())
                    : (sortingDirection == ListSortDirection.Ascending
                        ? orders.OrderBy(o => o. GeneralInfo.ReferenceNum).ToList()
                        :  orders.OrderByDescending(o => o.GeneralInfo.ReferenceNum).ToList());

                this.Logger.WriteInfo($"      GetFilteredOpenOrdersNoEPFilter completed. Returning {orders.Count} orders");
                return orders;
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"      ERROR in GetFilteredOpenOrdersNoEPFilter: {ex.Message}");
                this.Logger.WriteError($"      Stack trace: {ex. StackTrace}");
                throw;
            }
        }

        private List<OrderDetails> GetFilteredProcessedOrders(string subSource, string source, string sortField, string sortDirection, int lookBackDays, bool isShipped, string epFilterName)
        {
            this.Logger.WriteInfo($"      GetFilteredProcessedOrders started");
            this.Logger.WriteInfo($"      Parameters - subSource: {subSource}, source: {source}, sortField:  {sortField}, sortDirection: {sortDirection}, lookBackDays:  {lookBackDays}, isShipped: {isShipped}, epFilterName: {epFilterName}");

            try
            {
                var orders = new List<OrderDetails>();
                
                DateTime toDate = DateTime. UtcNow.Date.AddDays(1);
                DateTime fromDate = DateTime.UtcNow.Date.AddDays(-lookBackDays);

                this.Logger.WriteInfo($"      Date range: {fromDate: yyyy-MM-ddTHH:mm:ss. fffZ} to {toDate: yyyy-MM-ddTHH:mm:ss.fffZ}");

                var searchFilters = new List<SearchFilters>
                {
                    new SearchFilters { SearchField = SearchFieldTypes.SubSource, SearchTerm = subSource },
                    new SearchFilters { SearchField = SearchFieldTypes.Source, SearchTerm = source }
                };

                this.Logger.WriteInfo($"      SearchFilters:");
                foreach (var filter in searchFilters)
                {
                    this.Logger.WriteInfo($"        SearchField: {filter.SearchField}, SearchTerm: {filter.SearchTerm}");
                }

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

                this.Logger. WriteInfo($"      Search request:  DateField={request.DateField}, FromDate={request.FromDate:yyyy-MM-ddTHH: mm:ss}, ToDate={request.ToDate:yyyy-MM-ddTHH:mm:ss}, PageNumber={request.PageNumber}, ResultsPerPage={request.ResultsPerPage}");
                
                try
                {
                    var serializedRequest = Newtonsoft.Json.JsonConvert.SerializeObject(request);
                    this.Logger.WriteInfo($"      Serialized request JSON: {serializedRequest}");
                }
                catch (Exception serEx)
                {
                    this.Logger.WriteInfo($"      Could not serialize request for logging: {serEx.Message}");
                }

                this.Logger.WriteInfo($"      Calling Api.ProcessedOrders.SearchProcessedOrders...");
                var response = Api.ProcessedOrders.SearchProcessedOrders(request);

                this.Logger.WriteInfo($"      SearchProcessedOrders returned {response. ProcessedOrders?. Data?.Count ??  0} processed orders");

                if (response. ProcessedOrders != null)
                {
                    this.Logger.WriteInfo($"      Response TotalEntries: {response.ProcessedOrders.TotalEntries}");
                    this.Logger.WriteInfo($"      Response TotalPages: {response.ProcessedOrders.TotalPages}");
                    this.Logger.WriteInfo($"      Response PageNumber:  {response.ProcessedOrders. PageNumber}");
                    this.Logger.WriteInfo($"      Response EntriesPerPage: {response. ProcessedOrders.EntriesPerPage}");
                }

                int processedCount = 0;
                int skippedCompletedCount = 0;
                int nullOrderCount = 0;

                if (response.ProcessedOrders?. Data != null)
                {
                    foreach (var processedOrder in response.ProcessedOrders.Data)
                    {
                        processedCount++;
                        this.Logger.WriteInfo($"      Processing order {processedCount}:  pkOrderID={processedOrder.pkOrderID}, NumOrderId={processedOrder.nOrderId}, Source={processedOrder.Source}, SubSource={processedOrder.SubSource}");

                        var orderDetails = Api.Orders.GetOrderById(processedOrder.pkOrderID);
                        if (orderDetails != null)
                        {
                            if (orderDetails.FolderName == null || ! orderDetails.FolderName. Contains("Completed"))
                            {
                                orders.Add(orderDetails);
                                this.Logger.WriteInfo($"        Added order {orderDetails.NumOrderId} (Folder: {string.Join(", ", orderDetails.FolderName ?? new List<string>())})");
                            }
                            else
                            {
                                skippedCompletedCount++;
                                this.Logger.WriteInfo($"        Skipped order {orderDetails.NumOrderId} - already in 'Completed' folder");
                            }
                        }
                        else
                        {
                            nullOrderCount++;
                            this.Logger.WriteInfo($"        Skipped order {processedOrder. pkOrderID} - GetOrderById returned null");
                        }
                    }
                }

                this.Logger.WriteInfo($"      Processing summary: Total={processedCount}, Added={orders.Count}, SkippedCompleted={skippedCompletedCount}, NullOrders={nullOrderCount}");

                int beforeChannelFilter = orders.Count;
                orders = orders.Where(order => HasChannelUpdatesRequired(order)).ToList();
                this.Logger.WriteInfo($"      EP filter 'ChannelUpdatesRequired = TRUE': {beforeChannelFilter} -> {orders. Count} orders");

                if (! string.IsNullOrEmpty(epFilterName))
                {
                    int beforeStatusFilter = orders.Count;
                    orders = orders.Where(order => !HasStatusEPSetToTrue(order, epFilterName)).ToList();
                    this.Logger.WriteInfo($"      EP filter '{epFilterName} != TRUE': {beforeStatusFilter} -> {orders.Count} orders");
                }

                if (sortField.ToUpper() == "ORDERID")
                {
                    orders = sortDirection. ToUpper() == "ASCENDING"
                        ? orders.OrderBy(o => o.NumOrderId).ToList()
                        :  orders.OrderByDescending(o => o.NumOrderId).ToList();
                }
                else
                {
                    orders = sortDirection.ToUpper() == "ASCENDING"
                        ? orders.OrderBy(o => o. GeneralInfo.ReferenceNum).ToList()
                        :  orders.OrderByDescending(o => o.GeneralInfo.ReferenceNum).ToList();
                }

                this.Logger. WriteInfo($"      GetFilteredProcessedOrders completed. Returning {orders.Count} orders");
                return orders;
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"      ERROR in GetFilteredProcessedOrders: {ex.Message}");
                this. Logger.WriteError($"      Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        private string GetExtendedProperty(List<ExtendedProperty> props, string name)
        {
            if (props == null) return "";
            var prop = props.FirstOrDefault(ep => ep.Name == name);
            return prop != null ? prop.Value : "";
        }

        private string EscapeCsvValue(string value)
        {
            if (string.IsNullOrEmpty(value))
                return "";
            
            if (value.Contains(",") || value.Contains("\"") || value.Contains("\n") || value.Contains("\r"))
            {
                return "\"" + value.Replace("\"", "\"\"") + "\"";
            }
            return value;
        }

        private string GenerateStandardCsvLine(OrderDetails order, OrderItem item)
        {
            string primaryPO = GetExtendedProperty(order.ExtendedProperties, "PrimaryPONumber");
            string jdeOrderNo = GetExtendedProperty(order.ExtendedProperties, "JDEOrderNo");
            string soldTo = GetExtendedProperty(order.ExtendedProperties, "SoldTo");

            return string.Join(",",
                EscapeCsvValue(order.NumOrderId.ToString()),
                EscapeCsvValue(order.GeneralInfo.ReferenceNum),
                EscapeCsvValue(order.GeneralInfo.SecondaryReference),
                EscapeCsvValue(order.GeneralInfo. ExternalReferenceNum),
                EscapeCsvValue(primaryPO),
                EscapeCsvValue(jdeOrderNo),
                EscapeCsvValue(soldTo),
                order.GeneralInfo.ReceivedDate.ToString("yyyy-MM-dd"),
                EscapeCsvValue(order.GeneralInfo.Source),
                EscapeCsvValue(order.GeneralInfo. SubSource),
                order.GeneralInfo.DespatchByDate == DateTime.MinValue ? "" : order.GeneralInfo.DespatchByDate.ToString("yyyy-MM-dd"),
                order.GeneralInfo.NumItems.ToString(),
                EscapeCsvValue(order.ShippingInfo.PostalServiceName),
                order.ShippingInfo.TotalWeight.ToString(),
                EscapeCsvValue(order. ShippingInfo.TrackingNumber),
                EscapeCsvValue(item.SKU),
                EscapeCsvValue(item. ChannelSKU),
                EscapeCsvValue(item.Title),
                item.Quantity.ToString(),
                EscapeCsvValue(item.ItemNumber),
                item.PricePerUnit.ToString(),
                item. DiscountValue.ToString(),
                item.TaxRate.ToString(),
                item.Weight.ToString()
            );
        }

        private string SanitizeForFileName(string input)
        {
            if (string.IsNullOrEmpty(input))
                return "";
            
            char[] invalidChars = Path.GetInvalidFileNameChars();
            string sanitized = input;
            
            foreach (char c in invalidChars)
            {
                sanitized = sanitized.Replace(c. ToString(), "");
            }
            
            sanitized = sanitized.Replace(" ", "").Trim();
            
            return sanitized;
        }

        private (StringBuilder Csv, string FileName, Guid[] OrderIds, Guid[] NewFolderOrderIds) FormatStandardCsv(List<OrderDetails> orders, string filePrefix, string localFilePath, string filetype, string subSource, string newFolder)
        {
            this.Logger.WriteInfo($"        FormatStandardCsv started for {orders.Count} orders with prefix '{filePrefix}'");

            try
            {
                string sanitizedSubSource = SanitizeForFileName(subSource);
                string sanitizedFiletype = SanitizeForFileName(filetype);
                string sanitizedPrefix = SanitizeForFileName(filePrefix);
                
                string fileName = $"{sanitizedSubSource}_Orders_{sanitizedPrefix}_{DateTime.Now:yyyyMMddHHmmss}_{sanitizedFiletype}.csv";
                
                this.Logger.WriteInfo($"        Generated filename: {fileName}");

                var csv = new StringBuilder();
                csv.AppendLine(StandardCsvHeader);

                Guid[] orderIds = orders.Select(order => order.OrderId).ToArray();
                this.Logger.WriteInfo($"        Collected {orderIds.Length} order IDs");

                Guid[] newFolderOrderIds = new Guid[0];
                if (! string.IsNullOrEmpty(newFolder))
                {
                    newFolderOrderIds = orders
                        .Where(order => order.FolderName != null && order.FolderName.Any(f => string.Equals(f, newFolder, StringComparison.OrdinalIgnoreCase)))
                        .Select(order => order.OrderId)
                        .ToArray();
                    this.Logger.WriteInfo($"        Collected {newFolderOrderIds.Length} order IDs from '{newFolder}' folder for move to 'Updated'");

                    var ordersNotMoving = orders.Where(order => order.FolderName == null || !order.FolderName.Any(f => string.Equals(f, newFolder, StringComparison.OrdinalIgnoreCase))).ToList();
                    if (ordersNotMoving.Count > 0)
                    {
                        this.Logger.WriteInfo($"        {ordersNotMoving.Count} orders will NOT be moved (not in '{newFolder}' folder):");
                        foreach (var order in ordersNotMoving)
                        {
                            string folderList = order.FolderName != null ? string.Join(", ", order.FolderName) : "No folders";
                            this.Logger.WriteInfo($"          Order {order.NumOrderId} is in folder(s) '{folderList}' - will remain there");
                        }
                    }
                }

                int lineCount = 0;
                foreach (var order in orders)
                {
                    foreach (var item in order.Items)
                    {
                        csv.AppendLine(GenerateStandardCsvLine(order, item));
                        lineCount++;
                    }
                }

                this.Logger.WriteInfo($"        CSV contains {lineCount} item lines from {orders.Count} orders");

                string fullPath = Path.Combine(localFilePath, fileName);
                SaveCsvLocally(csv, fullPath);

                this.Logger.WriteInfo($"        FormatStandardCsv completed successfully");

                return (csv, fileName, orderIds, newFolderOrderIds);
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"        ERROR in FormatStandardCsv: {ex.Message}");
                this.Logger.WriteError($"        Stack trace: {ex. StackTrace}");
                throw;
            }
        }

        private bool SendByFTP(StringBuilder report, SFtpSettings sftpSettings)
        {
            this.Logger.WriteInfo($"        SendByFTP started");
            this.Logger.WriteInfo($"        SFTP Settings: Server={sftpSettings.Server}, Port={sftpSettings.Port}, User={sftpSettings.UserName}, Path={sftpSettings.FullPath}");

            try
            {
                this.Logger.WriteInfo($"        Getting SFTP upload proxy...");
                using var upload = this.ProxyFactory?. GetSFtpUploadProxy(sftpSettings);

                if (upload == null)
                {
                    this.Logger.WriteError($"        ERROR:  SFTP upload proxy is null");
                    throw new Exception("SFTP upload proxy is null.");
                }

                if (report == null)
                {
                    this.Logger.WriteError($"        ERROR: Report content is null");
                    throw new Exception("Report content is null.");
                }

                if (sftpSettings == null)
                {
                    this.Logger.WriteError($"        ERROR: SFTP settings is null");
                    throw new Exception("SFTP settings is null.");
                }

                this.Logger.WriteInfo($"        Writing content to SFTP stream...");
                upload.Write(report.ToString());

                this.Logger.WriteInfo($"        Completing upload...");
                var uploadResult = upload.CompleteUpload();

                if (! uploadResult.IsSuccess)
                {
                    this.Logger.WriteError($"        ERROR: Upload failed - {uploadResult.ErrorMessage}");
                    throw new ArgumentException(uploadResult.ErrorMessage);
                }

                this.Logger.WriteInfo($"        SendByFTP completed successfully");
                return true;
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"        ERROR in SendByFTP: {ex.Message}");
                this.Logger.WriteError($"        Stack trace: {ex.StackTrace}");
                return false;
            }
        }

        private void SaveCsvLocally(StringBuilder csv, string localPath)
        {
            this. Logger.WriteInfo($"        SaveCsvLocally called - Path: {localPath}");

            try
            {
                File.WriteAllText(localPath, csv.ToString());
                this. Logger.WriteInfo($"        File saved successfully to: {localPath}");
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"        ERROR saving file locally: {ex.Message}");
                this.Logger.WriteError($"        Stack trace: {ex. StackTrace}");
                throw;
            }
        }

        private void SendMacroEmail(List<Guid> recipientIds, string subject, string body, string templateType = null)
        {
            this. Logger.WriteInfo($"        SendMacroEmail started");
            this.Logger.WriteInfo($"        Recipients: {string.Join(",", recipientIds)}");
            this.Logger.WriteInfo($"        Subject: {subject}");
            this.Logger.WriteInfo($"        TemplateType: {templateType ??  "null"}");

            try
            {
                var emailRequest = new GenerateFreeTextEmailRequest
                {
                    ids = recipientIds,
                    subject = subject,
                    body = body,
                    templateType = templateType
                };

                this.Logger.WriteInfo("        Calling Api.Email.GenerateFreeTextEmail.. .");
                var emailResponse = Api.Email.GenerateFreeTextEmail(emailRequest);

                if (emailResponse.isComplete)
                {
                    this.Logger.WriteInfo("        Email sent successfully!");
                }
                else
                {
                    this.Logger. WriteError($"        Email failed to send.  Failed recipients: {string.Join(",", emailResponse. FailedRecipients ??  new List<string>())}");
                }
            }
            catch (Exception ex)
            {
                this.Logger. WriteError($"        ERROR in SendMacroEmail: {ex. Message}");
                this.Logger.WriteError($"        Stack trace: {ex.StackTrace}");
                throw;
            }
        }
    }
}