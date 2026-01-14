// UW Home Default Channel Updater
// ================================
// 
// SUMMARY: 
// This Linnworks macro processes orders and generates CSV files for various notification types,
// uploading them via SFTP or saving locally based on the outputMethod parameter.
// After successful file output, the corresponding ExtendedProperty is set to TRUE for each order.
//
// FUNCTIONALITY:
// 1. notifyAcknowledge (Type: Open)
//    - Returns ALL open orders (not just 'New' folder) filtered by subSource
//    - Excludes orders where ExtendedProperty 'statusACK' is TRUE
//    - Only includes orders where 'statusACK' is FALSE or not set
//    - Excludes parked and unpaid orders (Status=1 means PAID)
//    - After processing, moves orders to 'Updated' folder
//    - After processing, sets ExtendedProperty 'statusACK' to TRUE
//
// 2. notifyOOS (Type: Open)
//    - Returns open orders in the specified oosFolder filtered by subSource
//    - Excludes orders where ExtendedProperty 'statusOOS' is TRUE
//    - Only includes orders where 'statusOOS' is FALSE or not set
//    - Excludes parked and unpaid orders
//    - After processing, sets ExtendedProperty 'statusOOS' to TRUE
//
// 3. notifyBIS (Type: Open)
//    - Returns open orders in the specified bisFolder filtered by subSource
//    - Excludes orders where ExtendedProperty 'statusBIS' is TRUE
//    - Only includes orders where 'statusBIS' is FALSE or not set
//    - Excludes parked and unpaid orders
//    - After processing, moves orders to 'Updated' folder
//    - After processing, sets ExtendedProperty 'statusBIS' to TRUE
//
// 4. notifyShipped (Type: Shipped)
//    - Returns processed/shipped orders within lookBackDays
//    - Filtered by subSource and Source
//    - Excludes orders where ExtendedProperty 'StatusASN' is TRUE
//    - Only includes orders where 'StatusASN' is FALSE or not set
//    - Excludes orders already in 'Completed' folder
//    - After processing, sets ExtendedProperty 'StatusASN' to TRUE
//
// 5. actionCancelled (Type: Open)
//    - Returns open orders in the cancelFolder filtered by subSource
//    - NO ExtendedProperty filtering applied
//    - NO ExtendedProperty set after processing
//    - Excludes parked and unpaid orders
//    - Processes orders that are pending cancellation but not yet cancelled
//
// 6. notifyCancelled (Type: Cancelled)
//    - Returns cancelled orders within lookBackDays
//    - Filtered by subSource and Source
//    - Excludes orders where ExtendedProperty 'StatusCANC' is TRUE
//    - Only includes orders where 'StatusCANC' is FALSE or not set
//    - Excludes orders already in 'Completed' folder
//    - After processing, sets ExtendedProperty 'StatusCANC' to TRUE
//
// OUTPUT: 
// - CSV files are either saved locally (outputMethod = "Local") or uploaded via SFTP (outputMethod = "FTP")
// - Email notifications are sent after each file operation
//
// FILTERS APPLIED TO ALL OPEN ORDERS:
// - Status = 1 (Paid - Note: In Linnworks, Status 0=UNPAID, 1=PAID, so filtering Status=1 ensures only paid orders)
// - Parked = 0 (Not parked)
// - Locked = 0 (Not locked)
//
// EXTENDED PROPERTY FILTERS & UPDATES:
// | Notification Type  | EP Filter           | EP Set After Processing |
// |--------------------|---------------------|-------------------------|
// | notifyAcknowledge  | statusACK != TRUE   | statusACK = TRUE        |
// | notifyOOS          | statusOOS != TRUE   | statusOOS = TRUE        |
// | notifyBIS          | statusBIS != TRUE   | statusBIS = TRUE        |
// | notifyShipped      | StatusASN != TRUE   | StatusASN = TRUE        |
// | actionCancelled    | No EP filter        | No EP update            |
// | notifyCancelled    | StatusCANC != TRUE  | StatusCANC = TRUE       |
//
// PARAMETERS:
// - Source: The order source channel
// - subSource: The order sub-source for filtering
// - notifyAcknowledge, notifyOOS, notifyBIS, notifyShipped, actionCancelled, notifyCancelled: TRUE/FALSE flags
// - newFolder, oosFolder, bisFolder, cancelFolder:  Folder names for different order states
// - outputMethod: "Local" or "FTP" - determines where CSV files are saved
// - SFTP*: SFTP connection parameters (used when outputMethod = "FTP")
// - *Directory:  SFTP directory paths for each notification type
// - filetype: File type identifier for naming
// - sortField, sortDirection:  Sorting parameters
// - lookBackDays: Number of days to look back for processed orders
// - localFilePath: Local directory path for saving CSV files

using System;
using System.Collections.Generic;
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
            this.Logger.WriteInfo("Starting macro:  UW Home Default Channel Updater");
            this.Logger.WriteInfo("========================================");
            this.Logger.WriteInfo($"Execution started at: {DateTime. Now: yyyy-MM-dd HH:mm: ss}");
            
            // Log all input parameters for debugging
            this. Logger.WriteInfo("--- Input Parameters ---");
            this.Logger.WriteInfo($"Source: {Source}");
            this.Logger.WriteInfo($"subSource: {subSource}");
            this.Logger.WriteInfo($"notifyAcknowledge:  {notifyAcknowledge}");
            this.Logger.WriteInfo($"notifyOOS: {notifyOOS}");
            this.Logger.WriteInfo($"notifyBIS: {notifyBIS}");
            this.Logger.WriteInfo($"notifyShipped: {notifyShipped}");
            this.Logger.WriteInfo($"actionCancelled:  {actionCancelled}");
            this.Logger.WriteInfo($"notifyCancelled: {notifyCancelled}");
            this.Logger.WriteInfo($"newFolder: {newFolder}");
            this.Logger.WriteInfo($"oosFolder: {oosFolder}");
            this.Logger.WriteInfo($"bisFolder: {bisFolder}");
            this.Logger.WriteInfo($"cancelFolder:  {cancelFolder}");
            this.Logger.WriteInfo($"outputMethod: {outputMethod}");
            this.Logger.WriteInfo($"localFilePath: {localFilePath}");
            this.Logger.WriteInfo($"sortField: {sortField}");
            this.Logger.WriteInfo($"sortDirection:  {sortDirection}");
            this.Logger.WriteInfo($"lookBackDays:  {lookBackDays}");
            this.Logger.WriteInfo($"SFTPServer: {SFTPServer}");
            this.Logger.WriteInfo($"SFTPPort: {SFTPPort}");
            this.Logger.WriteInfo($"SFTPFolderRoot: {SFTPFolderRoot}");
            this.Logger.WriteInfo("------------------------");

            var notificationConfigs = new[]
            {
                new { Notify = notifyAcknowledge, NotifyType = "notifyAcknowledge", Folder = newFolder, Directory = acknowledgeDirectory, Type = "OpenAcknowledge", EPFilter = "statusACK", EPUpdate = "statusACK" },
                new { Notify = notifyOOS, NotifyType = "notifyOOS", Folder = oosFolder, Directory = oosDirectory, Type = "Open", EPFilter = "statusOOS", EPUpdate = "statusOOS" },
                new { Notify = notifyBIS, NotifyType = "notifyBIS", Folder = bisFolder, Directory = bisDirectory, Type = "Open", EPFilter = "statusBIS", EPUpdate = "statusBIS" },
                new { Notify = notifyShipped, NotifyType = "notifyShipped", Folder = shippedDirectory, Directory = shippedDirectory, Type = "Shipped", EPFilter = "StatusASN", EPUpdate = "StatusASN" },
                new { Notify = actionCancelled, NotifyType = "actionCancelled", Folder = cancelFolder, Directory = actionCancelledDirectory, Type = "OpenNoEPFilter", EPFilter = "", EPUpdate = "" },
                new { Notify = notifyCancelled, NotifyType = "notifyCancelled", Folder = cancelDirectory, Directory = cancelDirectory, Type = "Cancelled", EPFilter = "StatusCANC", EPUpdate = "StatusCANC" }
            };

            this.Logger.WriteInfo($"Total notification configs to process: {notificationConfigs.Length}");

            int configIndex = 0;
            foreach (var config in notificationConfigs)
            {
                configIndex++;
                this.Logger.WriteInfo("----------------------------------------");
                this.Logger.WriteInfo($"Processing config {configIndex}/{notificationConfigs.Length}:  {config.NotifyType}");
                this.Logger.WriteInfo($"  Notify flag value: {config. Notify}");
                this.Logger.WriteInfo($"  Folder:  {config.Folder}");
                this.Logger.WriteInfo($"  Directory:  {config.Directory}");
                this.Logger.WriteInfo($"  Type: {config.Type}");
                this.Logger.WriteInfo($"  EP Filter: {(string.IsNullOrEmpty(config.EPFilter) ? "NONE" : config. EPFilter)}");
                this.Logger.WriteInfo($"  EP Update: {(string.IsNullOrEmpty(config. EPUpdate) ? "NONE" : config.EPUpdate)}");

                if (! string. Equals(config. Notify, "TRUE", StringComparison.OrdinalIgnoreCase))
                {
                    this.Logger.WriteInfo($"  SKIPPED: {config.NotifyType} is not set to TRUE");
                    continue;
                }

                this.Logger.WriteInfo($"  Processing {config.NotifyType}.. .");

                List<OrderDetails> orders = new List<OrderDetails>();

                try
                {
                    if (config.Type == "OpenAcknowledge")
                    {
                        this.Logger.WriteInfo($"  Fetching ALL open orders for Acknowledge (filtered by statusACK EP)...");
                        orders = GetFilteredOpenOrdersForAcknowledge(subSource, sortField, sortDirection);
                    }
                    else if (config.Type == "Open")
                    {
                        this.Logger.WriteInfo($"  Fetching open orders from folder '{config.Folder}' with EP filter '{config.EPFilter}'...");
                        orders = GetFilteredOpenOrders(subSource, config.Folder, sortField, sortDirection, config.EPFilter);
                    }
                    else if (config.Type == "OpenNoEPFilter")
                    {
                        this.Logger.WriteInfo($"  Fetching open orders from folder '{config.Folder}' WITHOUT EP filter...");
                        orders = GetFilteredOpenOrdersNoEPFilter(subSource, config.Folder, sortField, sortDirection);
                    }
                    else
                    {
                        bool isShipped = config.Type == "Shipped";
                        this.Logger.WriteInfo($"  Fetching processed orders (isShipped: {isShipped}) with EP filter '{config. EPFilter}'...");
                        orders = GetFilteredProcessedOrders(subSource, Source, sortField, sortDirection, lookBackDays, isShipped, config. EPFilter);
                    }

                    this.Logger.WriteInfo($"  Orders retrieved: {orders.Count}");

                    if (orders.Count == 0)
                    {
                        this.Logger.WriteInfo($"  No orders found for {config.NotifyType} - folder: {config.Folder}.  Skipping to next config.");
                        continue;
                    }

                    // Log order IDs for debugging
                    this.Logger.WriteInfo($"  Order IDs found: {string.Join(", ", orders. Select(o => o.NumOrderId))}");
                }
                catch (Exception ex)
                {
                    this.Logger.WriteError($"  ERROR fetching orders for {config.NotifyType}:  {ex.Message}");
                    this.Logger.WriteError($"  Stack trace: {ex. StackTrace}");
                    continue;
                }

                (StringBuilder Csv, string FileName, Guid[] OrderIds) result;

                try
                {
                    this.Logger.WriteInfo($"  Formatting CSV for {config.NotifyType}...");

                    switch (config.NotifyType)
                    {
                        case "notifyAcknowledge":
                            result = FormatnotifyAcknowledge(orders, config. Folder, localFilePath, filetype);
                            break;
                        case "notifyOOS": 
                            result = FormatnotifyOOS(orders, config.Folder, localFilePath, filetype);
                            break;
                        case "notifyBIS":
                            result = FormatnotifyBIS(orders, config.Folder, localFilePath, filetype);
                            break;
                        case "notifyShipped":
                            result = FormatnotifyShipped(orders, config.Folder, localFilePath, filetype);
                            break;
                        case "actionCancelled":
                            result = FormatactionCancelled(orders, config. Folder, localFilePath, filetype);
                            break;
                        case "notifyCancelled": 
                            result = FormatnotifyCancelled(orders, config.Folder, localFilePath, filetype);
                            break;
                        default:
                            this.Logger.WriteError($"  Unknown NotifyType: {config.NotifyType}.  Using default CSV format.");
                            var csv = GenerateCsv(orders);
                            var fileName = $"Orders_FAILED_{config. Folder}_{DateTime.Now:yyyyMMddHHmmss}_{filetype}.csv";
                            SaveCsvLocally(csv, Path.Combine(localFilePath, fileName));
                            result = (csv, fileName, new Guid[0]);
                            break;
                    }

                    this.Logger.WriteInfo($"  CSV formatted successfully. FileName: {result.FileName}");
                    this.Logger.WriteInfo($"  OrderIds to process after output: {result.OrderIds?. Length ?? 0}");
                }
                catch (Exception ex)
                {
                    this.Logger.WriteError($"  ERROR formatting CSV for {config.NotifyType}: {ex.Message}");
                    this.Logger.WriteError($"  Stack trace: {ex.StackTrace}");
                    continue;
                }

                string emailSubject;
                string emailBody;
                bool outputSuccess = false;

                try
                {
                    // Determine output method
                    if (string.Equals(outputMethod, "FTP", StringComparison.OrdinalIgnoreCase))
                    {
                        this.Logger.WriteInfo($"  Output method:  FTP");

                        // Configure SFTP settings for file upload
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
                        this.Logger.WriteInfo($"    User: {sftpSettings.UserName}");
                        this.Logger.WriteInfo($"    FullPath: {sftpSettings.FullPath}");

                        // Upload the CSV to the SFTP server
                        this.Logger.WriteInfo($"  Attempting SFTP upload...");

                        if (SendByFTP(result. Csv, sftpSettings))
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
                        string fullLocalPath = Path. Combine(localFilePath, result.FileName);
                        this.Logger.WriteInfo($"  CSV already saved locally at: {fullLocalPath}");
                        outputSuccess = true;

                        emailSubject = $"Local Save Successful for {config.Folder}";
                        emailBody = $"The CSV file '{result. FileName}' was successfully saved locally at '{fullLocalPath}'.";
                    }

                    // Only process order updates if output was successful
                    if (outputSuccess)
                    {
                        this.Logger.WriteInfo($"  Output successful - processing order updates for {config.NotifyType}...");
                        
                        // Process folder moves
                        ProcessOrderFolderUpdates(config.NotifyType, result.OrderIds);
                        
                        // Set ExtendedProperty to TRUE for processed orders
                        if (! string.IsNullOrEmpty(config.EPUpdate))
                        {
                            SetOrderExtendedProperty(result.OrderIds, config.EPUpdate, "TRUE");
                        }
                        else
                        {
                            this.Logger.WriteInfo($"  No ExtendedProperty update configured for {config.NotifyType}");
                        }
                    }
                    else
                    {
                        this.Logger.WriteInfo($"  Output failed - skipping order updates for {config.NotifyType}");
                    }
                }
                catch (Exception ex)
                {
                    this.Logger.WriteError($"  ERROR during output for {config.NotifyType}: {ex. Message}");
                    this.Logger.WriteError($"  Stack trace: {ex.StackTrace}");
                    emailSubject = $"Error Processing {config. Folder}";
                    emailBody = $"An error occurred while processing {config.NotifyType}:  {ex.Message}";
                }

                // Send the email
                try
                {
                    this.Logger.WriteInfo($"  Sending notification email.. .");
                    SendMacroEmail(
                        new List<Guid> { Guid.Parse("6665d96a-ef96-46bc-a172-291b29785fbe") },
                        emailSubject,
                        emailBody
                    );
                    this.Logger.WriteInfo($"  Email sent successfully.");
                }
                catch (Exception ex)
                {
                    this.Logger.WriteError($"  ERROR sending email for {config.NotifyType}: {ex. Message}");
                    this.Logger.WriteError($"  Stack trace: {ex.StackTrace}");
                }

                this.Logger.WriteInfo($"  Completed processing {config.NotifyType}");
            }

            this.Logger.WriteInfo("========================================");
            this.Logger.WriteInfo($"Macro export complete at: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
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
                else
                {
                    this.Logger.WriteInfo($"    No folder move required for {notifyType}");
                }
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"    ERROR in ProcessOrderFolderUpdates for {notifyType}:  {ex.Message}");
                this.Logger.WriteError($"    Stack trace:  {ex.StackTrace}");
            }
        }

        /// <summary>
        /// Sets or updates a single ExtendedProperty on orders while preserving all existing properties. 
        /// This method gets all existing properties, updates/adds the specified property, then saves them all back.
        /// </summary>
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

                    // Get ALL existing extended properties for this order
                    var existingProperties = Api.Orders.GetExtendedProperties(orderId);
                    this.Logger.WriteInfo($"      Retrieved {existingProperties?. Count ??  0} existing properties for order {orderId}");

                    // Log existing properties for debugging
                    if (existingProperties != null && existingProperties.Count > 0)
                    {
                        this.Logger.WriteInfo($"      Existing properties: {string.Join(", ", existingProperties. Select(p => $"{p.Name}={p.Value}"))}");
                    }

                    // Create a list to hold all properties (existing + updated/new)
                    var allProperties = new List<ExtendedProperty>();

                    // Find if the property already exists
                    bool propertyFound = false;

                    if (existingProperties != null)
                    {
                        foreach (var prop in existingProperties)
                        {
                            if (string.Equals(prop.Name, propertyName, StringComparison.OrdinalIgnoreCase))
                            {
                                // Update this property with the new value
                                this.Logger.WriteInfo($"      Property '{propertyName}' exists (RowId: {prop. RowId}). Updating value from '{prop.Value}' to '{propertyValue}'.. .");
                                allProperties.Add(new ExtendedProperty
                                {
                                    RowId = prop. RowId,
                                    Name = prop.Name,
                                    Value = propertyValue,
                                    Type = prop.Type ??  "Order"
                                });
                                propertyFound = true;
                            }
                            else
                            {
                                // Keep the existing property unchanged
                                allProperties.Add(new ExtendedProperty
                                {
                                    RowId = prop.RowId,
                                    Name = prop. Name,
                                    Value = prop. Value,
                                    Type = prop. Type ?? "Order"
                                });
                            }
                        }
                    }

                    // If property doesn't exist, add it as new
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

                    this.Logger.WriteInfo($"      Saving {allProperties.Count} properties back to order {orderId}...");

                    // Save ALL properties back (this replaces all, but we're passing all of them)
                    Api.Orders.SetExtendedProperties(orderId, allProperties. ToArray());

                    this.Logger.WriteInfo($"      Successfully updated EP '{propertyName}' for order {orderId} (preserved {allProperties.Count - 1} other properties)");
                    successCount++;
                }
                catch (Exception ex)
                {
                    errorCount++;
                    this.Logger.WriteError($"      ERROR setting EP for order {orderId}:  {ex.Message}");
                    this.Logger.WriteError($"      Stack trace: {ex. StackTrace}");
                }
            }

            this.Logger.WriteInfo($"    SetOrderExtendedProperty completed.  Success: {successCount}, Errors: {errorCount}");
        }

        private List<OrderDetails> GetFilteredOpenOrdersForAcknowledge(string subSource, string sortField, string sortDirection)
        {
            this.Logger.WriteInfo($"      GetFilteredOpenOrdersForAcknowledge started");
            this.Logger.WriteInfo($"      Parameters - subSource: {subSource}, sortField: {sortField}, sortDirection: {sortDirection}");

            try
            {
                // Get ALL open orders filtered by subSource, excluding parked, locked, and unpaid
                // Note: Status=1 means PAID in Linnworks (0=UNPAID, 1=PAID, 2=RETURN, 3=PENDING, 4=RESEND)
                // Then filter by statusACK extended property
                var filter = new FieldsFilter
                {
                    NumericFields = new List<NumericFieldFilter>
                    {
                        new NumericFieldFilter { FieldCode = FieldCode. GENERAL_INFO_STATUS, Type = NumericFieldFilterType.Equal, Value = 1 },
                        new NumericFieldFilter { FieldCode = FieldCode.GENERAL_INFO_PARKED, Type = NumericFieldFilterType. Equal, Value = 0 },
                        new NumericFieldFilter { FieldCode = FieldCode. GENERAL_INFO_LOCKED, Type = NumericFieldFilterType.Equal, Value = 0 }
                    },
                    TextFields = new List<TextFieldFilter>
                    {
                        new TextFieldFilter { FieldCode = FieldCode. GENERAL_INFO_SUBSOURCE, Text = subSource, Type = TextFieldFilterType.Equal }
                    }
                };

                this.Logger.WriteInfo($"      Filter configured: Status=1 (PAID), Parked=0, Locked=0, SubSource={subSource}");

                FieldCode sortingCode = sortField. ToUpper() switch
                {
                    "ORDERID" => FieldCode.GENERAL_INFO_ORDER_ID,
                    "REFERENCE" => FieldCode.GENERAL_INFO_REFERENCE_NUMBER,
                    _ => FieldCode. GENERAL_INFO_ORDER_ID
                };

                ListSortDirection sortingDirection = sortDirection. ToUpper() == "ASCENDING"
                    ? ListSortDirection. Ascending
                    :  ListSortDirection. Descending;

                this.Logger.WriteInfo($"      Sorting by:  {sortingCode}, Direction: {sortingDirection}");

                this.Logger.WriteInfo($"      Calling Api.Orders.GetAllOpenOrders.. .");
                var guids = Api.Orders. GetAllOpenOrders(filter, new List<FieldSorting>
                {
                    new FieldSorting { FieldCode = sortingCode, Direction = sortingDirection, Order = 0 }
                }, Guid.Empty, "");

                this.Logger.WriteInfo($"      GetAllOpenOrders returned {guids.Count} order GUIDs");

                var orders = new List<OrderDetails>();
                if (guids.Count > 0)
                {
                    this.Logger.WriteInfo($"      Calling Api.Orders.GetOrdersById for {guids. Count} orders...");
                    orders = Api.Orders. GetOrdersById(guids);
                    this.Logger.WriteInfo($"      GetOrdersById returned {orders.Count} order details");
                }

                // Filter out orders where statusACK is TRUE (only include FALSE or not set)
                int beforeFilterCount = orders.Count;
                orders = orders.Where(order =>
                    !order.ExtendedProperties.Any(ep =>
                        ep. Name == "statusACK" &&
                        string.Equals(ep.Value, "TRUE", StringComparison.OrdinalIgnoreCase)
                    )
                ).ToList();

                this.Logger.WriteInfo($"      EP filter 'statusACK != TRUE':  {beforeFilterCount} -> {orders.Count} orders");

                // Log details of filtered orders
                if (beforeFilterCount > orders.Count)
                {
                    this.Logger.WriteInfo($"      {beforeFilterCount - orders.Count} orders excluded due to statusACK = TRUE");
                }

                // Sort orders
                orders = sortingCode == FieldCode. GENERAL_INFO_ORDER_ID
                    ? (sortingDirection == ListSortDirection.Ascending
                        ? orders. OrderBy(o => o.NumOrderId).ToList()
                        : orders.OrderByDescending(o => o.NumOrderId).ToList())
                    : (sortingDirection == ListSortDirection. Ascending
                        ?  orders.OrderBy(o => o.GeneralInfo.ReferenceNum).ToList()
                        : orders. OrderByDescending(o => o.GeneralInfo.ReferenceNum).ToList());

                this. Logger.WriteInfo($"      GetFilteredOpenOrdersForAcknowledge completed.  Returning {orders.Count} orders");
                return orders;
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"      ERROR in GetFilteredOpenOrdersForAcknowledge: {ex. Message}");
                this.Logger.WriteError($"      Stack trace: {ex. StackTrace}");
                throw;
            }
        }

        private List<OrderDetails> GetFilteredOpenOrders(string subSource, string folderName, string sortField, string sortDirection, string epFilterName)
        {
            this.Logger.WriteInfo($"      GetFilteredOpenOrders started");
            this.Logger.WriteInfo($"      Parameters - subSource: {subSource}, folderName: {folderName}, sortField:  {sortField}, sortDirection: {sortDirection}, epFilterName:  {epFilterName}");

            try
            {
                // Note: Status=1 means PAID in Linnworks (0=UNPAID, 1=PAID, 2=RETURN, 3=PENDING, 4=RESEND)
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
                        new ListFieldFilter { FieldCode = FieldCode. FOLDER, Value = folderName, Type = ListFieldFilterType.Is }
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

                ListSortDirection sortingDirection = sortDirection.ToUpper() == "ASCENDING"
                    ? ListSortDirection.Ascending
                    : ListSortDirection.Descending;

                this.Logger.WriteInfo($"      Sorting by: {sortingCode}, Direction: {sortingDirection}");

                this.Logger.WriteInfo($"      Calling Api. Orders.GetAllOpenOrders...");
                var guids = Api.Orders.GetAllOpenOrders(filter, new List<FieldSorting>
                {
                    new FieldSorting { FieldCode = sortingCode, Direction = sortingDirection, Order = 0 }
                }, Guid. Empty, "");

                this.Logger.WriteInfo($"      GetAllOpenOrders returned {guids. Count} order GUIDs");

                var orders = new List<OrderDetails>();
                if (guids. Count > 0)
                {
                    this.Logger.WriteInfo($"      Calling Api.Orders.GetOrdersById for {guids. Count} orders...");
                    orders = Api.Orders.GetOrdersById(guids);
                    this.Logger.WriteInfo($"      GetOrdersById returned {orders.Count} order details");
                }

                // Apply ExtendedProperty filter
                if (!string.IsNullOrEmpty(epFilterName))
                {
                    int beforeFilterCount = orders.Count;
                    orders = orders. Where(order =>
                        !order. ExtendedProperties. Any(ep =>
                            ep.Name == epFilterName &&
                            string. Equals(ep. Value, "TRUE", StringComparison. OrdinalIgnoreCase)
                        )
                    ).ToList();

                    this.Logger.WriteInfo($"      EP filter '{epFilterName} != TRUE': {beforeFilterCount} -> {orders.Count} orders");

                    if (beforeFilterCount > orders.Count)
                    {
                        this.Logger.WriteInfo($"      {beforeFilterCount - orders.Count} orders excluded due to {epFilterName} = TRUE");
                    }
                }

                // Sort orders
                orders = sortingCode == FieldCode.GENERAL_INFO_ORDER_ID
                    ? (sortingDirection == ListSortDirection.Ascending
                        ? orders.OrderBy(o => o.NumOrderId).ToList()
                        : orders.OrderByDescending(o => o. NumOrderId).ToList())
                    : (sortingDirection == ListSortDirection.Ascending
                        ? orders.OrderBy(o => o.GeneralInfo. ReferenceNum).ToList()
                        : orders.OrderByDescending(o => o.GeneralInfo.ReferenceNum).ToList());

                this.Logger.WriteInfo($"      GetFilteredOpenOrders completed. Returning {orders.Count} orders");
                return orders;
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"      ERROR in GetFilteredOpenOrders:  {ex.Message}");
                this.Logger.WriteError($"      Stack trace:  {ex.StackTrace}");
                throw;
            }
        }

        private List<OrderDetails> GetFilteredOpenOrdersNoEPFilter(string subSource, string folderName, string sortField, string sortDirection)
        {
            this.Logger.WriteInfo($"      GetFilteredOpenOrdersNoEPFilter started");
            this.Logger.WriteInfo($"      Parameters - subSource: {subSource}, folderName: {folderName}, sortField: {sortField}, sortDirection: {sortDirection}");

            try
            {
                // Note: Status=1 means PAID in Linnworks (0=UNPAID, 1=PAID, 2=RETURN, 3=PENDING, 4=RESEND)
                var filter = new FieldsFilter
                {
                    NumericFields = new List<NumericFieldFilter>
                    {
                        new NumericFieldFilter { FieldCode = FieldCode. GENERAL_INFO_STATUS, Type = NumericFieldFilterType.Equal, Value = 1 },
                        new NumericFieldFilter { FieldCode = FieldCode. GENERAL_INFO_PARKED, Type = NumericFieldFilterType.Equal, Value = 0 },
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
                this.Logger.WriteInfo($"      NO ExtendedProperty filter applied for actionCancelled");

                FieldCode sortingCode = sortField.ToUpper() switch
                {
                    "ORDERID" => FieldCode.GENERAL_INFO_ORDER_ID,
                    "REFERENCE" => FieldCode. GENERAL_INFO_REFERENCE_NUMBER,
                    _ => FieldCode.GENERAL_INFO_ORDER_ID
                };

                ListSortDirection sortingDirection = sortDirection.ToUpper() == "ASCENDING"
                    ?  ListSortDirection. Ascending
                    : ListSortDirection.Descending;

                this.Logger.WriteInfo($"      Sorting by:  {sortingCode}, Direction: {sortingDirection}");

                this.Logger.WriteInfo($"      Calling Api.Orders. GetAllOpenOrders...");
                var guids = Api. Orders.GetAllOpenOrders(filter, new List<FieldSorting>
                {
                    new FieldSorting { FieldCode = sortingCode, Direction = sortingDirection, Order = 0 }
                }, Guid.Empty, "");

                this.Logger.WriteInfo($"      GetAllOpenOrders returned {guids.Count} order GUIDs");

                var orders = new List<OrderDetails>();
                if (guids.Count > 0)
                {
                    this.Logger.WriteInfo($"      Calling Api.Orders.GetOrdersById for {guids.Count} orders...");
                    orders = Api. Orders.GetOrdersById(guids);
                    this.Logger.WriteInfo($"      GetOrdersById returned {orders. Count} order details");
                }

                // Sort orders
                orders = sortingCode == FieldCode.GENERAL_INFO_ORDER_ID
                    ?  (sortingDirection == ListSortDirection. Ascending
                        ? orders.OrderBy(o => o. NumOrderId).ToList()
                        : orders.OrderByDescending(o => o.NumOrderId).ToList())
                    : (sortingDirection == ListSortDirection.Ascending
                        ? orders.OrderBy(o => o.GeneralInfo.ReferenceNum).ToList()
                        : orders.OrderByDescending(o => o. GeneralInfo.ReferenceNum).ToList());

                this.Logger.WriteInfo($"      GetFilteredOpenOrdersNoEPFilter completed.  Returning {orders. Count} orders");
                return orders;
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"      ERROR in GetFilteredOpenOrdersNoEPFilter: {ex.Message}");
                this.Logger.WriteError($"      Stack trace: {ex. StackTrace}");
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
            string epFilterName
        )
        {
            this.Logger.WriteInfo($"      GetFilteredProcessedOrders started");
            this.Logger.WriteInfo($"      Parameters - subSource: {subSource}, source: {source}, sortField: {sortField}, sortDirection:  {sortDirection}, lookBackDays: {lookBackDays}, isShipped: {isShipped}, epFilterName: {epFilterName}");

            try
            {
                var orders = new List<OrderDetails>();
                DateTime toDate = DateTime.Today;
                DateTime fromDate = toDate.AddDays(-lookBackDays);

                this.Logger.WriteInfo($"      Date range: {fromDate:yyyy-MM-dd} to {toDate:yyyy-MM-dd}");

                var searchFilters = new List<SearchFilters>
                {
                    new SearchFilters { SearchField = SearchFieldTypes.SubSource, SearchTerm = subSource },
                    new SearchFilters { SearchField = SearchFieldTypes. Source, SearchTerm = source }
                };

                var request = new SearchProcessedOrdersRequest
                {
                    SearchFilters = searchFilters,
                    DateField = isShipped ? DateField.processed : DateField.cancelled,
                    FromDate = fromDate,
                    ToDate = toDate,
                    PageNumber = 1,
                    ResultsPerPage = 200
                };

                this.Logger.WriteInfo($"      Search request:  DateField={request.DateField}, PageNumber={request.PageNumber}, ResultsPerPage={request.ResultsPerPage}");

                this.Logger.WriteInfo($"      Calling Api.ProcessedOrders.SearchProcessedOrders.. .");
                var response = Api.ProcessedOrders.SearchProcessedOrders(request);

                this.Logger.WriteInfo($"      SearchProcessedOrders returned {response.ProcessedOrders?. Data?.Count ??  0} processed orders");

                int processedCount = 0;
                int skippedCompletedCount = 0;
                int nullOrderCount = 0;

                foreach (var processedOrder in response. ProcessedOrders. Data)
                {
                    processedCount++;
                    this.Logger.WriteInfo($"      Processing order {processedCount}:  pkOrderID={processedOrder. pkOrderID}");

                    var orderDetails = Api.Orders.GetOrderById(processedOrder. pkOrderID);
                    if (orderDetails != null)
                    {
                        if (! orderDetails.FolderName.Contains("Completed"))
                        {
                            orders.Add(orderDetails);
                            this.Logger.WriteInfo($"        Added order {orderDetails.NumOrderId} (Folder: {orderDetails. FolderName})");
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
                        this.Logger.WriteInfo($"        Skipped order {processedOrder.pkOrderID} - GetOrderById returned null");
                    }
                }

                this.Logger.WriteInfo($"      Processing summary: Total={processedCount}, Added={orders.Count}, SkippedCompleted={skippedCompletedCount}, NullOrders={nullOrderCount}");

                // Apply ExtendedProperty filter
                if (! string.IsNullOrEmpty(epFilterName))
                {
                    int beforeFilterCount = orders. Count;
                    orders = orders.Where(order =>
                        !order.ExtendedProperties.Any(ep =>
                            ep.Name == epFilterName &&
                            string. Equals(ep.Value, "TRUE", StringComparison.OrdinalIgnoreCase)
                        )
                    ).ToList();

                    this.Logger.WriteInfo($"      EP filter '{epFilterName} != TRUE': {beforeFilterCount} -> {orders.Count} orders");

                    if (beforeFilterCount > orders.Count)
                    {
                        this.Logger.WriteInfo($"      {beforeFilterCount - orders.Count} orders excluded due to {epFilterName} = TRUE");
                    }
                }

                // Sorting
                if (sortField.ToUpper() == "ORDERID")
                {
                    orders = sortDirection. ToUpper() == "ASCENDING"
                        ? orders.OrderBy(o => o.NumOrderId).ToList()
                        : orders.OrderByDescending(o => o. NumOrderId).ToList();
                }
                else
                {
                    orders = sortDirection.ToUpper() == "ASCENDING"
                        ? orders.OrderBy(o => o.GeneralInfo. ReferenceNum).ToList()
                        : orders.OrderByDescending(o => o.GeneralInfo.ReferenceNum).ToList();
                }

                this.Logger.WriteInfo($"      GetFilteredProcessedOrders completed. Returning {orders.Count} orders");
                return orders;
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"      ERROR in GetFilteredProcessedOrders: {ex.Message}");
                this.Logger.WriteError($"      Stack trace: {ex. StackTrace}");
                throw;
            }
        }

        private StringBuilder GenerateCsv(List<OrderDetails> orders)
        {
            this.Logger.WriteInfo($"        GenerateCsv called for {orders.Count} orders");

            var csv = new StringBuilder();
            csv.AppendLine("OrderNumber,CustomerName,Address,Items");

            foreach (var order in orders)
            {
                var address = $"{order.CustomerInfo.Address. FullName}, {order.CustomerInfo.Address.Address1}, {order.CustomerInfo.Address.Address2}, {order.CustomerInfo.Address.Town}, {order.CustomerInfo.Address.PostCode}";
                var items = string.Join(";", order.Items. Select(i => $"{i.SKU} x{i.Quantity}"));
                csv.AppendLine($"{order. NumOrderId},{order.CustomerInfo.Address.FullName},\"{address}\",\"{items}\"");
            }

            this.Logger.WriteInfo($"        GenerateCsv completed.  CSV has {orders.Count} data rows");
            return csv;
        }

        private bool SendByFTP(StringBuilder report, SFtpSettings sftpSettings)
        {
            this.Logger.WriteInfo($"        SendByFTP started");
            this.Logger.WriteInfo($"        SFTP Settings: Server={sftpSettings.Server}, Port={sftpSettings.Port}, User={sftpSettings.UserName}, Path={sftpSettings. FullPath}");

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
                    this.Logger.WriteError($"        ERROR:  Report content is null");
                    throw new Exception("Report content is null.");
                }

                if (sftpSettings == null)
                {
                    this.Logger.WriteError($"        ERROR:  SFTP settings is null");
                    throw new Exception("SFTP settings is null.");
                }

                this.Logger.WriteInfo($"        Writing content to SFTP stream...");
                upload.Write(report.ToString());

                this.Logger.WriteInfo($"        Completing upload...");
                var uploadResult = upload.CompleteUpload();

                if (! uploadResult.IsSuccess)
                {
                    this.Logger.WriteError($"        ERROR:  Upload failed - {uploadResult.ErrorMessage}");
                    throw new ArgumentException(uploadResult.ErrorMessage);
                }

                this.Logger.WriteInfo($"        SendByFTP completed successfully");
                return true;
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"        ERROR in SendByFTP: {ex. Message}");
                this.Logger.WriteError($"        Stack trace: {ex. StackTrace}");
                return false;
            }
        }

        private void SaveCsvLocally(StringBuilder csv, string localPath)
        {
            this.Logger.WriteInfo($"        SaveCsvLocally called - Path: {localPath}");

            try
            {
                File.WriteAllText(localPath, csv. ToString());
                this.Logger.WriteInfo($"        File saved successfully to:  {localPath}");
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"        ERROR saving file locally: {ex.Message}");
                this.Logger.WriteError($"        Stack trace: {ex. StackTrace}");
                throw;
            }
        }

        private (StringBuilder Csv, string FileName, Guid[] OrderIds) FormatnotifyAcknowledge(List<OrderDetails> orders, string folder, string localFilePath, string filetype)
        {
            this.Logger.WriteInfo($"        FormatnotifyAcknowledge started for {orders.Count} orders");

            try
            {
                // Set the file name and type
                string fileName = $"Orders_Acknowledge_{DateTime. Now:yyyyMMddHHmmss}_{filetype}.csv";
                this.Logger.WriteInfo($"        Generated filename: {fileName}");

                // Create the CSV content
                var csv = new StringBuilder();

                // Set the CSV header
                csv.AppendLine("Linnworks Order Number,Reference Num,Secondary Ref,External Ref,Primary PO Field,JDE Order Number,Sold To Account,Received Date,Source,Sub Source,Despatch By Date,Number Order Items,Postal Service Name,Total Order Weight,Tracking Number,Item > SKU,Item > ChannelSKU,Item > Description,Item > Quantity,Item > Line Ref,Item > Item Cost (ex VAT),Item > Item Discount (ex VAT),Item > Tax Rate,Item > Weight Per Item");

                // Helper to get ExtendedProperty value by name
                string GetExtendedProperty(List<ExtendedProperty> props, string name)
                {
                    if (props == null) return "";
                    var prop = props.FirstOrDefault(ep => ep.Name == name);
                    return prop != null ?  prop.Value : "";
                }

                // Create an array to record all order GUIDs
                Guid[] orderIds = orders.Select(order => order.OrderId).ToArray();
                this.Logger.WriteInfo($"        Collected {orderIds.Length} order IDs for post-processing");

                int lineCount = 0;
                foreach (var order in orders)
                {
                    string primaryPO = GetExtendedProperty(order.ExtendedProperties, "PrimaryPONumber");
                    string jdeOrderNo = GetExtendedProperty(order.ExtendedProperties, "JDEOrderNo");
                    string soldTo = GetExtendedProperty(order.ExtendedProperties, "SoldTo");

                    foreach (var item in order.Items)
                    {
                        csv.AppendLine($"{order.NumOrderId},{order.GeneralInfo.ReferenceNum},{order.GeneralInfo. SecondaryReference},{order.GeneralInfo.ExternalReferenceNum},{primaryPO},{jdeOrderNo},{soldTo},{order. GeneralInfo.ReceivedDate: yyyy-MM-dd},{order.GeneralInfo.Source},{order.GeneralInfo. SubSource},{order. GeneralInfo.DespatchByDate: yyyy-MM-dd},{order.GeneralInfo.NumItems},{order.ShippingInfo.PostalServiceName},{order.ShippingInfo.TotalWeight},{order.ShippingInfo.TrackingNumber},{item. SKU},{item. ChannelSKU},{item.Title},{item. Quantity},{item.ItemNumber},{item.PricePerUnit},{item. DiscountValue},{item.TaxRate},{item.Weight}");
                        lineCount++;
                    }
                }

                this.Logger.WriteInfo($"        CSV contains {lineCount} item lines from {orders.Count} orders");

                // Save the CSV locally
                string fullPath = Path. Combine(localFilePath, fileName);
                SaveCsvLocally(csv, fullPath);

                this.Logger.WriteInfo($"        FormatnotifyAcknowledge completed successfully");

                // Return the CSV, filename, and orderIds array
                return (csv, fileName, orderIds);
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"        ERROR in FormatnotifyAcknowledge: {ex.Message}");
                this.Logger.WriteError($"        Stack trace: {ex. StackTrace}");
                throw;
            }
        }

        private (StringBuilder Csv, string FileName, Guid[] OrderIds) FormatnotifyOOS(List<OrderDetails> orders, string folder, string localFilePath, string filetype)
        {
            this.Logger.WriteInfo($"        FormatnotifyOOS started for {orders. Count} orders");

            try
            {
                string fileName = $"Orders_OOS_{folder}_{DateTime.Now:yyyyMMddHHmmss}_{filetype}.csv";
                this. Logger.WriteInfo($"        Generated filename:  {fileName}");

                var csv = GenerateCsv(orders);

                string fullPath = Path. Combine(localFilePath, fileName);
                SaveCsvLocally(csv, fullPath);

                Guid[] orderIds = orders.Select(order => order.OrderId).ToArray();
                this.Logger.WriteInfo($"        FormatnotifyOOS completed.  OrderIds: {orderIds.Length}");

                return (csv, fileName, orderIds);
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"        ERROR in FormatnotifyOOS: {ex.Message}");
                this.Logger.WriteError($"        Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        private (StringBuilder Csv, string FileName, Guid[] OrderIds) FormatnotifyBIS(List<OrderDetails> orders, string folder, string localFilePath, string filetype)
        {
            this.Logger.WriteInfo($"        FormatnotifyBIS started for {orders. Count} orders");

            try
            {
                string fileName = $"Orders_BIS_{folder}_{DateTime.Now:yyyyMMddHHmmss}_{filetype}.csv";
                this.Logger.WriteInfo($"        Generated filename: {fileName}");

                var csv = GenerateCsv(orders);

                string fullPath = Path.Combine(localFilePath, fileName);
                SaveCsvLocally(csv, fullPath);

                Guid[] orderIds = orders. Select(order => order.OrderId).ToArray();
                this.Logger.WriteInfo($"        FormatnotifyBIS completed. OrderIds: {orderIds.Length}");

                return (csv, fileName, orderIds);
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"        ERROR in FormatnotifyBIS:  {ex.Message}");
                this.Logger.WriteError($"        Stack trace:  {ex.StackTrace}");
                throw;
            }
        }

        private (StringBuilder Csv, string FileName, Guid[] OrderIds) FormatnotifyShipped(List<OrderDetails> orders, string folder, string localFilePath, string filetype)
        {
            this.Logger.WriteInfo($"        FormatnotifyShipped started for {orders.Count} orders");

            try
            {
                string fileName = $"Orders_Shipped_{DateTime.Now:yyyyMMddHHmmss}_{filetype}.csv";
                this.Logger.WriteInfo($"        Generated filename: {fileName}");

                var csv = GenerateCsv(orders);

                string fullPath = Path.Combine(localFilePath, fileName);
                SaveCsvLocally(csv, fullPath);

                Guid[] orderIds = orders.Select(order => order. OrderId).ToArray();
                this. Logger.WriteInfo($"        FormatnotifyShipped completed.  OrderIds: {orderIds.Length}");

                return (csv, fileName, orderIds);
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"        ERROR in FormatnotifyShipped: {ex.Message}");
                this.Logger.WriteError($"        Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        private (StringBuilder Csv, string FileName, Guid[] OrderIds) FormatactionCancelled(List<OrderDetails> orders, string folder, string localFilePath, string filetype)
        {
            this.Logger.WriteInfo($"        FormatactionCancelled started for {orders.Count} orders");

            try
            {
                // Set the file name for action cancelled orders
                string fileName = $"Orders_ActionCancelled_{folder}_{DateTime.Now:yyyyMMddHHmmss}_{filetype}.csv";
                this.Logger.WriteInfo($"        Generated filename:  {fileName}");

                // Create the CSV content
                var csv = new StringBuilder();

                // Set the CSV header for action cancelled orders
                csv.AppendLine("Linnworks Order Number,Reference Num,Secondary Ref,External Ref,Received Date,Source,Sub Source,Customer Name,Address1,Address2,Town,PostCode,Country,Number Order Items,Item > SKU,Item > ChannelSKU,Item > Description,Item > Quantity");

                // Create an array to record all order GUIDs
                // Note: actionCancelled does NOT set ExtendedProperty, but we still track OrderIds for consistency
                Guid[] orderIds = orders. Select(order => order.OrderId).ToArray();
                this.Logger.WriteInfo($"        Collected {orderIds.Length} order IDs (Note: No EP update for actionCancelled)");

                int lineCount = 0;
                foreach (var order in orders)
                {
                    foreach (var item in order.Items)
                    {
                        csv.AppendLine($"{order.NumOrderId},{order. GeneralInfo.ReferenceNum},{order.GeneralInfo. SecondaryReference},{order.GeneralInfo.ExternalReferenceNum},{order. GeneralInfo.ReceivedDate:yyyy-MM-dd},{order.GeneralInfo.Source},{order. GeneralInfo.SubSource},{order.CustomerInfo.Address. FullName},{order.CustomerInfo.Address.Address1},{order.CustomerInfo.Address.Address2},{order.CustomerInfo.Address.Town},{order. CustomerInfo.Address. PostCode},{order. CustomerInfo.Address. Country},{order.GeneralInfo.NumItems},{item.SKU},{item.ChannelSKU},{item. Title},{item. Quantity}");
                        lineCount++;
                    }
                }

                this.Logger.WriteInfo($"        CSV contains {lineCount} item lines from {orders.Count} orders");

                // Save the CSV locally
                string fullPath = Path.Combine(localFilePath, fileName);
                SaveCsvLocally(csv, fullPath);

                this.Logger.WriteInfo($"        FormatactionCancelled completed successfully");

                // Return the CSV, filename, and orderIds array
                return (csv, fileName, orderIds);
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"        ERROR in FormatactionCancelled:  {ex.Message}");
                this.Logger.WriteError($"        Stack trace:  {ex.StackTrace}");
                throw;
            }
        }

        private (StringBuilder Csv, string FileName, Guid[] OrderIds) FormatnotifyCancelled(List<OrderDetails> orders, string folder, string localFilePath, string filetype)
        {
            this.Logger.WriteInfo($"        FormatnotifyCancelled started for {orders.Count} orders");

            try
            {
                string fileName = $"Orders_Cancelled_{DateTime.Now:yyyyMMddHHmmss}_{filetype}.csv";
                this.Logger.WriteInfo($"        Generated filename: {fileName}");

                var csv = GenerateCsv(orders);

                string fullPath = Path.Combine(localFilePath, fileName);
                SaveCsvLocally(csv, fullPath);

                Guid[] orderIds = orders.Select(order => order.OrderId).ToArray();
                this.Logger.WriteInfo($"        FormatnotifyCancelled completed. OrderIds: {orderIds.Length}");

                return (csv, fileName, orderIds);
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"        ERROR in FormatnotifyCancelled:  {ex.Message}");
                this.Logger.WriteError($"        Stack trace: {ex. StackTrace}");
                throw;
            }
        }

        private void SendMacroEmail(List<Guid> recipientIds, string subject, string body, string templateType = null)
        {
            this.Logger.WriteInfo($"        SendMacroEmail started");
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

                this.Logger.WriteInfo("        Calling Api. Email.GenerateFreeTextEmail.. .");
                var emailResponse = Api.Email.GenerateFreeTextEmail(emailRequest);

                if (emailResponse.isComplete)
                {
                    this.Logger.WriteInfo("        Email sent successfully!");
                }
                else
                {
                    this.Logger.WriteError($"        Email failed to send.  Failed recipients: {string.Join(",", emailResponse.FailedRecipients ?? new List<string>())}");
                }
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"        ERROR in SendMacroEmail:  {ex.Message}");
                this.Logger.WriteError($"        Stack trace: {ex. StackTrace}");
                throw;
            }
        }
    }
}