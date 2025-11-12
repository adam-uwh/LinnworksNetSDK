using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using LinnworksAPI;
using LinnworksMacroHelpers.Classes;
using System.ComponentModel;
using System.Text;

namespace LinnworksMacro
{
    public class LinnworksMacro : LinnworksMacroHelpers.LinnworksMacroBase
    {
        /* ***************************************
         * ORIGINAL TICKET DETAILS
         * LinnSystems Ticket ~ #1245004
         * Installation date ~ 20.12.2018
         * Description ~ Create a csv file and export it through FTP
         * ***************************************
         * AMENDMENT DETAILS FOR v3
         * LinnSystems Ticket ~ #1500957
         * Installation date ~ 11/07/2022
         * Description ~ Create a csv file and export it through SFTP.
         *              + Add extended property and sources filtering
         * ***************************************
         * AMENDMENT
         * Linnworks Ticket ~ #1544168
         * Installation date ~ 27/03/2023
         * Description ~ Include ex-vat unit cost and add shipping line + extra column to header lines
         * ***************************************
         * LATEST AMENDMENT
         * Added per-order processing based on extended property 'soldto'.
         * Added many new header/detail columns, folder handling, notes and dispatch date modifier logic.
         * Added option to save files locally using 'localFilePath' when provided (avoids SFTP for testing).
         * ****************************************/

        private readonly StringBuilder _csvFile = new();
        private readonly Dictionary<Guid, OrderHead> _dictHead = new();
        private readonly Dictionary<Guid, List<OrderDetail>> _dictDetails = new();

        public void Execute(
            string source,
            string subSource,
            string accountNumber,
            string SFTPServer,
            int SFTPPort,
            string SFTPUsername,
            string SFTPPassword,
            string SFTPFolderPath,
            string localFilePath,             // NEW - if not empty, write files locally instead of SFTP
            string sortField,
            string sortDirection,
            int lastDays,
            bool addShippingCharge,
            string shippingChargeSku,
            string extendedPropertyName,

            // New variables
            string priceFlag,
            string orderType,
            string branchPlan,
            string shipTo,
            string shipCode,
            string holdStatus,
            bool addDispatchDays,
            int dispatchModifier,
            string folderUpdated,
            string folderCompleted,
            bool ignoreUnknownSKUs
            )
        {
            this.Logger.WriteInfo("Starting updated script - per-order export to SFTP or local path");

            SFtpSettings sftpSettings = new SFtpSettings()
            {
                UserName = SFTPUsername,
                Password = SFTPPassword,
                Server = SFTPServer.StartsWith("sftp://") ? SFTPServer : $"sftp://{SFTPServer}",
                Port = SFTPPort,
                FullPath = SFTPFolderPath
            };

            // Get all matching orders for the source/subsource and folderUpdated
            List<OrderDetails> orders = GetOrderDetails(source, subSource, sortField, sortDirection, lastDays, ignoreUnknownSKUs, extendedPropertyName, folderUpdated);

            // Process each order individually:
            foreach (OrderDetails ods in orders)
            {
                try
                {
                    // Get soldto extended property (case-insensitive)
                    string soldTo = GetExtendedPropertyValue(ods, "soldto");

                    if (string.IsNullOrWhiteSpace(soldTo))
                    {
                        // Move to Order Errors and add note
                        string errFolder = "Order Errors";
                        try
                        {
                            SetOrderFolder(ods.OrderId, errFolder);
                            AddOrderNote(ods.OrderId, $"No SoldTo extended property found (searched for 'soldto'/'SoldTo'). Order moved to '{errFolder}'.");
                            this.Logger.WriteInfo($"Order {ods.NumOrderId} moved to '{errFolder}' due to missing soldto.");
                        }
                        catch (Exception ex)
                        {
                            this.Logger.WriteError($"Failed to move order {ods.NumOrderId} to '{errFolder}': {ex.Message}");
                        }

                        // skip to next order
                        continue;
                    }

                    // Ensure items are filtered for unknown SKUs if requested
                    List<OrderItem> items = ods.Items?.ToList() ?? new List<OrderItem>();
                    if (ignoreUnknownSKUs)
                    {
                        items = items.Where(item => item.ItemId != Guid.Empty && !string.IsNullOrEmpty(item.SKU)).ToList();
                        if (items.Count == 0)
                        {
                            // No valid items, move to Order Errors and note
                            string errFolder = "Order Errors";
                            try
                            {
                                SetOrderFolder(ods.OrderId, errFolder);
                                AddOrderNote(ods.OrderId, $"Order contained no valid/linked items after filtering unknown SKUs. Order moved to '{errFolder}'.");
                                this.Logger.WriteInfo($"Order {ods.NumOrderId} moved to '{errFolder}' due to no valid items after filtering.");
                            }
                            catch (Exception ex)
                            {
                                this.Logger.WriteError($"Failed to move order {ods.NumOrderId} to '{errFolder}': {ex.Message}");
                            }
                            continue;
                        }
                    }

                    // Build CSV for this single order
                    _csvFile.Clear();
                    _dictHead.Clear();
                    _dictDetails.Clear();

                    BuildOrderMapping(ods, items, addShippingCharge, shippingChargeSku, addDispatchDays, dispatchModifier, priceFlag, orderType, branchPlan, shipCode, holdStatus, soldTo);

                    CompileCsv(); // fills _csvFile

                    // Send file using soldTo as accountNumber for filename prefix. If localFilePath provided, it will save locally.
                    bool sent = SendReport(_csvFile, soldTo, sftpSettings, localFilePath);

                    if (sent)
                    {
                        try
                        {
                            SetOrderFolder(ods.OrderId, folderCompleted);
                        }
                        catch (Exception ex)
                        {
                            this.Logger.WriteError($"Failed to set order {ods.NumOrderId} folder to '{folderCompleted}': {ex.Message}");
                        }

                        try
                        {
                            AddOrderNote(ods.OrderId, $"Order successfully sent to JDE on {DateTime.Now:yyyy-MM-dd HH:mm:ss} using soldto '{soldTo}'.");
                        }
                        catch (Exception ex)
                        {
                            this.Logger.WriteError($"Failed to add success note to order {ods.NumOrderId}: {ex.Message}");
                        }

                        this.Logger.WriteInfo($"Order {ods.NumOrderId} exported and sent for soldto '{soldTo}'.");
                    }
                    else
                    {
                        // Sending failed - move to Order Errors and add note
                        string errFolder = "Order Errors";
                        try
                        {
                            SetOrderFolder(ods.OrderId, errFolder);
                            AddOrderNote(ods.OrderId, $"Failed to send order to JDE for soldto '{soldTo}'. Order moved to '{errFolder}'.");
                        }
                        catch (Exception ex)
                        {
                            this.Logger.WriteError($"Failed to move order {ods.NumOrderId} to '{errFolder}' after send failure: {ex.Message}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    this.Logger.WriteError($"Unexpected error processing order {ods.NumOrderId}: {ex.Message}");
                    try
                    {
                        SetOrderFolder(ods.OrderId, "Order Errors");
                        AddOrderNote(ods.OrderId, $"Unexpected processing error: {ex.Message}");
                    }
                    catch
                    {
                        // swallow further exceptions
                    }
                }
            }

            this.Logger.WriteInfo("Script finished.");
        }

        private void BuildOrderMapping(OrderDetails ods, List<OrderItem> items, bool addShippingSku, string shippingSku, bool addDispatchDays, int dispatchModifier, string priceFlag, string orderType, string branchPlan, string shipCode, string holdStatus, string soldTo)
        {
            // Header mapping
            string customerOrderReference = GetExtendedPropertyValue(ods, "custpo");
            if (string.IsNullOrWhiteSpace(customerOrderReference))
                customerOrderReference = ods.GeneralInfo.ReferenceNum;

            DateTime despatchBy = ods.GeneralInfo.DespatchByDate;
            DateTime requiredDate = ComputeRequiredDate(despatchBy, addDispatchDays, dispatchModifier);

            string postalService = (ods.ShippingInfo.PostalServiceName == "Standard") ? "" : ods.ShippingInfo.PostalServiceName;

            string channelTrackingRef = GetExtendedPropertyValue(ods, "trackno");

            // Delivery telephone/email/county/country
            string deliveryTelephone = GetTelephoneFromOrder(ods);
            string deliveryEmail = GetEmailFromOrder(ods);
            if (string.IsNullOrWhiteSpace(deliveryEmail))
                deliveryEmail = "noreply@uwhome.com";

            string deliveryTown = ods.CustomerInfo?.Address?.Town ?? string.Empty;
            // 'CustomerAddress' in this SDK provides Region instead of County
            string deliveryCounty = ods.CustomerInfo?.Address?.Region ?? string.Empty;
            // No explicit CountryCode on CustomerAddress; fall back to Country string
            string deliveryCountryCode = ods.CustomerInfo?.Address?.Country ?? string.Empty;
            string deliveryCountry = ods.CustomerInfo?.Address?.Country ?? string.Empty;

            var oh = new OrderHead()
            {
                Record_Type = "OH",
                Customer_Order_Reference = customerOrderReference,
                Linnworks_Order_Number = ods.NumOrderId.ToString(),
                Order_Date = ods.GeneralInfo.ReceivedDate.ToString("yyyyMMdd"),
                Required_Date = requiredDate.ToString("yyyyMMdd"),
                Postal_Service = postalService,
                Blank1 = string.Empty,
                Blank2 = string.Empty,
                Delivery_Customer_Name = ods.CustomerInfo?.Address?.FullName ?? string.Empty,
                Delivery_Address1 = ods.CustomerInfo?.Address?.Address1 ?? string.Empty,
                Delivery_Address2 = ods.CustomerInfo?.Address?.Address2 ?? string.Empty,
                Delivery_Address3 = ods.CustomerInfo?.Address?.Address3 ?? string.Empty,
                Delivery_Address4 = ods.CustomerInfo?.Address?.Town ?? string.Empty,
                Delivery_Postcode = ods.CustomerInfo?.Address?.PostCode ?? string.Empty,
                Blank3 = string.Empty,
                Blank4 = string.Empty,
                Channel_Tracking_Ref = channelTrackingRef,
                Price_Flag = priceFlag,
                Shipping_Code = shipCode,
                Delivery_Telephone = deliveryTelephone,
                Delivery_Email = deliveryEmail,
                Order_Type = orderType,
                Branch_Plan = branchPlan,
                Delivery_Town = deliveryTown,
                Delivery_County = deliveryCounty,
                Delivery_Country_Code = deliveryCountryCode,
                Delivery_Country = deliveryCountry,
                Ship_To_Account_Number = soldTo,
                Hold_Status = holdStatus
            };

            _dictHead[ods.OrderId] = oh;

            // Details
            int lineNumber = 1;
            foreach (OrderItem oi in items)
            {
                var detail = new OrderDetail()
                {
                    Record_Type = "OD",
                    Line_Number = lineNumber++,
                    JRSL_Product_Code = string.IsNullOrWhiteSpace(oi.SKU) ? oi.ChannelSKU : oi.SKU,
                    Blank1 = string.Empty,
                    Quantity_Required = oi.Quantity,
                    Item_Cost = Math.Round(oi.Cost / (oi.Quantity == 0 ? 1 : oi.Quantity), 2),
                    Blank2 = string.Empty,
                    Required_Date = requiredDate.ToString("yyyyMMdd")
                };

                if (!_dictDetails.ContainsKey(ods.OrderId))
                    _dictDetails.Add(ods.OrderId, new List<OrderDetail>() { detail });
                else
                    _dictDetails[ods.OrderId].Add(detail);
            }

            // Shipping charge as OD line if required
            if (addShippingSku && ods.ShippingInfo?.PostageCostExTax > 0)
            {
                var shipDetail = new OrderDetail()
                {
                    Record_Type = "OD",
                    Line_Number = _dictDetails.ContainsKey(ods.OrderId) ? _dictDetails[ods.OrderId].Count + 1 : lineNumber,
                    JRSL_Product_Code = shippingSku,
                    Blank1 = string.Empty,
                    Quantity_Required = 1,
                    Item_Cost = ods.ShippingInfo.PostageCostExTax,
                    Blank2 = string.Empty,
                    Required_Date = requiredDate.ToString("yyyyMMdd")
                };

                if (!_dictDetails.ContainsKey(ods.OrderId))
                    _dictDetails.Add(ods.OrderId, new List<OrderDetail>() { shipDetail });
                else
                    _dictDetails[ods.OrderId].Add(shipDetail);
            }
        }

        private string GetExtendedPropertyValue(OrderDetails ods, string name)
        {
            if (ods?.ExtendedProperties == null)
                return string.Empty;

            var ep = ods.ExtendedProperties.FirstOrDefault(x => string.Equals(x?.Name, name, StringComparison.OrdinalIgnoreCase));
            return ep?.Value ?? string.Empty;
        }

        private DateTime ComputeRequiredDate(DateTime despatchBy, bool addDispatchDays, int dispatchModifier)
        {
            if (!addDispatchDays)
                return despatchBy;

            DateTime result = despatchBy.AddDays(dispatchModifier);

            // If Saturday or Sunday push to Monday as per logic
            if (result.DayOfWeek == DayOfWeek.Sunday)
                result = result.AddDays(1);
            else if (result.DayOfWeek == DayOfWeek.Saturday)
                result = result.AddDays(2);

            return result;
        }

        private string GetTelephoneFromOrder(OrderDetails ods)
        {
            // Try several common locations for telephone values - best-effort since SDK models can vary
            try
            {
                var addr = ods?.CustomerInfo?.Address;
                if (addr == null) return string.Empty;

                // Common property names to try
                var possible = new[] { "TelephoneNumber", "Telephone", "Phone", "PhoneNumber", "ContactNumber" };
                foreach (var prop in possible)
                {
                    var p = addr.GetType().GetProperty(prop);
                    if (p != null)
                    {
                        var val = p.GetValue(addr) as string;
                        if (!string.IsNullOrWhiteSpace(val))
                            return val;
                    }
                }
            }
            catch { }

            // As fallback check CustomerInfo fields
            try
            {
                var ci = ods?.CustomerInfo;
                if (ci != null)
                {
                    var possible = new[] { "TelephoneNumber", "Telephone", "Phone", "PhoneNumber" };
                    foreach (var prop in possible)
                    {
                        var p = ci.GetType().GetProperty(prop);
                        if (p != null)
                        {
                            var val = p.GetValue(ci) as string;
                            if (!string.IsNullOrWhiteSpace(val))
                                return val;
                        }
                    }
                }
            }
            catch { }

            return string.Empty;
        }

        private string GetEmailFromOrder(OrderDetails ods)
        {
            try
            {
                // Try CustomerInfo.Email
                var ci = ods?.CustomerInfo;
                if (ci != null)
                {
                    var p = ci.GetType().GetProperty("Email");
                    if (p != null)
                    {
                        var val = p.GetValue(ci) as string;
                        if (!string.IsNullOrWhiteSpace(val))
                            return val;
                    }
                }

                // Try Address.Email
                var addr = ods?.CustomerInfo?.Address;
                if (addr != null)
                {
                    var p2 = addr.GetType().GetProperty("Email");
                    if (p2 != null)
                    {
                        var val2 = p2.GetValue(addr) as string;
                        if (!string.IsNullOrWhiteSpace(val2))
                            return val2;
                    }
                }
            }
            catch { }

            return string.Empty;
        }

        private void CompileCsv()
        {
            // Header columns order as per new specification
            // We'll write header row per order head, then OD rows per order
            foreach (KeyValuePair<Guid, OrderHead> kv in _dictHead)
            {
                var oh = kv.Value;
                _csvFile.AppendLine(string.Join("|"
                    , oh.Record_Type
                    , oh.Customer_Order_Reference
                    , oh.Linnworks_Order_Number
                    , oh.Order_Date
                    , oh.Required_Date
                    , oh.Postal_Service
                    , oh.Blank1
                    , oh.Blank2
                    , oh.Delivery_Customer_Name
                    , oh.Delivery_Address1
                    , oh.Delivery_Address2
                    , oh.Delivery_Address3
                    , oh.Delivery_Address4
                    , oh.Delivery_Postcode
                    , oh.Blank3
                    , oh.Blank4
                    , oh.Channel_Tracking_Ref
                    , oh.Price_Flag
                    , oh.Shipping_Code
                    , oh.Delivery_Telephone
                    , oh.Delivery_Email
                    , oh.Order_Type
                    , oh.Branch_Plan
                    , oh.Delivery_Town
                    , oh.Delivery_County
                    , oh.Delivery_Country_Code
                    , oh.Delivery_Country
                    , oh.Ship_To_Account_Number
                    , oh.Hold_Status
                    ));

                if (_dictDetails.ContainsKey(kv.Key))
                {
                    foreach (var od in _dictDetails[kv.Key])
                    {
                        AddOrderDetails(od);
                    }
                }
            }
        }

        private void AddOrderDetails(OrderDetail od)
        {
            _csvFile.AppendLine(string.Join("|"
                , od.Record_Type
                , od.Line_Number
                , od.JRSL_Product_Code
                , od.Blank1
                , od.Quantity_Required
                , od.Item_Cost
                , od.Blank2
                , od.Required_Date
                ));
        }

        private sealed class OrderHead
        {
            public string Record_Type { get; set; } = string.Empty;
            public string Customer_Order_Reference { get; set; } = string.Empty; // custpo or reference
            public string Linnworks_Order_Number { get; set; } = string.Empty; // NumOrderId
            public string Order_Date { get; set; } = string.Empty; // yyyyMMdd
            public string Required_Date { get; set; } = string.Empty; // yyyyMMdd (with dispatch days)
            public string Postal_Service { get; set; } = string.Empty; // Delivery method
            public string Blank1 { get; set; } = string.Empty;
            public string Blank2 { get; set; } = string.Empty;
            public string Delivery_Customer_Name { get; set; } = string.Empty;
            public string Delivery_Address1 { get; set; } = string.Empty;
            public string Delivery_Address2 { get; set; } = string.Empty;
            public string Delivery_Address3 { get; set; } = string.Empty;
            public string Delivery_Address4 { get; set; } = string.Empty;
            public string Delivery_Postcode { get; set; } = string.Empty;
            public string Blank3 { get; set; } = string.Empty;
            public string Blank4 { get; set; } = string.Empty;
            public string Channel_Tracking_Ref { get; set; } = string.Empty; // trackno
            public string Price_Flag { get; set; } = string.Empty; // from variable
            public string Shipping_Code { get; set; } = string.Empty; // shipCode variable
            public string Delivery_Telephone { get; set; } = string.Empty;
            public string Delivery_Email { get; set; } = string.Empty;
            public string Order_Type { get; set; } = string.Empty;
            public string Branch_Plan { get; set; } = string.Empty;
            public string Delivery_Town { get; set; } = string.Empty;
            public string Delivery_County { get; set; } = string.Empty;
            public string Delivery_Country_Code { get; set; } = string.Empty;
            public string Delivery_Country { get; set; } = string.Empty;
            public string Ship_To_Account_Number { get; set; } = string.Empty; // soldto
            public string Hold_Status { get; set; } = string.Empty;
        }

        private sealed class OrderDetail
        {
            public string Record_Type { get; set; } = string.Empty;
            public int Line_Number { get; set; }
            public string JRSL_Product_Code { get; set; } = string.Empty;
            public string Blank1 { get; set; } = string.Empty;
            public int Quantity_Required { get; set; }
            public double Item_Cost { get; set; }
            public string Blank2 { get; set; } = string.Empty;
            public string Required_Date { get; set; } = string.Empty;
        }

        /** Transport: either SFTP or save local file **/
        private bool SendReport(StringBuilder report, string accountNumber, SFtpSettings sftpSettings, string localFilePath)
        {
            // Create filename
            string fileName = $"{accountNumber}_{DateTime.Now.ToString("ddMMyyyyHHmmss")}.csv";

            // If localFilePath provided (not null/empty) write locally instead of SFTP
            if (!string.IsNullOrWhiteSpace(localFilePath))
            {
                try
                {
                    SaveLocalFile(report, fileName, localFilePath);
                    this.Logger.WriteInfo($"File saved locally to '{localFilePath}' as '{fileName}'.");
                    return true;
                }
                catch (Exception ex)
                {
                    this.Logger.WriteError($"Failed to save file locally: {ex.Message}");
                    return false;
                }
            }

            // Otherwise use SFTP. Do not mutate original sftpSettings.FullPath permanently.
            try
            {
                string baseRemote = sftpSettings.FullPath ?? string.Empty;
                // Ensure trailing slash handling
                string remotePath;
                if (baseRemote.EndsWith("/"))
                    remotePath = baseRemote + fileName;
                else if (baseRemote == string.Empty)
                    remotePath = "/" + fileName;
                else
                    remotePath = baseRemote + (baseRemote.EndsWith("/") ? "" : "/") + fileName;

                // create a copy of settings with the remote full path set to the file path
                SFtpSettings toUse = new SFtpSettings()
                {
                    UserName = sftpSettings.UserName,
                    Password = sftpSettings.Password,
                    Server = sftpSettings.Server,
                    Port = sftpSettings.Port,
                    FullPath = remotePath
                };

                return SendByFTP(report, toUse);
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"Error while preparing SFTP upload: {ex.Message}");
                return false;
            }
        }

        private void SaveLocalFile(StringBuilder report, string fileName, string localDirectory)
        {
            // Ensure the localDirectory is a directory path (not a file). Use Path.Combine.
            // If localDirectory points to a file name, the caller should pass folder only.
            string dir = localDirectory;
            // Trim surrounding whitespace
            dir = dir.Trim();

            // If user provided a path including a filename (ends with .csv) - treat it as directory if it exists; otherwise use directory portion.
            if (Path.HasExtension(dir))
            {
                // If file specified, use its directory
                dir = Path.GetDirectoryName(dir) ?? dir;
            }

            if (string.IsNullOrWhiteSpace(dir))
                throw new ArgumentException("Local directory path is empty.");

            if (!Directory.Exists(dir))
                Directory.CreateDirectory(dir);

            string fullPath = Path.Combine(dir, fileName);

            File.WriteAllText(fullPath, report.ToString(), Encoding.UTF8);
        }

        private bool SendByFTP(StringBuilder report, SFtpSettings sftpSettings)
        {
            try
            {
                using var upload = this.ProxyFactory.GetSFtpUploadProxy(sftpSettings);
                upload.Write(report.ToString());

                SftpUploadResult uploadResult = upload.CompleteUpload();
                this.Logger.WriteDebug("upload: " + uploadResult.IsSuccess + " (-> " + uploadResult.ErrorMessage + ")");
                if (!uploadResult.IsSuccess)
                    throw new ArgumentException(uploadResult.ErrorMessage);
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"Error while sending the file to the SFTP: {ex.Message}");
                return false;
            }
            return true;
        }

        #region Order Folder / Notes helpers (best-effort wrappers)
        // These helper methods call the Orders API to set folder and add notes.
        // Depending on your Linnworks SDK version the exact method names/signatures may differ.
        // Adjust to match your SDK if compilation errors occur.

        private void SetOrderFolder(Guid orderId, string folderName)
        {
            try
            {
                // Assign the order to the specified folder using the SDK method available in this repo
                Api.Orders.AssignToFolder(new System.Collections.Generic.List<Guid> { orderId }, folderName);
            }
            catch (Exception ex)
            {
                // Re-throw to allow caller to handle logging and fallback behaviour.
                throw new InvalidOperationException($"SetOrderFolder (AssignToFolder) failed: {ex.Message}", ex);
            }
        }

        private void AddOrderNote(Guid orderId, string note)
        {
            try
            {
                // SDK method names vary. Common ones include AddNote or AddNoteToOrder.
                // Try AddNote first, otherwise try AddNoteToOrder or SaveOrderNote (adjust to your SDK).
                try
                {
                    // Get existing notes, append and set back since this SDK exposes GetOrderNotes/SetOrderNotes
                    var existing = Api.Orders.GetOrderNotes(orderId) ?? new System.Collections.Generic.List<OrderNote>();
                    existing.Add(new OrderNote
                    {
                        OrderNoteId = Guid.NewGuid(),
                        OrderId = orderId,
                        NoteDate = DateTime.Now,
                        Internal = false,
                        Note = note,
                        CreatedBy = "Macro"
                    });

                    Api.Orders.SetOrderNotes(orderId, existing);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"AddOrderNote failed: {ex.Message}", ex);
                }
            }
            catch (Exception ex)
            {
                // Re-throw to let the caller log and decide what to do
                throw new InvalidOperationException($"AddOrderNote failed: {ex.Message}", ex);
            }
        }
        #endregion

        private List<OrderDetails> GetOrderDetails(string sources, string subSources, string sortCode, string sortDirection, int lastDays, bool ignoreUnknownSKUs, string extendedPropertyName, string folderUpdated)
        {
            List<OrderDetails> orders = new List<OrderDetails>();

            //filter for paid, unparked and unlocked orders 
            FieldsFilter filter = new FieldsFilter()
            {
                NumericFields = new List<NumericFieldFilter>()
                {
                    new NumericFieldFilter() { FieldCode = FieldCode.GENERAL_INFO_STATUS, Type = NumericFieldFilterType.Equal, Value = 1},
                    new NumericFieldFilter() { FieldCode = FieldCode.GENERAL_INFO_PARKED, Type = NumericFieldFilterType.Equal, Value = 0},
                    new NumericFieldFilter() { FieldCode = FieldCode.GENERAL_INFO_LOCKED, Type = NumericFieldFilterType.Equal, Value = 0}
                },
                TextFields = new List<TextFieldFilter>(),
                DateFields = new List<DateFieldFilter>()
                {
                    new DateFieldFilter()
                    {
                        FieldCode = FieldCode.GENERAL_INFO_DATE,
                        Type = DateTimeFieldFilterType.LastDays,
                        Value = lastDays
                    }
                }
            };

            foreach (string src in sources.Split(',').Select(s => s.Trim()))
            {
                if (!string.IsNullOrWhiteSpace(src))
                {
                    filter.TextFields.Add(new TextFieldFilter()
                    {
                        FieldCode = FieldCode.GENERAL_INFO_SOURCE,
                        Text = src,
                        Type = TextFieldFilterType.Equal
                    });
                }
            }

            foreach (string sub in subSources.Split(',').Select(s => s.Trim()))
            {
                if (!string.IsNullOrWhiteSpace(sub))
                {
                    filter.TextFields.Add(new TextFieldFilter()
                    {
                        FieldCode = FieldCode.GENERAL_INFO_SUBSOURCE,
                        Text = sub,
                        Type = TextFieldFilterType.Equal
                    });
                }
            }

            // Filter by folder updated (new requirement)
            if (!string.IsNullOrWhiteSpace(folderUpdated))
            {
                filter.TextFields.Add(new TextFieldFilter()
                {
                    FieldCode = FieldCode.FOLDER,
                    Text = folderUpdated,
                    Type = TextFieldFilterType.Equal
                });
            }

            FieldCode sortingCode = FieldCode.GENERAL_INFO_ORDER_ID;
            ListSortDirection sortingDirection = ListSortDirection.Descending;

            if (sortCode.ToUpper() == "REFERENCE")
                sortingCode = FieldCode.GENERAL_INFO_REFERENCE_NUMBER;
            if (sortDirection.ToUpper() == "ASCENDING")
                sortingDirection = ListSortDirection.Ascending;

            List<Guid> guids = Api.Orders.GetAllOpenOrders(filter, new List<FieldSorting>()
            {
                new FieldSorting()
                {
                    FieldCode = sortingCode,
                    Direction = sortingDirection,
                    Order = 0
                }
            }, Guid.Empty, "");

            if (guids.Count > 200)
            {
                for (int i = 0; i < guids.Count; i += 200)
                {
                    orders.AddRange(Api.Orders.GetOrdersById(guids.Skip(i).Take(200).ToList()));
                }
            }
            else if (guids.Count <= 200 && guids.Count > 0)
            {
                orders = Api.Orders.GetOrdersById(guids);
            }

            // If enabled, filter the order items which are not present in Linnworks
            if (ignoreUnknownSKUs)
            {
                this.Logger.WriteInfo("Unlinked items will be ignored");
                foreach (OrderDetails order in orders)
                {
                    order.Items = order.Items.Where(item => item.ItemId != Guid.Empty && !string.IsNullOrEmpty(item.SKU)).ToList();
                }

                // Skip orders which do not have any valid item
                orders = orders.Where(order => order.Items.Count > 0).ToList();
            }

            //If extended property name provided. Filter by orders with the extended property
            if (!string.IsNullOrWhiteSpace(extendedPropertyName))
            {
                for (int i = 0; i < orders.Count; i++)
                {
                    if (orders[i].ExtendedProperties.Any(ep => ep.Name == extendedPropertyName))
                        continue;

                    orders.Remove(orders[i]);
                    i--;
                }
            }

            List<OrderDetails> sortedOrders;

            if (sortingCode == FieldCode.GENERAL_INFO_ORDER_ID)
            {
                if (sortingDirection == ListSortDirection.Ascending)
                    sortedOrders = orders.OrderBy(order => order.NumOrderId).ToList();
                else
                    sortedOrders = orders.OrderByDescending(order => order.NumOrderId).ToList();
            }
            else
            {
                if (sortingDirection == ListSortDirection.Ascending)
                    sortedOrders = orders.OrderBy(order => order.GeneralInfo.ReferenceNum).ToList();
                else
                    sortedOrders = orders.OrderByDescending(order => order.GeneralInfo.ReferenceNum).ToList();
            }

            return sortedOrders;
        }
    }
}