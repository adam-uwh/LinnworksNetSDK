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
         * Linnworks macro - per-order CSV export
         * - Uses Api.Orders.GetCountries() once to map CountryId -> CountryCode
         * - No use of SR APIs
         * - Dynamic-safe property access with try/catch for known property names
         * - Writes local files as UTF-8 without BOM when localFilePath provided
         * - OD lines include Customer_Order_Reference as second column
         * - Ship_To_Account_Number taken from macro parameter shipTo
         * *************************************** */

        private readonly StringBuilder _csvFile = new();
        private readonly Dictionary<Guid, OrderHead> _dictHead = new();
        private readonly Dictionary<Guid, List<OrderDetail>> _dictDetails = new();

        // Cached countries map: key = CountryId.ToString() -> value = CountryCode (2-letter)
        private Dictionary<string, string> _countriesMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public void Execute(
            string source,
            string subSource,
            string accountNumber,
            string SFTPServer,
            int SFTPPort,
            string SFTPUsername,
            string SFTPPassword,
            string SFTPFolderPath,
            string localFilePath,
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
            this.Logger.WriteInfo("Starting script - per-order export");

            SFtpSettings sftpSettings = new SFtpSettings()
            {
                UserName = SFTPUsername,
                Password = SFTPPassword,
                Server = SFTPServer.StartsWith("sftp://") ? SFTPServer : $"sftp://{SFTPServer}",
                Port = SFTPPort,
                FullPath = SFTPFolderPath
            };

            // Load country map once
            try
            {
                _countriesMap = LoadCountriesMap();
                this.Logger.WriteInfo($"Loaded {_countriesMap.Count} countries for code mapping.");
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"Failed to load countries map: {ex.Message}. Country codes may be empty.");
                _countriesMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            }

            // Get orders matching filters
            List<OrderDetails> orders = GetOrderDetails(source, subSource, sortField, sortDirection, lastDays, ignoreUnknownSKUs, extendedPropertyName, folderUpdated);

            foreach (OrderDetails ods in orders)
            {
                try
                {
                    // Per-order soldto extended property
                    string soldTo = GetExtendedPropertyValue(ods, "soldto");

                    if (string.IsNullOrWhiteSpace(soldTo))
                    {
                        // Move to Order Errors and add note
                        string errFolder = "Order Errors";
                        try
                        {
                            SetOrderFolder(ods.OrderId, errFolder);
                            AddOrderNote(ods.OrderId, $"No SoldTo extended property found. Order moved to '{errFolder}'.");
                            this.Logger.WriteInfo($"Order {ods.NumOrderId} moved to '{errFolder}' due to missing soldto.");
                        }
                        catch (Exception ex)
                        {
                            this.Logger.WriteError($"Failed to move order {ods.NumOrderId} to '{errFolder}': {ex.Message}");
                        }
                        continue;
                    }

                    // Optionally filter unknown SKUs
                    List<OrderItem> items = ods.Items?.ToList() ?? new List<OrderItem>();
                    if (ignoreUnknownSKUs)
                    {
                        items = items.Where(item => item.ItemId != Guid.Empty && !string.IsNullOrEmpty(item.SKU)).ToList();
                        if (items.Count == 0)
                        {
                            string errFolder = "Order Errors";
                            try
                            {
                                SetOrderFolder(ods.OrderId, errFolder);
                                AddOrderNote(ods.OrderId, $"Order contained no valid items after filtering unknown SKUs. Order moved to '{errFolder}'.");
                                this.Logger.WriteInfo($"Order {ods.NumOrderId} moved to '{errFolder}' due to no valid items after filtering.");
                            }
                            catch (Exception ex)
                            {
                                this.Logger.WriteError($"Failed to move order {ods.NumOrderId} to '{errFolder}': {ex.Message}");
                            }
                            continue;
                        }
                    }

                    // Build per-order CSV
                    _csvFile.Clear();
                    _dictHead.Clear();
                    _dictDetails.Clear();

                    BuildOrderMapping(ods, items, addShippingCharge, shippingChargeSku, addDispatchDays, dispatchModifier, priceFlag, orderType, branchPlan, shipTo, shipCode, holdStatus, soldTo);

                    CompileCsv();

                    // Send file: if localFilePath provided, save locally; otherwise SFTP
                    bool sent = SendReport(_csvFile, soldTo, sftpSettings, localFilePath);

                    if (sent)
                    {
                        try { SetOrderFolder(ods.OrderId, folderCompleted); } catch (Exception ex) { this.Logger.WriteError($"SetOrderFolder failed: {ex.Message}"); }

                        try { AddOrderNote(ods.OrderId, $"Order successfully sent to JDE on {DateTime.Now:yyyy-MM-dd HH:mm:ss} using soldto '{soldTo}'."); } catch { }

                        this.Logger.WriteInfo($"Order {ods.NumOrderId} exported and sent for soldto '{soldTo}'.");
                    }
                    else
                    {
                        try
                        {
                            SetOrderFolder(ods.OrderId, "Order Errors");
                            AddOrderNote(ods.OrderId, $"Failed to send order to JDE for soldto '{soldTo}'. Order moved to 'Order Errors'.");
                        }
                        catch { }
                    }
                }
                catch (Exception ex)
                {
                    this.Logger.WriteError($"Unexpected error processing order {ods.NumOrderId}: {ex.Message}");
                    try { SetOrderFolder(ods.OrderId, "Order Errors"); AddOrderNote(ods.OrderId, $"Unexpected processing error: {ex.Message}"); } catch { }
                }
            }

            this.Logger.WriteInfo("Script finished.");
        }

        /// <summary>
        /// Load countries once using Api.Orders.GetCountries(); map CountryId -> CountryCode
        /// Uses dynamic access only (no SR)
        /// </summary>
        private Dictionary<string, string> LoadCountriesMap()
        {
            var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var countries = Api.Orders.GetCountries();
            if (countries == null) return map;

            foreach (var c in countries)
            {
                try
                {
                    dynamic dc = c;
                    object idVal = null;
                    string codeVal = null;

                    // Try common id properties
                    try { idVal = dc.CountryId; } catch { }
                    if (idVal == null) try { idVal = dc.Id; } catch { }
                    if (idVal == null) try { idVal = dc.countryid; } catch { }

                    // Try common code properties
                    try { codeVal = dc.CountryCode as string; } catch { }
                    if (string.IsNullOrWhiteSpace(codeVal)) try { codeVal = dc.Code as string; } catch { }

                    if (idVal != null && !string.IsNullOrWhiteSpace(codeVal))
                    {
                        var key = idVal.ToString();
                        var code = codeVal.Trim().ToUpperInvariant();
                        if (!map.ContainsKey(key)) map[key] = code;
                    }
                }
                catch
                {
                    // skip problematic country entries
                }
            }

            return map;
        }

        private void BuildOrderMapping(OrderDetails ods, List<OrderItem> items, bool addShippingSku, string shippingSku, bool addDispatchDays, int dispatchModifier, string priceFlag, string orderType, string branchPlan, string shipTo, string shipCode, string holdStatus, string soldTo)
        {
            string customerOrderReference = GetExtendedPropertyValue(ods, "custpo");
            if (string.IsNullOrWhiteSpace(customerOrderReference))
                customerOrderReference = ods.GeneralInfo.ReferenceNum;

            DateTime despatchBy = ods.GeneralInfo.DespatchByDate;
            DateTime requiredDate = ComputeRequiredDate(despatchBy, addDispatchDays, dispatchModifier);

            string postalService = (ods.ShippingInfo?.PostalServiceName == "Standard") ? "" : ods.ShippingInfo?.PostalServiceName;
            string channelTrackingRef = GetExtendedPropertyValue(ods, "trackno");

            string deliveryTelephone = GetTelephoneFromOrder(ods);
            string deliveryEmail = GetEmailFromOrder(ods);
            if (string.IsNullOrWhiteSpace(deliveryEmail)) deliveryEmail = "noreply@uwhome.com";

            string deliveryTown = SafeGetAddressString(ods, "Town");
            string deliveryCounty = SafeGetAddressString(ods, "Region");
            if (string.IsNullOrWhiteSpace(deliveryCounty)) deliveryCounty = SafeGetAddressString(ods, "County");

            string deliveryCountryCode = ResolveCountryCodeFromOrder(ods);
            string deliveryCountry = SafeGetAddressString(ods, "Country");

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
                Delivery_Customer_Name = SafeGetAddressString(ods, "FullName"),
                Delivery_Address1 = SafeGetAddressString(ods, "Address1"),
                Delivery_Address2 = SafeGetAddressString(ods, "Address2"),
                Delivery_Address3 = SafeGetAddressString(ods, "Address3"),
                Delivery_Address4 = SafeGetAddressString(ods, "Town"),
                Delivery_Postcode = SafeGetAddressString(ods, "PostCode"),
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
                Ship_To_Account_Number = shipTo, // use macro parameter
                Hold_Status = holdStatus
            };

            _dictHead[ods.OrderId] = oh;

            int lineNumber = 1;
            foreach (OrderItem oi in items)
            {
                var detail = new OrderDetail()
                {
                    Record_Type = "OD",
                    Customer_Order_Reference = customerOrderReference,
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

            // Optional shipping charge line (as OD)
            if (addShippingSku && ods.ShippingInfo?.PostageCostExTax > 0)
            {
                var shipDetail = new OrderDetail()
                {
                    Record_Type = "OD",
                    Customer_Order_Reference = customerOrderReference,
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

        // Safe dynamic helper to get address string properties (no reflection)
        private string SafeGetAddressString(OrderDetails ods, string propName)
        {
            try
            {
                dynamic addr = ods?.CustomerInfo?.Address;
                if (addr == null) return string.Empty;

                switch (propName)
                {
                    case "FullName":
                        try { string v = addr.FullName; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { } break;
                    case "Address1":
                        try { string v = addr.Address1; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { } break;
                    case "Address2":
                        try { string v = addr.Address2; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { } break;
                    case "Address3":
                        try { string v = addr.Address3; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { } break;
                    case "Town":
                        try { string v = addr.Town; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { } break;
                    case "PostCode":
                        try { string v = addr.PostCode; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { } break;
                    case "Country":
                        try { string v = addr.Country; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { } break;
                    case "Region":
                        try { string v = addr.Region; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { } break;
                    case "County":
                        try { string v = addr.County; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { } break;
                }
            }
            catch { }

            return string.Empty;
        }

        /// <summary>
        /// Resolve the country code for the order using CountryId from the order address and the cached countries map.
        /// If not found, returns empty string.
        /// </summary>
        private string ResolveCountryCodeFromOrder(OrderDetails ods)
        {
            try
            {
                if (_countriesMap != null && _countriesMap.Count > 0)
                {
                    dynamic addr = ods?.CustomerInfo?.Address;
                    if (addr != null)
                    {
                        object countryIdVal = null;
                        try { countryIdVal = addr.CountryId; } catch { }
                        if (countryIdVal == null) try { countryIdVal = addr.CountryID; } catch { }
                        if (countryIdVal == null) try { countryIdVal = addr.Country; } catch { }

                        if (countryIdVal != null)
                        {
                            var key = countryIdVal.ToString();
                            if (!string.IsNullOrWhiteSpace(key) && _countriesMap.TryGetValue(key, out var code))
                                return code;
                        }
                    }
                }
            }
            catch { }

            return string.Empty;
        }

        private string GetExtendedPropertyValue(OrderDetails ods, string name)
        {
            if (ods?.ExtendedProperties == null) return string.Empty;
            var ep = ods.ExtendedProperties.FirstOrDefault(x => string.Equals(x?.Name, name, StringComparison.OrdinalIgnoreCase));
            return ep?.Value ?? string.Empty;
        }

        private DateTime ComputeRequiredDate(DateTime despatchBy, bool addDispatchDays, int dispatchModifier)
        {
            if (!addDispatchDays) return despatchBy;
            DateTime result = despatchBy.AddDays(dispatchModifier);
            if (result.DayOfWeek == DayOfWeek.Sunday) result = result.AddDays(1);
            else if (result.DayOfWeek == DayOfWeek.Saturday) result = result.AddDays(2);
            return result;
        }

        // Phone lookup using dynamic access only
        private string GetTelephoneFromOrder(OrderDetails ods)
        {
            try
            {
                dynamic addr = ods?.CustomerInfo?.Address;
                if (addr != null)
                {
                    try { string v = addr.TelephoneNumber; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    try { string v = addr.Telephone; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    try { string v = addr.Phone; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    try { string v = addr.PhoneNumber; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    try { string v = addr.ContactNumber; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    try { string v = addr.ContactNo; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    try { string v = addr.Contact; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                }
            }
            catch { }

            try
            {
                dynamic ci = ods?.CustomerInfo;
                if (ci != null)
                {
                    try { string v = ci.TelephoneNumber; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    try { string v = ci.Telephone; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    try { string v = ci.Phone; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    try { string v = ci.PhoneNumber; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                }
            }
            catch { }

            return string.Empty;
        }

        // Email lookup using dynamic access only
        private string GetEmailFromOrder(OrderDetails ods)
        {
            try
            {
                dynamic ci = ods?.CustomerInfo;
                if (ci != null)
                {
                    try { string v = ci.Email; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    try { string v = ci.cEmailAddress; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                }
            }
            catch { }

            try
            {
                dynamic addr = ods?.CustomerInfo?.Address;
                if (addr != null)
                {
                    try { string v = addr.Email; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                }
            }
            catch { }

            return string.Empty;
        }

        private void CompileCsv()
        {
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
                , od.Customer_Order_Reference
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
            public string Customer_Order_Reference { get; set; } = string.Empty;
            public string Linnworks_Order_Number { get; set; } = string.Empty;
            public string Order_Date { get; set; } = string.Empty;
            public string Required_Date { get; set; } = string.Empty;
            public string Postal_Service { get; set; } = string.Empty;
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
            public string Channel_Tracking_Ref { get; set; } = string.Empty;
            public string Price_Flag { get; set; } = string.Empty;
            public string Shipping_Code { get; set; } = string.Empty;
            public string Delivery_Telephone { get; set; } = string.Empty;
            public string Delivery_Email { get; set; } = string.Empty;
            public string Order_Type { get; set; } = string.Empty;
            public string Branch_Plan { get; set; } = string.Empty;
            public string Delivery_Town { get; set; } = string.Empty;
            public string Delivery_County { get; set; } = string.Empty;
            public string Delivery_Country_Code { get; set; } = string.Empty;
            public string Delivery_Country { get; set; } = string.Empty;
            public string Ship_To_Account_Number { get; set; } = string.Empty;
            public string Hold_Status { get; set; } = string.Empty;
        }

        private sealed class OrderDetail
        {
            public string Record_Type { get; set; } = string.Empty;
            public string Customer_Order_Reference { get; set; } = string.Empty;
            public int Line_Number { get; set; }
            public string JRSL_Product_Code { get; set; } = string.Empty;
            public string Blank1 { get; set; } = string.Empty;
            public int Quantity_Required { get; set; }
            public double Item_Cost { get; set; }
            public string Blank2 { get; set; } = string.Empty;
            public string Required_Date { get; set; } = string.Empty;
        }

        private bool SendReport(StringBuilder report, string accountNumber, SFtpSettings sftpSettings, string localFilePath)
        {
            string fileName = $"{accountNumber}_{DateTime.Now:ddMMyyyyHHmmss}.csv";

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

            // Build remote path (do not modify original sftpSettings.FullPath)
            try
            {
                string baseRemote = sftpSettings.FullPath ?? string.Empty;
                string remotePath;
                if (baseRemote.EndsWith("/")) remotePath = baseRemote + fileName;
                else if (baseRemote == string.Empty) remotePath = "/" + fileName;
                else remotePath = baseRemote + (baseRemote.EndsWith("/") ? "" : "/") + fileName;

                SFtpSettings toUse = new SFtpSettings()
                {
                    UserName = sftpSettings.UserName,
                    Password = sftpSettings.Password,
                    Server = sftpSettings.Server,
                    Port = sftpSettings.Port,
                    FullPath = remotePath
                };

                bool result = SendByFTP(report, toUse);

                if (result) this.Logger.WriteInfo($"SFTP upload successful -> {remotePath}");
                else this.Logger.WriteError($"SFTP upload failed -> {remotePath}");

                return result;
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"Error while preparing SFTP upload: {ex.Message}");
                return false;
            }
        }

        private void SaveLocalFile(StringBuilder report, string fileName, string localDirectory)
        {
            string dir = localDirectory?.Trim() ?? string.Empty;
            if (Path.HasExtension(dir)) dir = Path.GetDirectoryName(dir) ?? dir;
            if (string.IsNullOrWhiteSpace(dir)) throw new ArgumentException("Local directory path is empty.");
            if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
            string fullPath = Path.Combine(dir, fileName);

            // Write UTF-8 without BOM
            File.WriteAllText(fullPath, report.ToString(), new UTF8Encoding(false));
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
                {
                    this.Logger.WriteError($"SFTP upload reported failure: {uploadResult.ErrorMessage}");
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"Error while sending the file to the SFTP: {ex.Message}");
                return false;
            }
        }

        #region Order Folder / Notes helpers
        private void SetOrderFolder(Guid orderId, string folderName)
        {
            try
            {
                Api.Orders.AssignToFolder(new List<Guid> { orderId }, folderName);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"SetOrderFolder failed: {ex.Message}", ex);
            }
        }

        private void AddOrderNote(Guid orderId, string note)
        {
            try
            {
                var existing = Api.Orders.GetOrderNotes(orderId) ?? new List<OrderNote>();
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
        #endregion

        private List<OrderDetails> GetOrderDetails(string sources, string subSources, string sortCode, string sortDirection, int lastDays, bool ignoreUnknownSKUs, string extendedPropertyName, string folderUpdated)
        {
            List<OrderDetails> orders = new List<OrderDetails>();

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

            if (!string.IsNullOrWhiteSpace(folderUpdated))
            {
                if (filter.ListFields == null) filter.ListFields = new List<ListFieldFilter>();
                filter.ListFields.Add(new ListFieldFilter()
                {
                    FieldCode = FieldCode.FOLDER,
                    Value = folderUpdated,
                    Type = ListFieldFilterType.Is
                });
            }

            FieldCode sortingCode = FieldCode.GENERAL_INFO_ORDER_ID;
            ListSortDirection sortingDirection = ListSortDirection.Descending;
            if (!string.IsNullOrEmpty(sortCode) && sortCode.ToUpper() == "REFERENCE") sortingCode = FieldCode.GENERAL_INFO_REFERENCE_NUMBER;
            if (!string.IsNullOrEmpty(sortDirection) && sortDirection.ToUpper() == "ASCENDING") sortingDirection = ListSortDirection.Ascending;

            List<Guid> guids = Api.Orders.GetAllOpenOrders(filter, new List<FieldSorting>()
            {
                new FieldSorting()
                {
                    FieldCode = sortingCode,
                    Direction = sortingDirection,
                    Order = 0
                }
            }, Guid.Empty, "");

            if (guids == null || guids.Count == 0) return new List<OrderDetails>();

            if (guids.Count > 200)
            {
                for (int i = 0; i < guids.Count; i += 200)
                    orders.AddRange(Api.Orders.GetOrdersById(guids.Skip(i).Take(200).ToList()));
            }
            else
            {
                orders = Api.Orders.GetOrdersById(guids);
            }

            if (ignoreUnknownSKUs)
            {
                foreach (OrderDetails order in orders)
                    order.Items = order.Items.Where(item => item.ItemId != Guid.Empty && !string.IsNullOrEmpty(item.SKU)).ToList();
                orders = orders.Where(order => order.Items.Count > 0).ToList();
            }

            if (!string.IsNullOrWhiteSpace(extendedPropertyName))
            {
                for (int i = 0; i < orders.Count; i++)
                {
                    if (orders[i].ExtendedProperties.Any(ep => string.Equals(ep?.Name, extendedPropertyName, StringComparison.OrdinalIgnoreCase))) continue;
                    orders.RemoveAt(i);
                    i--;
                }
            }

            // Final sort
            List<OrderDetails> sortedOrders;
            if (sortingCode == FieldCode.GENERAL_INFO_ORDER_ID)
            {
                sortedOrders = (sortingDirection == ListSortDirection.Ascending) ? orders.OrderBy(o => o.NumOrderId).ToList() : orders.OrderByDescending(o => o.NumOrderId).ToList();
            }
            else
            {
                sortedOrders = (sortingDirection == ListSortDirection.Ascending) ? orders.OrderBy(o => o.GeneralInfo.ReferenceNum).ToList() : orders.OrderByDescending(o => o.GeneralInfo.ReferenceNum).ToList();
            }

            return sortedOrders;
        }
    }
}