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
    /// <summary>
    /// =====================================================
    /// Default JDE Order Export Macro
    /// =====================================================
    /// 
    /// SUMMARY:
    /// This Linnworks macro exports open orders to a JDE (JD Edwards) system via CSV file output.
    /// Files can be sent via SFTP or saved locally. Each order generates its own CSV file.
    /// 
    /// KEY LOGIC AND FLOW:
    /// 1. INITIALISATION
    ///    - Loads country map from Linnworks API (CountryId -> 2-letter CountryCode) for address resolution
    ///    - Configures SFTP settings from macro parameters (if not saving locally)
    /// 
    /// 2. ORDER RETRIEVAL
    ///    - Retrieves all open orders from the specified 'folderUpdated' folder
    ///    - Filters: Status=Paid(1), Not Parked, Not Locked, within lastDays date range
    ///    - No longer filters by source/subSource - folder is the only filter criteria
    ///    - Orders must have a 'SoldTo' extended property (case-insensitive) or they are moved to error folder
    /// 
    /// 3. ORDER PROCESSING (per order)
    ///    a) SoldTo Validation: Retrieves 'SoldTo'/'soldto' extended property (case-insensitive)
    ///       - If missing, order is moved to folderError with a note
    ///    
    ///    b) Customer Order Reference Resolution (via 'PrimaryPONumber' extended property):
    ///       - 'ReferenceNum' -> uses order.GeneralInfo.ReferenceNum
    ///       - 'SecondaryReference' -> uses order.GeneralInfo.SecondaryReference
    ///       - 'ExternalRef' -> uses order.GeneralInfo.ExternalReferenceNum
    ///       - 'Attribute' -> uses 'custpo' extended property value
    ///       - Default fallback -> uses ReferenceNum
    ///    
    ///    c) Dispatch Date Modification (via order extended properties):
    ///       - 'RequestDateModifier' (TRUE/FALSE) determines if modification applies
    ///       - 'RequestDateDays' specifies days to add to despatch date (OPTIONAL - defaults to 0)
    ///       - Weekend logic: if result falls on Saturday, adds 2 days; Sunday adds 1 day
    ///       - Empty/invalid values default to original despatch date
    ///    
    ///    d) Shipping Charge (via order extended properties):
    ///       - 'Carriage' (TRUE/FALSE) determines if shipping line is added
    ///       - 'CarriageSKU' specifies the SKU for the shipping line item (OPTIONAL - if empty, no shipping line added)
    ///       - Only adds if Carriage=TRUE, CarriageSKU has value, and PostageCostExTax > 0
    ///    
    ///    e) Other Extended Property Mappings:
    ///       - 'PriceOverride' -> Price_Flag field (TRUE = "P", FALSE/empty = blank)
    ///       - 'OrderType' -> Order_Type field
    ///       - 'Branch' -> Branch_Plan field
    ///       - 'HoldStatus' -> Hold_Status field (OPTIONAL - defaults to blank)
    ///       - 'ShipTo' -> Ship_To_Account_Number field (OPTIONAL - defaults to blank)
    ///       - 'trackno' -> Channel_Tracking_Ref field (OPTIONAL - defaults to blank)
    ///    
    ///    f) SKU Filtering (optional):
    ///       - If ignoreUnknownSKUs=true, filters out items with empty ItemId or SKU
    ///       - Orders with no valid items after filtering are moved to folderError
    /// 
    /// 4. CSV GENERATION
    ///    - OH (Order Header) line: Contains order-level information (29 pipe-delimited fields)
    ///    - OD (Order Detail) lines: One per item (9 pipe-delimited fields)
    ///    - Optional shipping charge as additional OD line
    /// 
    /// 5. FILE OUTPUT
    ///    - Filename format: {SoldTo}_{ddMMyyyyHHmmss}.csv
    ///    - If localFilePath provided: saves to local directory (UTF-8 without BOM)
    ///    - Otherwise: uploads via SFTP to configured server
    /// 
    /// 6. POST-PROCESSING
    ///    - Successful: Order moved to folderCompleted, note added with timestamp
    ///    - Failed: Order moved to folderError with error details in note
    /// 
    /// PASSED PARAMETERS:
    ///    - SFTPServer, SFTPPort, SFTPUsername, SFTPPassword, SFTPFolderPath: SFTP connection settings
    ///    - localFilePath: If provided, saves files locally instead of SFTP
    ///    - sortField: 'ORDERID' or 'REFERENCE' for sorting orders
    ///    - sortDirection: 'ASCENDING' or 'DESCENDING'
    ///    - lastDays: Number of days to look back for orders
    ///    - shipCode: Shipping code value for the export
    ///    - folderUpdated: Source folder to retrieve orders from
    ///    - folderCompleted: Destination folder for successfully processed orders
    ///    - folderError: Destination folder for orders with errors
    ///    - ignoreUnknownSKUs: If true, filters out items with invalid/empty SKUs
    /// 
    /// OPTIONAL EXTENDED PROPERTIES (defaults to blank if not present):
    ///    - ShipTo
    ///    - HoldStatus
    ///    - CarriageSKU
    ///    - RequestDateDays
    ///    - trackno
    /// 
    /// ERROR HANDLING:
    ///    - All exceptions are caught and logged
    ///    - Failed orders are moved to folderError with descriptive notes
    ///    - Missing required extended properties (SoldTo) trigger error folder assignment
    ///    - Missing optional extended properties use blank/default values
    /// =====================================================
    /// </summary>
    public class LinnworksMacro : LinnworksMacroHelpers.LinnworksMacroBase
    {
        private readonly StringBuilder _csvFile = new();
        private readonly Dictionary<Guid, OrderHead> _dictHead = new();
        private readonly Dictionary<Guid, List<OrderDetail>> _dictDetails = new();

        // Cached countries map: key = CountryId.ToString() -> value = CountryCode (2-letter)
        private Dictionary<string, string> _countriesMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public void Execute(
            string SFTPServer,
            int SFTPPort,
            string SFTPUsername,
            string SFTPPassword,
            string SFTPFolderPath,
            string localFilePath,
            string sortField,
            string sortDirection,
            int lastDays,
            string shipCode,
            string folderUpdated,
            string folderCompleted,
            string folderError,
            bool ignoreUnknownSKUs
            )
        {
            this.Logger.WriteInfo("Starting script - Default JDE Order Export Macro (per-order export)");

            // Validate required folder parameters
            if (string.IsNullOrWhiteSpace(folderUpdated))
            {
                this.Logger.WriteError("folderUpdated parameter is required but was empty. Exiting.");
                return;
            }
            if (string.IsNullOrWhiteSpace(folderCompleted))
            {
                this.Logger.WriteError("folderCompleted parameter is required but was empty. Exiting.");
                return;
            }
            if (string.IsNullOrWhiteSpace(folderError))
            {
                folderError = "Order Errors"; // Default error folder
                this.Logger.WriteInfo($"folderError not provided, defaulting to '{folderError}'");
            }

            SFtpSettings sftpSettings = new SFtpSettings()
            {
                UserName = SFTPUsername,
                Password = SFTPPassword,
                Server = SFTPServer?.StartsWith("sftp://") == true ? SFTPServer : $"sftp://{SFTPServer}",
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

            // Get orders matching filters (folder-based only, no source/subSource filtering)
            List<OrderDetails> orders = GetOrderDetails(sortField, sortDirection, lastDays, ignoreUnknownSKUs, folderUpdated);

            this.Logger.WriteInfo($"Found {orders.Count} orders to process in folder '{folderUpdated}'.");

            foreach (OrderDetails ods in orders)
            {
                try
                {
                    // Per-order soldto extended property (case-insensitive) - REQUIRED
                    string soldTo = GetExtendedPropertyValueCaseInsensitive(ods, "soldto");

                    if (string.IsNullOrWhiteSpace(soldTo))
                    {
                        // Move to error folder and add note
                        MoveOrderToErrorFolder(ods, folderError, "No SoldTo extended property found.");
                        continue;
                    }

                    // Optionally filter unknown SKUs
                    List<OrderItem> items = ods.Items?.ToList() ?? new List<OrderItem>();
                    if (ignoreUnknownSKUs)
                    {
                        items = items.Where(item => item.ItemId != Guid.Empty && !string.IsNullOrEmpty(item.SKU)).ToList();
                        if (items.Count == 0)
                        {
                            MoveOrderToErrorFolder(ods, folderError, "Order contained no valid items after filtering unknown SKUs.");
                            continue;
                        }
                    }

                    // Retrieve all extended properties needed for this order
                    var orderExtProps = BuildExtendedPropertyCache(ods);

                    // Build per-order CSV
                    _csvFile.Clear();
                    _dictHead.Clear();
                    _dictDetails.Clear();

                    bool buildSuccess = BuildOrderMapping(ods, items, orderExtProps, shipCode, soldTo, folderError);
                    if (!buildSuccess)
                    {
                        // Order already moved to error folder within BuildOrderMapping
                        continue;
                    }

                    CompileCsv();

                    // Send file: if localFilePath provided, save locally; otherwise SFTP
                    bool sent = SendReport(_csvFile, soldTo, sftpSettings, localFilePath);

                    if (sent)
                    {
                        try { SetOrderFolder(ods.OrderId, folderCompleted); }
                        catch (Exception ex) { this.Logger.WriteError($"SetOrderFolder to '{folderCompleted}' failed: {ex.Message}"); }

                        try { AddOrderNote(ods.OrderId, $"Order successfully sent to JDE on {DateTime.Now:yyyy-MM-dd HH:mm:ss} using soldto '{soldTo}'."); }
                        catch { }

                        this.Logger.WriteInfo($"Order {ods.NumOrderId} exported and sent for soldto '{soldTo}'.");
                    }
                    else
                    {
                        MoveOrderToErrorFolder(ods, folderError, $"Failed to send order to JDE for soldto '{soldTo}'.");
                    }
                }
                catch (Exception ex)
                {
                    this.Logger.WriteError($"Unexpected error processing order {ods.NumOrderId}: {ex.Message}");
                    MoveOrderToErrorFolder(ods, folderError, $"Unexpected processing error: {ex.Message}");
                }
            }

            this.Logger.WriteInfo("Script finished.");
        }

        /// <summary>
        /// Moves an order to the specified error folder and adds a note with the reason.
        /// </summary>
        private void MoveOrderToErrorFolder(OrderDetails ods, string folderError, string reason)
        {
            try
            {
                SetOrderFolder(ods.OrderId, folderError);
                AddOrderNote(ods.OrderId, $"{reason} Order moved to '{folderError}'.");
                this.Logger.WriteInfo($"Order {ods.NumOrderId} moved to '{folderError}': {reason}");
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"Failed to move order {ods.NumOrderId} to '{folderError}': {ex.Message}");
            }
        }

        /// <summary>
        /// Builds a dictionary cache of all extended properties for an order (case-insensitive keys).
        /// </summary>
        private Dictionary<string, string> BuildExtendedPropertyCache(OrderDetails ods)
        {
            var cache = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (ods?.ExtendedProperties != null)
            {
                foreach (var ep in ods.ExtendedProperties)
                {
                    if (ep != null && !string.IsNullOrWhiteSpace(ep.Name))
                    {
                        // Store by key to ensure case-insensitive lookup
                        var key = ep.Name.Trim();
                        if (!cache.ContainsKey(key))
                            cache[key] = ep.Value ?? string.Empty;
                    }
                }
            }
            return cache;
        }

        /// <summary>
        /// Gets an extended property value from the cache (case-insensitive).
        /// Returns empty string if not found.
        /// </summary>
        private string GetCachedExtendedProperty(Dictionary<string, string> cache, string propertyName)
        {
            if (cache != null && !string.IsNullOrWhiteSpace(propertyName) && cache.TryGetValue(propertyName.Trim(), out var value))
                return value ?? string.Empty;
            return string.Empty;
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

        /// <summary>
        /// Builds the order mapping for CSV export. Returns false if order should be skipped due to errors.
        /// </summary>
        private bool BuildOrderMapping(
            OrderDetails ods,
            List<OrderItem> items,
            Dictionary<string, string> extProps,
            string shipCode,
            string soldTo,
            string folderError)
        {
            try
            {
                // Resolve Customer Order Reference based on PrimaryPONumber extended property
                string customerOrderReference = ResolveCustomerOrderReference(ods, extProps);

                // Get dispatch date modification settings from extended properties
                DateTime despatchBy = ods.GeneralInfo.DespatchByDate;
                DateTime requiredDate = ComputeRequiredDateFromExtProps(despatchBy, extProps);

                string postalService = (ods.ShippingInfo?.PostalServiceName == "Standard") ? "" : ods.ShippingInfo?.PostalServiceName ?? string.Empty;
                string channelTrackingRef = GetCachedExtendedProperty(extProps, "trackno");

                string deliveryTelephone = GetTelephoneFromOrder(ods);
                string deliveryEmail = GetEmailFromOrder(ods);
                if (string.IsNullOrWhiteSpace(deliveryEmail)) deliveryEmail = "noreply@uwhome.com";

                string deliveryTown = SafeGetAddressString(ods, "Town");
                string deliveryCounty = SafeGetAddressString(ods, "Region");
                if (string.IsNullOrWhiteSpace(deliveryCounty)) deliveryCounty = SafeGetAddressString(ods, "County");

                string deliveryCountryCode = ResolveCountryCodeFromOrder(ods);
                string deliveryCountry = SafeGetAddressString(ods, "Country");

                // Get values from extended properties (previously passed as parameters)
                // These are all OPTIONAL - default to empty string if not present
                
                // PriceOverride: TRUE = "P", FALSE/empty = blank
                string priceOverrideFlag = GetCachedExtendedProperty(extProps, "priceoverride");
                string priceFlag = (!string.IsNullOrWhiteSpace(priceOverrideFlag) && 
                                    priceOverrideFlag.Trim().Equals("TRUE", StringComparison.OrdinalIgnoreCase)) 
                                    ? "P" 
                                    : string.Empty;

                string orderType = GetCachedExtendedProperty(extProps, "ordertype");
                string branchPlan = GetCachedExtendedProperty(extProps, "branch");
                string holdStatus = GetCachedExtendedProperty(extProps, "holdstatus");      // OPTIONAL - blank if not found
                string shipToValue = GetCachedExtendedProperty(extProps, "shipto");         // OPTIONAL - blank if not found

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
                    Ship_To_Account_Number = shipToValue,
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

                // Shipping charge from extended properties: Carriage (TRUE/FALSE) and CarriageSKU (OPTIONAL)
                bool addShippingCharge = false;
                string carriageFlag = GetCachedExtendedProperty(extProps, "carriage");
                string carriageSku = GetCachedExtendedProperty(extProps, "carriagesku");  // OPTIONAL - blank if not found

                // Only add shipping charge if Carriage=TRUE AND CarriageSKU has a value
                if (!string.IsNullOrWhiteSpace(carriageFlag) &&
                    carriageFlag.Trim().Equals("TRUE", StringComparison.OrdinalIgnoreCase) &&
                    !string.IsNullOrWhiteSpace(carriageSku))
                {
                    addShippingCharge = true;
                }

                if (addShippingCharge && ods.ShippingInfo?.PostageCostExTax > 0)
                {
                    var shipDetail = new OrderDetail()
                    {
                        Record_Type = "OD",
                        Customer_Order_Reference = customerOrderReference,
                        Line_Number = _dictDetails.ContainsKey(ods.OrderId) ? _dictDetails[ods.OrderId].Count + 1 : lineNumber,
                        JRSL_Product_Code = carriageSku.Trim(),
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

                return true;
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"Error building order mapping for order {ods.NumOrderId}: {ex.Message}");
                MoveOrderToErrorFolder(ods, folderError, $"Error building order mapping: {ex.Message}");
                return false;
            }
        }

        /// <summary>
        /// Resolves the Customer Order Reference based on the PrimaryPONumber extended property.
        /// Logic:
        /// - 'ReferenceNum' -> order.GeneralInfo.ReferenceNum
        /// - 'SecondaryReference' -> order.GeneralInfo.SecondaryReference
        /// - 'ExternalRef' -> order.GeneralInfo.ExternalReferenceNum
        /// - 'Attribute' -> extended property 'custpo'
        /// - Default/empty -> order.GeneralInfo.ReferenceNum
        /// </summary>
        private string ResolveCustomerOrderReference(OrderDetails ods, Dictionary<string, string> extProps)
        {
            string primaryPONumber = GetCachedExtendedProperty(extProps, "primaryponumber");
            string result = string.Empty;

            if (!string.IsNullOrWhiteSpace(primaryPONumber))
            {
                string normalized = primaryPONumber.Trim();

                if (normalized.Equals("ReferenceNum", StringComparison.OrdinalIgnoreCase))
                {
                    result = ods.GeneralInfo?.ReferenceNum ?? string.Empty;
                }
                else if (normalized.Equals("SecondaryReference", StringComparison.OrdinalIgnoreCase))
                {
                    result = ods.GeneralInfo?.SecondaryReference ?? string.Empty;
                }
                else if (normalized.Equals("ExternalRef", StringComparison.OrdinalIgnoreCase))
                {
                    result = ods.GeneralInfo?.ExternalReferenceNum ?? string.Empty;
                }
                else if (normalized.Equals("Attribute", StringComparison.OrdinalIgnoreCase))
                {
                    result = GetCachedExtendedProperty(extProps, "custpo");
                }
            }

            // If result is empty, default to ReferenceNum
            if (string.IsNullOrWhiteSpace(result))
            {
                result = ods.GeneralInfo?.ReferenceNum ?? string.Empty;
            }

            return result;
        }

        /// <summary>
        /// Computes the required date based on extended properties RequestDateModifier and RequestDateDays.
        /// If RequestDateModifier is TRUE, adds RequestDateDays to the despatch date (skipping weekends).
        /// RequestDateDays is OPTIONAL - defaults to 0 if not present or invalid.
        /// If any values are missing or invalid, returns the original despatch date.
        /// </summary>
        private DateTime ComputeRequiredDateFromExtProps(DateTime despatchBy, Dictionary<string, string> extProps)
        {
            try
            {
                string modifierFlag = GetCachedExtendedProperty(extProps, "requestdatemodifier");
                if (string.IsNullOrWhiteSpace(modifierFlag) ||
                    !modifierFlag.Trim().Equals("TRUE", StringComparison.OrdinalIgnoreCase))
                {
                    return despatchBy;
                }

                // RequestDateDays is OPTIONAL - default to 0 if not present or invalid
                string daysStr = GetCachedExtendedProperty(extProps, "requestdatedays");
                int dispatchModifier = 0;
                if (!string.IsNullOrWhiteSpace(daysStr))
                {
                    int.TryParse(daysStr.Trim(), out dispatchModifier);
                }

                // If dispatchModifier is 0, just return original date
                if (dispatchModifier == 0)
                {
                    return despatchBy;
                }

                DateTime result = despatchBy.AddDays(dispatchModifier);

                // Skip weekends
                if (result.DayOfWeek == DayOfWeek.Sunday)
                    result = result.AddDays(1);
                else if (result.DayOfWeek == DayOfWeek.Saturday)
                    result = result.AddDays(2);

                return result;
            }
            catch
            {
                // On any error, return original date
                return despatchBy;
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

        /// <summary>
        /// Gets an extended property value by name (case-insensitive search).
        /// Handles all case variations: soldto, SoldTo, SOLDTO, etc.
        /// </summary>
        private string GetExtendedPropertyValueCaseInsensitive(OrderDetails ods, string name)
        {
            if (ods?.ExtendedProperties == null || string.IsNullOrWhiteSpace(name))
                return string.Empty;

            var ep = ods.ExtendedProperties.FirstOrDefault(x =>
                !string.IsNullOrWhiteSpace(x?.Name) &&
                x.Name.Trim().Equals(name.Trim(), StringComparison.OrdinalIgnoreCase));

            return ep?.Value ?? string.Empty;
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
        /// <summary>
        /// Email lookup - FIXED to use correct Linnworks API property names.
        /// The CustomerAddress class has 'EmailAddress' property (not 'Email').
        /// Also checks BillingAddress as a fallback.
        /// </summary>
        private string GetEmailFromOrder(OrderDetails ods)
        {
            // First, try the shipping/delivery address (CustomerInfo.Address)
            try
            {
                dynamic addr = ods?.CustomerInfo?.Address;
                if (addr != null)
                {
                    // Primary property name in CustomerAddress class is 'EmailAddress'
                    try { string v = addr.EmailAddress; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    // Also try alternate property names just in case
                    try { string v = addr.Email; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    try { string v = addr.cEmailAddress; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                }
            }
            catch { }

            // Second, try the billing address (CustomerInfo.BillingAddress)
            try
            {
                dynamic billingAddr = ods?.CustomerInfo?.BillingAddress;
                if (billingAddr != null)
                {
                    try { string v = billingAddr.EmailAddress; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    try { string v = billingAddr.Email; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    try { string v = billingAddr.cEmailAddress; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                }
            }
            catch { }

            // Third, try CustomerInfo directly (some API versions may have email at this level)
            try
            {
                dynamic ci = ods?.CustomerInfo;
                if (ci != null)
                {
                    try { string v = ci.EmailAddress; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    try { string v = ci.Email; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
                    try { string v = ci.cEmailAddress; if (!string.IsNullOrWhiteSpace(v)) return v; } catch { }
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

        /// <summary>
        /// Retrieves all open orders from the specified folder.
        /// No longer filters by source/subSource - only by folder.
        /// </summary>
        private List<OrderDetails> GetOrderDetails(string sortCode, string sortDirection, int lastDays, bool ignoreUnknownSKUs, string folderUpdated)
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
                },
                ListFields = new List<ListFieldFilter>()
            };

            // Filter by folder (required)
            if (!string.IsNullOrWhiteSpace(folderUpdated))
            {
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