using System;
using System.Collections.Generic;
using System.Linq;
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
         * ***************************************/

        private readonly StringBuilder _csvFile = new();
        private readonly Dictionary<Guid, OrderHead> _dictHead = new();
        private readonly Dictionary<Guid, List<OrderDetail>> _dictDetails = new();


        public void Execute(
            string sources,
            string subSources,
            string accountNumber,
            string SFTPServer,
            int SFTPPort,
            string SFTPUsername,
            string SFTPPassword,
            string SFTPFolderPath,
            string sortField,
            string sortDirection,
            int lastDays,
            bool ignoreUnknownSKUs,
            string extendedPropertyName,
            bool addShippingCharge,
            string shippingChargeSku
            )
        {
            this.Logger.WriteInfo("Starting script 1245004");

            SFtpSettings sftpSettings = new SFtpSettings()
            {
                UserName = SFTPUsername,
                Password = SFTPPassword,
                Server = SFTPServer.StartsWith("sftp://") ? SFTPServer : $"sftp://{SFTPServer}",
                Port = SFTPPort,
                FullPath = SFTPFolderPath
            };

            // Get Order Details
            List<OrderDetails> orders = GetOrderDetails(sources, subSources, sortField, sortDirection, lastDays, ignoreUnknownSKUs, extendedPropertyName);

            // Map Order Data to Dictionary
            AddDictionaryData(orders, addShippingCharge, shippingChargeSku);

            // Compile CSV
            CompileCsv();

            // Send it through ftp
            if (SendReport(_csvFile, accountNumber, sftpSettings))
                this.Logger.WriteInfo("End script 1234004");
            else
                this.Logger.WriteInfo("Failed sending csv file");
        }

        private void AddDictionaryData(List<OrderDetails> lod, bool addShippingSku, string shippingSku)
        {
            foreach (OrderDetails ods in lod)
            {
                if (!_dictHead.ContainsKey(ods.OrderId))
                {
                    _dictHead.Add(ods.OrderId, new OrderHead()
                    {
                        Record_Type = "OH",
                        Customer_Order_Number = ods.GeneralInfo.SecondaryReference,
                        Internal_Order_Id = ods.NumOrderId.ToString(),
                        Order_Date = ods.GeneralInfo.ReceivedDate.ToString("yyyyMMdd"),
                        Requested_Date = ods.GeneralInfo.DespatchByDate.ToString("yyyyMMdd"),
                        Delivery_Method = (ods.ShippingInfo.PostalServiceName == "Standard") ? "" : ods.ShippingInfo.PostalServiceName,
                        Delivery_Charge = "",
                        Ship_To_Reference_Code = "",
                        Ship_To = ods.CustomerInfo.Address.FullName,
                        Address1 = ods.CustomerInfo.Address.Address1,
                        Address2 = ods.CustomerInfo.Address.Address2,
                        Address3 = ods.CustomerInfo.Address.Address3,
                        Address4 = ods.CustomerInfo.Address.Town,
                        Postcode = ods.CustomerInfo.Address.PostCode,
                        Telephone_Number = "",
                        EmailAddress = "",
                        Special_Instructions = ods.Notes.Count > 0 ? String.Join(" - ", ods.Notes.Select(x => x.Note)).Replace("\n", " ") : ""
                    });
                }

                int lineNumber = 1;
                foreach (OrderItem oi in ods.Items)
                {
                    OrderDetail detail = new OrderDetail()
                    {
                        Record_Type = "OD",
                        Customer_Order_Number = ods.GeneralInfo.SecondaryReference,
                        Line_Number = lineNumber,
                        JRSL_Product_Code = string.IsNullOrWhiteSpace(oi.SKU) ? oi.ChannelSKU : oi.SKU,
                        Customer_Own_Item_Code = "",
                        Quantity_Required = oi.Quantity,
                        Unit_Price = Math.Round(oi.Cost / (oi.Quantity == 0 ? 1 : oi.Quantity), 2),
                        Line_Value = "",
                        Required_Date = ods.GeneralInfo.DespatchByDate.ToString("yyyyMMdd"),
                        Special_Instructions = ""
                    };

                    if (_dictDetails.ContainsKey(ods.OrderId))
                    {
                        detail.Line_Number = ++lineNumber;
                        _dictDetails[ods.OrderId].Add(detail);
                    }
                    else
                        _dictDetails.Add(ods.OrderId, new List<OrderDetail>() { detail });
                }

                if (ods.ShippingInfo.PostageCostExTax > 0 && addShippingSku)
                {
                    OrderDetail detail = new OrderDetail()
                    {
                        Record_Type = "OD",
                        Customer_Order_Number = ods.GeneralInfo.SecondaryReference,
                        Line_Number = _dictDetails[ods.OrderId].Count,
                        JRSL_Product_Code = shippingSku,
                        Customer_Own_Item_Code = string.Empty,
                        Quantity_Required = 1,
                        Unit_Price = ods.ShippingInfo.PostageCostExTax,
                        Line_Value = string.Empty,
                        Required_Date = ods.GeneralInfo.DespatchByDate.ToString("yyyyMMdd"),
                        Special_Instructions = string.Empty
                    };

                    if (_dictDetails.ContainsKey(ods.OrderId))
                        _dictDetails[ods.OrderId].Add(detail);
                    else
                        _dictDetails.Add(ods.OrderId, new List<OrderDetail>() { detail });
                }
            }
        }

        private List<OrderDetails> GetOrderDetails(string sources, string subSources, string sortCode, string sortDirection, int lastDays, bool ignoreUnknownSKUs, string extendedPropertyName)
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

            foreach (string source in sources.Split(',').Select(source => source.Trim()))
            {
                if (!string.IsNullOrWhiteSpace(source))
                {
                    filter.TextFields.Add(new TextFieldFilter()
                    {
                        FieldCode = FieldCode.GENERAL_INFO_SOURCE,
                        Text = source,
                        Type = TextFieldFilterType.Equal
                    });
                }
            }

            foreach (string subsource in subSources.Split(',').Select(subsource => subsource.Trim()))
            {
                if (!string.IsNullOrWhiteSpace(subsource))
                {
                    filter.TextFields.Add(new TextFieldFilter()
                    {
                        FieldCode = FieldCode.GENERAL_INFO_SUBSOURCE,
                        Text = subsource,
                        Type = TextFieldFilterType.Equal
                    });
                }
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

        private void CompileCsv()
        {
            _csvFile.Append(""); // OH Headers
            _csvFile.AppendLine(""); // OD Headers

            foreach (KeyValuePair<Guid, OrderHead> oh in _dictHead)
            {
                AddOrderHeader(oh.Value);

                if (_dictDetails.ContainsKey(oh.Key))
                {
                    foreach (OrderDetail order in _dictDetails[oh.Key])
                    {
                        AddOrderDetails(order);
                    }
                }
            }
        }

        private void AddOrderHeader(OrderHead oh)
        {
            _csvFile.AppendLine(string.Join("|"
                , oh.Record_Type
                , oh.Customer_Order_Number
                , oh.Internal_Order_Id
                , oh.Order_Date
                , oh.Requested_Date
                , oh.Delivery_Method
                , oh.Delivery_Charge
                , oh.Ship_To_Reference_Code
                , oh.Ship_To
                , oh.Address1
                , oh.Address2
                , oh.Address3
                , oh.Address4
                , oh.Postcode
                , oh.Telephone_Number
                , oh.EmailAddress
                , oh.Special_Instructions
                , "P" // All headers should end with this column
                ));
        }
        private void AddOrderDetails(OrderDetail od)
        {
            _csvFile.AppendLine(string.Join("|"
                , od.Record_Type
                , od.Customer_Order_Number
                , od.Line_Number
                , od.JRSL_Product_Code
                , od.Customer_Own_Item_Code
                , od.Quantity_Required
                , od.Unit_Price
                , od.Line_Value
                , od.Required_Date
                , od.Special_Instructions
                ));
        }
        private sealed class OrderHead
        {
            public string Record_Type { get; set; } = string.Empty;
            public string Customer_Order_Number { get; set; } = string.Empty;
            public string Internal_Order_Id { get; set; } = string.Empty;
            public string Order_Date { get; set; } = string.Empty;
            public string Requested_Date { get; set; } = string.Empty;
            public string Delivery_Method { get; set; } = string.Empty;
            public string Delivery_Charge { get; set; } = string.Empty;
            public string Ship_To_Reference_Code { get; set; } = string.Empty;
            public string Ship_To { get; set; } = string.Empty;
            public string Address1 { get; set; } = string.Empty;
            public string Address2 { get; set; } = string.Empty;
            public string Address3 { get; set; } = string.Empty;
            public string Address4 { get; set; } = string.Empty;
            public string Postcode { get; set; } = string.Empty;
            public string Telephone_Number { get; set; } = string.Empty;
            public string EmailAddress { get; set; } = string.Empty;
            public string Special_Instructions { get; set; } = string.Empty;
        }
        private sealed class OrderDetail
        {
            public string Record_Type { get; set; } = string.Empty;
            public string Customer_Order_Number { get; set; } = string.Empty;
            public int Line_Number { get; set; }
            public string JRSL_Product_Code { get; set; } = string.Empty;
            public string Customer_Own_Item_Code { get; set; } = string.Empty;
            public int Quantity_Required { get; set; }
            public double Unit_Price { get; set; }
            public string Line_Value { get; set; } = string.Empty;
            public string Required_Date { get; set; } = string.Empty;
            public string Special_Instructions { get; set; } = string.Empty;
        }

        /** FTP **/
        private bool SendReport(StringBuilder report, string accountNumber, SFtpSettings sftpSettings)
        {
            string fileName = $"{(sftpSettings.FullPath.EndsWith("/") ? "" : "/")}{accountNumber}_{DateTime.Now.ToString("ddMMyyyyHHmmss")}.csv";
            sftpSettings.FullPath += fileName;
            return SendByFTP(report, sftpSettings);
        }

        private bool SendByFTP(StringBuilder report, SFtpSettings sftpSettings)
        {
            try
            {
                using var upload = this.ProxyFactory.GetSFtpUploadProxy(sftpSettings);
                upload.Write(report.ToString());

                SftpUploadResult uploadResult = upload.CompleteUpload();
                this.Logger.WriteDebug("upload: " + uploadResult.IsSuccess + " (-> " + uploadResult.ErrorMessage);
                if (!uploadResult.IsSuccess)
                    throw new ArgumentException(uploadResult.ErrorMessage);
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"Error while sending the file to the FTP: {ex.Message}");
                return false;
            }
            return true;
        }
    }
}