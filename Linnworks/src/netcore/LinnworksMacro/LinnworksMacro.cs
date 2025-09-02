using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using LinnworksAPI;
using LinnworksMacroHelpers.Classes;
using System.ComponentModel;

namespace LinnworksMacro
{
    public class LinnworksMacro : LinnworksMacroHelpers.LinnworksMacroBase
    {
        public void Execute(
            string subSource,
            string notifyAcknowledge,
            string notifyOOS,
            string notifyBIS,
            string notifyShipped,
            string notifyCancelled,
            int tagValue,
            string newFolder,
            string oosFolder,
            string bisFolder,
            string SFTPServer,
            int SFTPPort,
            string SFTPUsername,
            string SFTPPassword,
            string SFTPFolderRoot,
            string acknowledgeDirectory,
            string oosDirectory,
            string bisDirectory,
            string shippedDirectory,
            string cancelDirectory,
            string filetype,
            string sortField,
            string sortDirection,
            int lookBackDays,
            string Source
        )
        {
            this.Logger.WriteInfo("Starting macro channel updater");

            var notificationConfigs = new[]
            {
                new { Notify = notifyAcknowledge, Folder = newFolder, Directory = acknowledgeDirectory, Type = "Open" },
                new { Notify = notifyOOS, Folder = oosFolder, Directory = oosDirectory, Type = "Open" },
                new { Notify = notifyBIS, Folder = bisFolder, Directory = bisDirectory, Type = "Open" },
                new { Notify = notifyShipped, Folder = shippedDirectory, Directory = shippedDirectory, Type = "Shipped" },
                new { Notify = notifyCancelled, Folder = cancelDirectory, Directory = cancelDirectory, Type = "Cancelled" }
            };

            foreach (var config in notificationConfigs)
            {
                if (!string.Equals(config.Notify, "TRUE", StringComparison.OrdinalIgnoreCase))
                    continue;

                List<OrderDetails> orders = new List<OrderDetails>();

                if (config.Type == "Open")
                {
                    orders = GetFilteredOpenOrders(subSource, config.Folder, tagValue, sortField, sortDirection);
                }
                else
                {
                    bool isShipped = config.Type == "Shipped";
                    orders = GetFilteredProcessedOrders(subSource, Source, sortField, sortDirection, lookBackDays, isShipped);
                }

                if (orders.Count == 0)
                {
                    this.Logger.WriteInfo($"No orders found for folder {config.Folder}");
                    continue;
                }

                var csv = GenerateCsv(orders);
                var sftpSettings = new SFtpSettings
                {
                    UserName = SFTPUsername,
                    Password = SFTPPassword,
                    Server = SFTPServer.StartsWith("sftp://") ? SFTPServer : $"sftp://{SFTPServer}",
                    Port = SFTPPort,
                    FullPath = $"{SFTPFolderRoot}/{config.Directory}/"
                };

                var fileName = $"Orders_{config.Folder}_{DateTime.Now:yyyyMMddHHmmss}.{filetype}";
                sftpSettings.FullPath += fileName;

                if (SendByFTP(csv, sftpSettings))
                    this.Logger.WriteInfo($"CSV sent for {config.Folder} to {sftpSettings.FullPath}");
                else
                    this.Logger.WriteError($"Failed to send CSV for {config.Folder}");
            }

            this.Logger.WriteInfo("Macro export complete");
        }

        private List<OrderDetails> GetFilteredOpenOrders(string subSource, string folderName, int tagValue, string sortField, string sortDirection)
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
                    new ListFieldFilter { FieldCode = FieldCode.FOLDER, Value = folderName, Type = ListFieldFilterType.Is },
                    new ListFieldFilter { FieldCode = FieldCode.GENERAL_INFO_TAG, Value = tagValue.ToString(), Type = ListFieldFilterType.Is }
                },
                TextFields = new List<TextFieldFilter>
                {
                    new TextFieldFilter { FieldCode = FieldCode.GENERAL_INFO_SUBSOURCE, Text = subSource, Type = TextFieldFilterType.Equal }
                }
            };

            FieldCode sortingCode = sortField.ToUpper() switch
            {
                "ORDERID" => FieldCode.GENERAL_INFO_ORDER_ID,
                "REFERENCE" => FieldCode.GENERAL_INFO_REFERENCE_NUMBER,
                _ => FieldCode.GENERAL_INFO_ORDER_ID
            };

            ListSortDirection sortingDirection = sortDirection.ToUpper() == "ASCENDING"
                ? ListSortDirection.Ascending
                : ListSortDirection.Descending;

            var guids = Api.Orders.GetAllOpenOrders(filter, new List<FieldSorting>
            {
                new FieldSorting { FieldCode = sortingCode, Direction = sortingDirection, Order = 0 }
            }, Guid.Empty, "");

            var orders = guids.Count > 0
                ? Api.Orders.GetOrdersById(guids)
                : new List<OrderDetails>();

            // Sort orders
            orders = sortingCode == FieldCode.GENERAL_INFO_ORDER_ID
                ? (sortingDirection == ListSortDirection.Ascending
                    ? orders.OrderBy(o => o.NumOrderId).ToList()
                    : orders.OrderByDescending(o => o.NumOrderId).ToList())
                : (sortingDirection == ListSortDirection.Ascending
                    ? orders.OrderBy(o => o.GeneralInfo.ReferenceNum).ToList()
                    : orders.OrderByDescending(o => o.GeneralInfo.ReferenceNum).ToList());

            return orders;
        }

        private List<OrderDetails> GetFilteredProcessedOrders(
            string subSource,
            string source,
            string sortField,
            string sortDirection,
            int lookBackDays,
            bool isShipped
        )
        {
            var orders = new List<OrderDetails>();
            DateTime toDate = DateTime.Today;
            DateTime fromDate = toDate.AddDays(-lookBackDays);

            var searchFilters = new List<SearchFilters>
            {
                new SearchFilters { SearchField = SearchFieldTypes.SubSource, SearchTerm = subSource },
                new SearchFilters { SearchField = SearchFieldTypes.Source, SearchTerm = source }
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

            var response = Api.ProcessedOrders.SearchProcessedOrders(request);

            foreach (var processedOrder in response.ProcessedOrders.Data)
            {
                var orderDetails = Api.Orders.GetOrderById(processedOrder.pkOrderID);
                if (orderDetails != null && !orderDetails.FolderName.Contains("Completed"))
                {
                    orders.Add(orderDetails);
                }
            }

            // Sorting
            if (sortField.ToUpper() == "ORDERID")
            {
                orders = sortDirection.ToUpper() == "ASCENDING"
                    ? orders.OrderBy(o => o.NumOrderId).ToList()
                    : orders.OrderByDescending(o => o.NumOrderId).ToList();
            }
            else
            {
                orders = sortDirection.ToUpper() == "ASCENDING"
                    ? orders.OrderBy(o => o.GeneralInfo.ReferenceNum).ToList()
                    : orders.OrderByDescending(o => o.GeneralInfo.ReferenceNum).ToList();
            }

            return orders;
        }

        private StringBuilder GenerateCsv(List<OrderDetails> orders)
        {
            var csv = new StringBuilder();
            csv.AppendLine("OrderNumber,CustomerName,Address,Items");

            foreach (var order in orders)
            {
                var address = $"{order.CustomerInfo.Address.FullName}, {order.CustomerInfo.Address.Address1}, {order.CustomerInfo.Address.Address2}, {order.CustomerInfo.Address.Town}, {order.CustomerInfo.Address.PostCode}";
                var items = string.Join(";", order.Items.Select(i => $"{i.SKU} x{i.Quantity}"));
                csv.AppendLine($"{order.NumOrderId},{order.CustomerInfo.Address.FullName},\"{address}\",\"{items}\"");
            }
            return csv;
        }

        private bool SendByFTP(StringBuilder report, SFtpSettings sftpSettings)
        {
            try
            {
                using var upload = this.ProxyFactory.GetSFtpUploadProxy(sftpSettings);
                upload.Write(report.ToString());
                var uploadResult = upload.CompleteUpload();
                if (!uploadResult.IsSuccess)
                    throw new ArgumentException(uploadResult.ErrorMessage);
            }
            catch (Exception ex)
            {
                this.Logger.WriteError($"Error while sending the file to SFTP: {ex.Message}");
                return false;
            }
            return true;
        }
    }
}