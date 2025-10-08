using System;
using System.Collections.Generic;
using System.Linq;
using LinnworksAPI;
//using LinnworksMacroHelpers.Classes;
using System.ComponentModel;
//using Newtonsoft.Json; 

namespace LinnworksMacro
{
    public class LinnworksMacro : LinnworksMacroHelpers.LinnworksMacroBase
    {
        public void Execute(
            string subSource,
            int tag,
            string emailAddresses,
            int lastDays
        )
        {
            Logger.WriteInfo($"Starting processed order retrieval for last {lastDays} days, Tag: {tag}, SubSource: {subSource}");

            // Build filter for processed orders (status = 2)
            var filter = new FieldsFilter()
            {
                ListFields = new List<ListFieldFilter>()
                {
                    new ListFieldFilter()
                    {
                        FieldCode = FieldCode.GENERAL_INFO_STATUS,
                        Value = "2", // 2 = PROCESSED
                        Type = ListFieldFilterType.Is
                    }
                },
                DateFields = new List<DateFieldFilter>()
                {
                    new DateFieldFilter()
                    {
                        FieldCode = FieldCode.GENERAL_INFO_DATE,
                        Type = DateTimeFieldFilterType.OlderThan,
                        Value = lastDays
                    }
                }
            };

            // Add subSource filter if provided
            if (!string.IsNullOrWhiteSpace(subSource))
            {
                filter.ListFields.Add(new ListFieldFilter()
                {
                    FieldCode = FieldCode.GENERAL_INFO_SUBSOURCE,
                    Value = subSource,
                    Type = ListFieldFilterType.Is
                });
            }

            // Query orders
            Logger.WriteInfo("Querying processed order GUIDs from Linnworks...");
            var guids = Api.Orders.GetAllOpenOrders(filter, null, Guid.Empty, "");
            Logger.WriteInfo($"Order GUIDs returned: {guids.Count}");

            // Fetch order details in batches of 200
            var orders = new List<OrderDetails>();
            if (guids.Count > 0)
            {
                for (int i = 0; i < guids.Count; i += 200)
                {
                    var batch = guids.Skip(i).Take(200).ToList();
                    orders.AddRange(Api.Orders.GetOrdersById(batch));
                }
            }

            // Filter orders by tag/marker in ExtendedProperties
            var taggedOrders = orders.Where(order =>
                order.GeneralInfo != null &&
                order.GeneralInfo.Marker == tag
            ).ToList();
            Logger.WriteInfo($"Tagged processed orders found: {taggedOrders.Count}");

            // Next: email details to provided addresses
        }

        private List<OrderDetails> GetOrderDetails(string folderName, int lastDays, bool ignoreUnknownSKUs, Guid locationGuid)
        {
            List<OrderDetails> orders = new List<OrderDetails>();

            // Filter for paid, folder, and date
            FieldsFilter filter = new FieldsFilter()
            {
                ListFields = new List<ListFieldFilter>()
                {
                    new ListFieldFilter()
                    {
                        FieldCode = FieldCode.GENERAL_INFO_STATUS, // Use status as a ListFieldFilter
                        Value = "1",  // 1 corresponds to PAID status
                        Type = ListFieldFilterType.Is
                    },
                    new ListFieldFilter()
                    {
                        FieldCode = FieldCode.FOLDER,
                        Value = folderName,
                        Type = ListFieldFilterType.Is
                    }
                },
                DateFields = new List<DateFieldFilter>()
                {
                    new DateFieldFilter()
                    {
                        FieldCode = FieldCode.GENERAL_INFO_DATE,
                        Type = DateTimeFieldFilterType.OlderThan,
                        Value = lastDays
                    }
                }
            };

            // Log the filter object as JSON
            //Logger.WriteInfo("Final filter: " + JsonConvert.SerializeObject(filter, Formatting.Indented));

            // Sorting setup
            FieldCode sortingCode = FieldCode.GENERAL_INFO_REFERENCE_NUMBER;
            ListSortDirection sortingDirection = ListSortDirection.Ascending;

            Logger.WriteInfo("Querying order GUIDs from Linnworks...");
            List<Guid> guids = Api.Orders.GetAllOpenOrders(filter, new List<FieldSorting>()
            {
                new FieldSorting()
                {
                    FieldCode = sortingCode,
                    Direction = sortingDirection,
                    Order = 0
                }
            }, locationGuid, "");

            Logger.WriteInfo($"Order GUIDs returned: {guids.Count}");

            if (guids.Count > 200)
            {
                for (int i = 0; i < guids.Count; i += 200)
                {
                    var batch = guids.Skip(i).Take(200).ToList();
                    Logger.WriteInfo($"Fetching order details for batch {i / 200 + 1}: {batch.Count} orders");
                    orders.AddRange(Api.Orders.GetOrdersById(batch));
                }
            }
            else if (guids.Count <= 200 && guids.Count > 0)
            {
                Logger.WriteInfo("Fetching order details for all orders in a single batch.");
                orders = Api.Orders.GetOrdersById(guids);
            }
            else
            {
                Logger.WriteInfo("No orders found matching the filter.");
            }

            // If enabled, filter the order items which are not present in Linnworks
            if (ignoreUnknownSKUs)
            {
                Logger.WriteInfo("Filtering out orders with unlinked items (unknown SKUs)...");
                foreach (OrderDetails order in orders)
                {
                    order.Items = order.Items.Where(item => item.ItemId != Guid.Empty && !string.IsNullOrEmpty(item.SKU)).ToList();
                }
                // Skip orders which do not have any valid item
                orders = orders.Where(order => order.Items.Count > 0).ToList();
                Logger.WriteInfo($"Orders remaining after filtering unknown SKUs: {orders.Count}");
            }

            // Final sort
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

            Logger.WriteInfo($"Sorted orders count: {sortedOrders.Count}");
            return sortedOrders;
        }
    }
}