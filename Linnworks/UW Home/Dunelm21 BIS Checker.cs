using System;
using System.Collections.Generic;
using System.Linq;
using LinnworksAPI;
using LinnworksMacroHelpers.Classes;
using System.ComponentModel;

namespace LinnworksMacro
{
    public class LinnworksMacro : LinnworksMacroHelpers.LinnworksMacroBase
    {
        public void Execute(
            string subSources,
            int tagNumber,
            int lastDays,
            bool ignoreUnknownSKUs
        )
        {
            Logger.WriteInfo("Starting order retrieval with filters:");
            Logger.WriteInfo($"SubSources: {subSources}, Tag: {tagNumber}, LastDays: {lastDays}, IgnoreUnknownSKUs: {ignoreUnknownSKUs}");

            var sortedOrders = GetOrderDetails(subSources, lastDays, ignoreUnknownSKUs);

            Logger.WriteInfo($"Total orders returned: {sortedOrders.Count}");
            foreach (var order in sortedOrders)
            {
                Logger.WriteInfo($"OrderId: {order.OrderId}, Reference: {order.GeneralInfo.ReferenceNum}, NumOrderId: {order.NumOrderId}, Items: {order.Items.Count}");
            }

            foreach (var order in sortedOrders)
            {
                bool insufficientStock = false;

                foreach (var item in order.Items)
                {
                    var request = new LinnworksAPI.GetStockLevelByLocationRequest
                    {
                        StockItemId = item.StockItemId,
                        LocationId = new Guid("132db06e-f55d-40d1-9705-67fa0068dc3c") // replace with your actual location if needed
                    };

                    var stockItemLevel = Api.Stock.GetStockLevelByLocation(request);
                    if (stockItemLevel == null)
                    {
                        Logger.WriteError($"Stock item not found for item {item.StockItemId} in order {order.OrderId}");
                        insufficientStock = true;
                        break;
                    }

                    // Null or negative available stock is insufficient
                    if (stockItemLevel.StockLevel == null ||
                        stockItemLevel.StockLevel.Available < item.Quantity ||
                        stockItemLevel.StockLevel.Available < 0)
                    {
                        insufficientStock = true;
                        Logger.WriteInfo(
                            $"Insufficient stock for item {item.StockItemId} in order {order.OrderId}: required {item.Quantity}, available {(stockItemLevel.StockLevel?.Available.ToString() ?? "null")}"
                        );
                        break;
                    }
                }

                if (!insufficientStock)
                {
                    try
                    {
                        Api.Orders.ChangeStatus(new List<Guid> { order.OrderId }, 1); // Set to PAID
                        Logger.WriteInfo($"Order {order.OrderId} status changed to PAID.");

                        Api.Orders.ChangeOrderTag(new List<Guid> { order.OrderId }, tagNumber); // Tag 6
                        Logger.WriteInfo($"Order {order.OrderId} tagged with {tagNumber}.");

                        // Add order note without overwriting existing notes
                        var existingNotes = Api.Orders.GetOrderNotes(order.OrderId) ?? new List<OrderNote>();
                        existingNotes.Add(new OrderNote
                        {
                            Note = "Order updated to PAID as stock now available for all lines.",
                            CreatedBy = "Rules Engine"
                        });
                        Api.Orders.SetOrderNotes(order.OrderId, existingNotes);
                        Logger.WriteInfo($"Order note added to order {order.OrderId}.");
                    }
                    catch (Exception ex)
                    {
                        Logger.WriteError($"Failed to update order {order.OrderId}: {ex.Message}");
                    }
                }
            }
        }

        private List<OrderDetails> GetOrderDetails(string subSources, int lastDays, bool ignoreUnknownSKUs)
        {
            List<OrderDetails> orders = new List<OrderDetails>();

            // Filter for paid, tagged, and date
            FieldsFilter filter = new FieldsFilter()
            {
                NumericFields = new List<NumericFieldFilter>()
                {
                    new NumericFieldFilter() { FieldCode = FieldCode.GENERAL_INFO_STATUS, Type = NumericFieldFilterType.Equal, Value = 0 }
                },
                TextFields = new List<TextFieldFilter>(),
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
            }, new Guid("132db06e-f55d-40d1-9705-67fa0068dc3c"), "");

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