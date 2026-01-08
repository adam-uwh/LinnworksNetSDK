using LinnworksAPI;
using System;
using System. Collections.Generic;
using System.Linq;

namespace LinnworksMacro
{
    public class LinnworksMacro :  LinnworksMacroHelpers. LinnworksMacroBase
    {
        /// <summary>
        /// UW Home Linnworks Default Stock Check Macro. 
        /// This macro checks new orders to determine if there is sufficient stock available for all items.
        /// 
        /// Logic:
        /// - If there is insufficient stock: 
        ///   - If the order has BackOrders extended property set to TRUE, assign to the specified out of stock folder
        ///   - Otherwise, assign to the specified to be cancelled folder
        /// - If there is sufficient stock:
        ///   - If the order has ChannelUpdatesRequired extended property set to TRUE, assign to the specified new orders folder
        ///   - Otherwise, assign to the specified updated folder
        /// 
        /// When insufficient stock is detected, an order note is added indicating the stock shortage.
        /// The macro uses the fulfilment location from the order details to check stock levels.
        /// If a stock item is not found, it is treated as insufficient stock and an error is logged.
        /// </summary>
        /// <param name="OrderIds">An array of GUID order IDs on which to perform operations (passed when a rules engine rule executes a macro)</param>
        /// <param name="outOfStockFolder">The folder name to assign orders with insufficient stock when back orders are allowed</param>
        /// <param name="toBeCancelledFolder">The folder name to assign orders with insufficient stock when back orders are not allowed</param>
        /// <param name="newFolder">The folder name to assign orders with sufficient stock that require channel updates</param>
        /// <param name="updatedFolder">The folder name to assign orders with sufficient stock that do not require channel updates</param>
        /// <param name="channelUpdatesRequiredProperty">The name of the extended property that indicates if channel updates are required</param>
        /// <param name="backOrdersProperty">The name of the extended property that indicates if back orders are allowed</param>

        public void Execute(
            Guid[] OrderIds,
            string outOfStockFolder,
            string toBeCancelledFolder,
            string newFolder,
            string updatedFolder,
            string channelUpdatesRequiredProperty,
            string backOrdersProperty)
        {
            Logger.WriteDebug("Starting macro");

            var orders = LoadOrderDetails(OrderIds);

            foreach (var order in orders)
            {
                bool insufficientStock = false;

                foreach (var item in order.Items)
                {
                    var request = new LinnworksAPI. GetStockLevelByLocationRequest
                    {
                        StockItemId = item.StockItemId,
                        LocationId = order.FulfilmentLocationId
                    };

                    var stockItemLevel = Api.Stock.GetStockLevelByLocation(request);
                    if (stockItemLevel == null)
                    {
                        Logger.WriteError($"Stock item not found for item {item.StockItemId} in order {order.OrderId}");
                        insufficientStock = true;
                        break;
                    }

                    if (stockItemLevel.StockLevel. Available < item. Quantity ||
                        stockItemLevel.StockLevel.Available < 0)
                    {
                        insufficientStock = true;
                        Logger.WriteInfo(
                            $"Insufficient stock for item {item.StockItemId} in order {order.OrderId}"
                        );
                        break;
                    }
                }

                // Check for ChannelUpdatesRequired extended property
                bool channelUpdatesRequired = order.ExtendedProperties != null &&
                    order.ExtendedProperties.Any(ep =>
                        ep. Name == channelUpdatesRequiredProperty &&
                        string.Equals(ep.Value, "TRUE", StringComparison.OrdinalIgnoreCase)
                    );

                // Check for BackOrders extended property
                bool backOrders = order.ExtendedProperties != null &&
                    order. ExtendedProperties. Any(ep =>
                        ep.Name == backOrdersProperty &&
                        string.Equals(ep.Value, "TRUE", StringComparison.OrdinalIgnoreCase)
                    );

                try
                {
                    if (insufficientStock)
                    {
                        if (backOrders)
                        {
                            Api.Orders.AssignToFolder(new List<Guid> { order.OrderId }, outOfStockFolder);
                            Logger. WriteInfo($"Order {order.OrderId} has insufficient stock and back orders allowed - assigned to '{outOfStockFolder}' folder.");
                        }
                        else
                        {
                            Api.Orders.AssignToFolder(new List<Guid> { order.OrderId }, toBeCancelledFolder);
                            Logger.WriteInfo($"Order {order. OrderId} has insufficient stock and back orders not allowed - assigned to '{toBeCancelledFolder}' folder.");
                        }

                        // Add order note without overwriting existing notes
                        var existingNotes = Api.Orders.GetOrderNotes(order. OrderId) ?? new List<OrderNote>();
                        existingNotes.Add(new OrderNote
                        {
                            Note = "Order has insufficient stock available for all lines.",
                            CreatedBy = "Rules Engine"
                        });
                        Api. Orders.SetOrderNotes(order.OrderId, existingNotes);
                        Logger.WriteInfo($"Order note added to order {order.OrderId}.");
                    }
                    else
                    {
                        if (channelUpdatesRequired)
                        {
                            Api.Orders.AssignToFolder(new List<Guid> { order.OrderId }, newFolder);
                            Logger.WriteInfo($"Order {order.OrderId} has sufficient stock and requires channel update - assigned to '{newFolder}' folder.");
                        }
                        else
                        {
                            Api.Orders.AssignToFolder(new List<Guid> { order.OrderId }, updatedFolder);
                            Logger. WriteInfo($"Order {order.OrderId} has sufficient stock and no channel update required - assigned to '{updatedFolder}' folder.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Logger.WriteError($"Failed to update order {order. OrderId}: {ex.Message}");
                }
            }
        }

        private List<OrderDetails> LoadOrderDetails(Guid[] orderIds)
        {
            try
            {
                return Api. Orders.GetOrdersById(orderIds. ToList());
            }
            catch (Exception ex)
            {
                Logger. WriteError($"Got error '{ex.Message}' when loading order details");
                return new List<OrderDetails>();
            }
        }
    }
}