using LinnworksAPI;
using System;
using System.Collections.Generic;
using System.Linq;

namespace LinnworksMacro
{
    public class LinnworksMacro : LinnworksMacroHelpers.LinnworksMacroBase
    {
        /// <summary>
        /// Linworks New Order In Stock Check Macro.
        /// The entry point for this macro, this function will be executed and its result returned when the macro is run
        /// This macro checks a new order to see if there is enough stock available for all items in the order.
        /// If there is insufficient stock, it will move the order to the 'Out of Stock' folder and add a note to the order. 
        /// If there is sufficient stock, it will change the order status to PAID and move the order to the 'New' folder.
        /// It will also tag the order with tag 6 if it requires channel updates, otherwise it will remove any tag.
        /// It checks the 'ChannelUpdatesRequired' extended property to determine if channel updates are needed.
        /// If the stock item is not found, it will log an error and treat it as insufficient stock.
        /// It uses the fulfilment location from the order details to check stock levels.
        /// </summary>
        /// <param name="OrderIds">An array of GUID order IDs on which to perform operations (passed when a rules engine rule executes a macro)</param>

        public void Execute(Guid[] OrderIds)

        {
            Logger.WriteDebug("Starting macro");

            var orders = LoadOrderDetails(OrderIds);

            foreach (var order in orders)
            {
                bool insufficientStock = false;

                foreach (var item in order.Items)
                {
                    var request = new LinnworksAPI.GetStockLevelByLocationRequest
                    {
                        StockItemId = item.StockItemId,
                        LocationId = order.FulfilmentLocationId // Use the location from order details
                    };

                    var stockItemLevel = Api.Stock.GetStockLevelByLocation(request);
                    if (stockItemLevel == null)
                    {
                        Logger.WriteError($"Stock item not found for item {item.StockItemId} in order {order.OrderId}");
                        insufficientStock = true;
                        break;
                    }

                    // Null or negative available stock is insufficient
                    if (stockItemLevel == null ||
                        stockItemLevel.StockLevel.Available < item.Quantity ||
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
                        ep.Name == "ChannelUpdatesRequired" &&
                        string.Equals(ep.Value, "TRUE", StringComparison.OrdinalIgnoreCase)
                    );

                try
                {
                    if (insufficientStock)
                    {

                        if (channelUpdatesRequired)
                        {
                            Api.Orders.ChangeOrderTag(new List<Guid> { order.OrderId }, 6);
                            Api.Orders.AssignToFolder(new List<Guid> { order.OrderId }, "Out of Stock");
                            Logger.WriteInfo($"Order {order.OrderId} requires channel updates so tagged with 6 and moved to Out of Stock folder due to insufficient stock.");
                        }
                        else
                        {
                            Api.Orders.ChangeOrderTag(new List<Guid> { order.OrderId }, null);
                            Api.Orders.AssignToFolder(new List<Guid> { order.OrderId }, "Out of Stock");
                            Logger.WriteInfo($"Order {order.OrderId} requires no channel updates so moved to Out of Stock folder due to insufficient stock.");

                        }
                        
                        // Add order note without overwriting existing notes
                            var existingNotes = Api.Orders.GetOrderNotes(order.OrderId) ?? new List<OrderNote>();
                        existingNotes.Add(new OrderNote
                        {
                            Note = "Order has insufficient stock available for all lines.",
                            CreatedBy = "Rules Engine"
                        });
                        Api.Orders.SetOrderNotes(order.OrderId, existingNotes);
                        Logger.WriteInfo($"Order note added to order {order.OrderId}.");
                    }
                    else
                    {
                        if (channelUpdatesRequired)
                        {
                            // All items have sufficient stock, set status to PAID (1)
                            Api.Orders.ChangeOrderTag(new List<Guid> { order.OrderId }, 6);
                            Api.Orders.ChangeStatus(new List<Guid> { order.OrderId }, 1);
                            Api.Orders.AssignToFolder(new List<Guid> { order.OrderId }, "New");
                            Logger.WriteInfo($"Order {order.OrderId} status changed to PAID awaiting channel update and saved to NEW folder.");
                        }
                        else
                        {
                            // All items have sufficient stock, set status to PAID (1)
                            Api.Orders.ChangeOrderTag(new List<Guid> { order.OrderId }, null);
                            Api.Orders.ChangeStatus(new List<Guid> { order.OrderId }, 1);
                            Api.Orders.AssignToFolder(new List<Guid> { order.OrderId }, "Updated");
                            Logger.WriteInfo($"Order {order.OrderId} status changed to PAID no update required and moved to Updated folder.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Logger.WriteError($"Failed to update order {order.OrderId}: {ex.Message}");
                }
            }
        }

        private List<OrderDetails> LoadOrderDetails(Guid[] orderIds)
        {
            try
            {
                return Api.Orders.GetOrdersById(orderIds.ToList());
            }
            catch (Exception ex)
            {
                Logger.WriteError($"Got error '{ex.Message}' when loading order details");
                return new List<OrderDetails>();
            }
        }
    }
}