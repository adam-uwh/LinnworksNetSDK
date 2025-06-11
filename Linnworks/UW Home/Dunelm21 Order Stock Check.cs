using LinnworksAPI;
using System;
using System.Collections.Generic;
using System.Linq;

namespace LinnworksMacro
{
    public class LinnworksMacro : LinnworksMacroHelpers.LinnworksMacroBase
    {
        /// <summary>
        /// The entry point for this macro, this function will be executed and its result returned when the macro is run
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

                try
                {
                    if (insufficientStock)
                    {
                        Api.Orders.ChangeOrderTag(new List<Guid> { order.OrderId }, 6);
                        Logger.WriteInfo($"Order {order.OrderId} tagged with 6 due to insufficient stock.");

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
                        // All items have sufficient stock, set status to PAID (1)
                        Api.Orders.ChangeStatus(new List<Guid> { order.OrderId }, 1);
                        Logger.WriteInfo($"Order {order.OrderId} status changed to PAID.");
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