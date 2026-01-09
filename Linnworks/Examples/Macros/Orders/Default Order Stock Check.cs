using LinnworksAPI;
using System;
using System.Collections.Generic;
using System.Linq;

namespace LinnworksMacro
{
    public class LinnworksMacro : LinnworksMacroHelpers.LinnworksMacroBase
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
                // Skip parked orders
                if (order.GeneralInfo != null && order.GeneralInfo.IsParked)
                {
                    Logger.WriteInfo($"Order {order.OrderId} is parked - skipping.");
                    continue;
                }

                bool insufficientStock = false;

                foreach (var item in order.Items)
                {
                    var request = new LinnworksAPI.GetStockLevelByLocationRequest
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

                    if (stockItemLevel.StockLevel.Available < item.Quantity ||
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
                        ep.Name == channelUpdatesRequiredProperty &&
                        string.Equals(ep.Value, "TRUE", StringComparison.OrdinalIgnoreCase)
                    );

                // Check for BackOrders extended property
                bool backOrders = order.ExtendedProperties != null &&
                    order.ExtendedProperties.Any(ep =>
                        ep.Name == backOrdersProperty &&
                        string.Equals(ep.Value, "TRUE", StringComparison.OrdinalIgnoreCase)
                    );

                try
                {
                    if (insufficientStock)
                    {
                        string targetFolder = backOrders ? outOfStockFolder : toBeCancelledFolder;
                        string reason = backOrders ? "back orders allowed" : "back orders not allowed";
                        
                        Logger.WriteDebug($"Attempting to assign order {order.OrderId} to folder '{targetFolder}' (insufficient stock, {reason})");
                        
                        try
                        {
                            Api.Orders.AssignToFolder(new List<Guid> { order.OrderId }, targetFolder);
                            Logger.WriteInfo($"Order {order.OrderId} has insufficient stock and {reason} - assigned to '{targetFolder}' folder.");
                        }
                        catch (Exception folderEx)
                        {
                            Logger.WriteError($"Failed to assign order {order.OrderId} to folder '{targetFolder}': {folderEx.Message}");
                            Logger.WriteDebug($"Folder assignment error details: {folderEx}");
                            throw; // Re-throw to be caught by outer try-catch
                        }

                        // Add order note without overwriting existing notes
                        try
                        {
                            var existingNotes = Api.Orders.GetOrderNotes(order.OrderId) ?? new List<OrderNote>();
                            Logger.WriteDebug($"Found {existingNotes.Count} existing notes for order {order.OrderId}");
                            
                            existingNotes.Add(new OrderNote
                            {
                                OrderNoteId = Guid.NewGuid(),
                                OrderId = order.OrderId,
                                NoteDate = DateTime.UtcNow,
                                Internal = false,
                                Note = "Order has insufficient stock available for all lines.",
                                CreatedBy = "Rules Engine",
                                NoteTypeId = null
                            });
                            
                            Logger.WriteDebug($"Attempting to set {existingNotes.Count} notes for order {order.OrderId}");
                            Api.Orders.SetOrderNotes(order.OrderId, existingNotes);
                            Logger.WriteInfo($"Order note added to order {order.OrderId}.");
                        }
                        catch (Exception noteEx)
                        {
                            Logger.WriteError($"Failed to add note to order {order.OrderId}: {noteEx.Message}");
                            Logger.WriteDebug($"Note error stack trace: {noteEx.StackTrace}");
                        }
                    }
                    else
                    {
                        string targetFolder = channelUpdatesRequired ? newFolder : updatedFolder;
                        string reason = channelUpdatesRequired ? "requires channel update" : "no channel update required";
                        
                        Logger.WriteDebug($"Attempting to assign order {order.OrderId} to folder '{targetFolder}' (sufficient stock, {reason})");
                        
                        try
                        {
                            Api.Orders.AssignToFolder(new List<Guid> { order.OrderId }, targetFolder);
                            Logger.WriteInfo($"Order {order.OrderId} has sufficient stock and {reason} - assigned to '{targetFolder}' folder.");
                        }
                        catch (Exception folderEx)
                        {
                            Logger.WriteError($"Failed to assign order {order.OrderId} to folder '{targetFolder}': {folderEx.Message}");
                            Logger.WriteDebug($"Folder assignment error details: {folderEx}");
                            throw; // Re-throw to be caught by outer try-catch
                        }
                    }
                }
                catch (Exception ex)
                {
                    Logger.WriteError($"Failed to update order {order.OrderId}: {ex.Message}");
                    Logger.WriteDebug($"Error stack trace: {ex.StackTrace}");
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