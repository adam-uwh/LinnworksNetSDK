using System;
using System. Collections.Generic;
using System. Linq;
using LinnworksAPI;
using System.ComponentModel;

namespace LinnworksMacro
{
    public class LinnworksMacro : LinnworksMacroHelpers.LinnworksMacroBase
    {
        /// <summary>
        /// UW Home Linnworks Default Back in Stock Check Macro. 
        /// This macro retrieves orders from a specified folder and checks if stock is now available for all items.
        /// 
        /// Logic:
        /// - Orders are retrieved from the specified check folder at the specified location
        /// - For each order, stock availability is checked for all items
        /// - If there is insufficient stock:  
        ///   - If the order has BackOrders extended property set to TRUE, assign to the specified out of stock folder
        ///   - Otherwise, assign to the specified to be cancelled folder
        /// - If there is sufficient stock:
        ///   - If the order has ChannelUpdatesRequired extended property set to TRUE, assign to the specified new orders folder
        ///   - Otherwise, assign to the specified updated folder
        /// 
        /// When insufficient stock is detected, an order note is added indicating the stock shortage.
        /// When sufficient stock is detected, an order note is added indicating stock is now available.
        /// Orders with unknown SKUs can optionally be filtered out.
        /// If a stock item is not found, it is treated as insufficient stock and an error is logged.
        /// Parked orders are skipped.
        /// </summary>
        /// <param name="locationId">The GUID of the location to check stock levels against</param>
        /// <param name="checkFolder">The folder name to retrieve orders from for stock checking</param>
        /// <param name="outOfStockFolder">The folder name to assign orders with insufficient stock when back orders are allowed</param>
        /// <param name="toBeCancelledFolder">The folder name to assign orders with insufficient stock when back orders are not allowed</param>
        /// <param name="newFolder">The folder name to assign orders with sufficient stock that require channel updates</param>
        /// <param name="updatedFolder">The folder name to assign orders with sufficient stock that do not require channel updates</param>
        /// <param name="channelUpdatesRequiredProperty">The name of the extended property that indicates if channel updates are required</param>
        /// <param name="backOrdersProperty">The name of the extended property that indicates if back orders are allowed</param>
        /// <param name="ignoreUnknownSKUs">If true, orders with unlinked/unknown SKUs will be filtered out</param>

        public void Execute(
            string locationId,
            string checkFolder,
            string outOfStockFolder,
            string toBeCancelledFolder,
            string newFolder,
            string updatedFolder,
            string channelUpdatesRequiredProperty,
            string backOrdersProperty,
            bool ignoreUnknownSKUs)
        {
            Logger.WriteDebug("Starting macro");
            Logger.WriteInfo("Starting back in stock check with filters:");
            Logger.WriteInfo($"Check Folder: {checkFolder}, Location Guid: {locationId}, IgnoreUnknownSKUs:  {ignoreUnknownSKUs}");
            Logger.WriteInfo($"Out of Stock Folder: {outOfStockFolder}, To Be Cancelled Folder: {toBeCancelledFolder}");
            Logger.WriteInfo($"New Folder: {newFolder}, Updated Folder: {updatedFolder}");
            Logger.WriteInfo($"Channel Updates Property: {channelUpdatesRequiredProperty}, Back Orders Property: {backOrdersProperty}");

            Guid locationGuid;
            if (!Guid.TryParse(locationId, out locationGuid))
            {
                Logger. WriteError($"Invalid locationId:  {locationId}");
                return;
            }

            var orders = GetOrderDetails(checkFolder, ignoreUnknownSKUs, locationGuid);

            Logger. WriteInfo($"Total orders returned:  {orders.Count}");
            foreach (var order in orders)
            {
                Logger.WriteDebug($"OrderId: {order.OrderId}, Reference: {order.GeneralInfo.ReferenceNum}, NumOrderId: {order.NumOrderId}, Items: {order.Items. Count}");
            }

            foreach (var order in orders)
            {
                // Skip parked orders
                if (order.GeneralInfo != null && order.GeneralInfo. IsParked)
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
                        LocationId = locationGuid
                    };

                    var stockItemLevel = Api.Stock.GetStockLevelByLocation(request);
                    if (stockItemLevel == null)
                    {
                        Logger.WriteError($"Stock item not found for item {item.StockItemId} in order {order.OrderId}");
                        insufficientStock = true;
                        break;
                    }

                    if (stockItemLevel.StockLevel == null ||
                        stockItemLevel.StockLevel.Available < item.Quantity ||
                        stockItemLevel.StockLevel. Available < 0)
                    {
                        insufficientStock = true;
                        Logger. WriteInfo(
                            $"Insufficient stock for item {item. StockItemId} in order {order.OrderId}: required {item.Quantity}, available {(stockItemLevel.StockLevel?.Available.ToString() ?? "null")}"
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
                bool backOrders = order. ExtendedProperties != null &&
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
                            throw;
                        }

                        // Add order note without overwriting existing notes
                        try
                        {
                            var existingNotes = Api.Orders.GetOrderNotes(order. OrderId) ?? new List<OrderNote>();
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
                            Api.Orders. SetOrderNotes(order.OrderId, existingNotes);
                            Logger.WriteInfo($"Order note added to order {order.OrderId}.");
                        }
                        catch (Exception noteEx)
                        {
                            Logger. WriteError($"Failed to add note to order {order.OrderId}: {noteEx.Message}");
                            Logger.WriteDebug($"Note error stack trace: {noteEx. StackTrace}");
                        }
                    }
                    else
                    {
                        string targetFolder = channelUpdatesRequired ? newFolder :  updatedFolder;
                        string reason = channelUpdatesRequired ?  "requires channel update" : "no channel update required";

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
                            throw;
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
                                NoteDate = DateTime. UtcNow,
                                Internal = false,
                                Note = "Order updated as stock now available for all lines.",
                                CreatedBy = "Rules Engine",
                                NoteTypeId = null
                            });

                            Logger.WriteDebug($"Attempting to set {existingNotes.Count} notes for order {order.OrderId}");
                            Api.Orders.SetOrderNotes(order.OrderId, existingNotes);
                            Logger. WriteInfo($"Order note added to order {order.OrderId}.");
                        }
                        catch (Exception noteEx)
                        {
                            Logger.WriteError($"Failed to add note to order {order.OrderId}: {noteEx.Message}");
                            Logger.WriteDebug($"Note error stack trace: {noteEx. StackTrace}");
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

        private List<OrderDetails> GetOrderDetails(string folderName, bool ignoreUnknownSKUs, Guid locationGuid)
        {
            List<OrderDetails> orders = new List<OrderDetails>();

            // Filter for folder
            FieldsFilter filter = new FieldsFilter()
            {
                ListFields = new List<ListFieldFilter>()
                {
                    new ListFieldFilter()
                    {
                        FieldCode = FieldCode.FOLDER,
                        Value = folderName,
                        Type = ListFieldFilterType.Is
                    }
                }
            };

            // Sorting setup
            FieldCode sortingCode = FieldCode. GENERAL_INFO_REFERENCE_NUMBER;
            ListSortDirection sortingDirection = ListSortDirection. Ascending;

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
                    Logger.WriteInfo($"Fetching order details for batch {i / 200 + 1}:  {batch.Count} orders");
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
            if (sortingDirection == ListSortDirection.Ascending)
                sortedOrders = orders.OrderBy(order => order.GeneralInfo.ReferenceNum).ToList();
            else
                sortedOrders = orders.OrderByDescending(order => order. GeneralInfo.ReferenceNum).ToList();

            Logger.WriteInfo($"Sorted orders count: {sortedOrders.Count}");
            return sortedOrders;
        }
    }
}