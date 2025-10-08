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
            string folderName,
            string moveToFolder,
            string subSource,
            int filterTag, // Use -1 for no filter
            int tagNumber,
            string locationId,
            int lastDays
        )
        {
            Logger.WriteInfo($"Retrieving orders in folder '{folderName}' with subSource '{subSource}' from last {lastDays} days.");

            // Build filter for folder, subSource, tag (optional), and date
            var filter = new FieldsFilter();
            filter.ListFields = new List<ListFieldFilter>
            {
                new ListFieldFilter
                {
                    FieldCode = FieldCode.FOLDER,
                    Value = folderName,
                    Type = ListFieldFilterType.Is
                },
                new ListFieldFilter
                {
                    FieldCode = FieldCode.GENERAL_INFO_SUBSOURCE,
                    Value = subSource,
                    Type = ListFieldFilterType.Is
                }
            };
            if (filterTag != -1)
            {
                filter.ListFields.Add(new ListFieldFilter
                {
                    FieldCode = FieldCode.GENERAL_INFO_TAG,
                    Value = filterTag.ToString(),
                    Type = ListFieldFilterType.Is
                });
            }
            filter.DateFields = new List<DateFieldFilter>
            {
                new DateFieldFilter
                {
                    FieldCode = FieldCode.GENERAL_INFO_DATE,
                    Type = DateTimeFieldFilterType.LastDays,
                    Value = lastDays
                }
            };

            // Sort by reference number ascending
            var sorting = new List<FieldSorting>
            {
                new FieldSorting
                {
                    FieldCode = FieldCode.GENERAL_INFO_REFERENCE_NUMBER,
                    Direction = ListSortDirection.Ascending,
                    Order = 0
                }
            };

            Logger.WriteInfo("Querying order GUIDs from Linnworks...");
            Guid locationGuid;
            if (!Guid.TryParse(locationId, out locationGuid))
            {
                Logger.WriteError($"Invalid locationId: {locationId}, using Guid.Empty");
                locationGuid = Guid.Empty;
            }
            var guids = Api.Orders.GetAllOpenOrders(filter, sorting, locationGuid, "");
            Logger.WriteInfo($"Order GUIDs returned: {guids.Count}");

            if (guids == null || guids.Count == 0)
            {
                Logger.WriteInfo("No orders found matching the filter.");
                return;
            }

            // Fetch order details in batches of 200
            var allOrders = new List<OrderDetails>();
            for (int i = 0; i < guids.Count; i += 200)
            {
                var batch = guids.Skip(i).Take(200).ToList();
                Logger.WriteInfo($"Fetching order details for batch {i / 200 + 1}: {batch.Count} orders");
                allOrders.AddRange(Api.Orders.GetOrdersById(batch));
            }

            Logger.WriteInfo($"Total orders returned: {allOrders.Count}");
            if (allOrders.Count == 0)
            {
                Logger.WriteInfo("No order details found.");
                return;
            }

            // Move and tag each order
            foreach (var order in allOrders)
            {
                try
                {
                    Api.Orders.AssignToFolder(new List<Guid> { order.OrderId }, moveToFolder);
                    Logger.WriteInfo($"Order {order.OrderId} moved to folder {moveToFolder}.");

                    Api.Orders.ChangeOrderTag(new List<Guid> { order.OrderId }, tagNumber);
                    Logger.WriteInfo($"Order {order.OrderId} tagged with {tagNumber}.");
                }
                catch (Exception ex)
                {
                    Logger.WriteError($"Failed to update order {order.OrderId}: {ex.Message}");
                }
            }
        }
    }
}