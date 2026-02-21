using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using ProductService.Interfaces;
using ProductService.Services;

namespace ProductService.Processors;

// =============================================================================
// WarehouseProcessor — scoped processor for orders.warehouse-picked topic
// =============================================================================
// LIFETIME: Scoped (InstancePerLifetimeScope), keyed to "orders.warehouse-picked"
//
// Message key   = order ID
// Message value = name or ID of the warehouse staff member who picked the order
//
// Calls OrderDetailsAggregator to set PickedByWarehouseStaff on the OrderDetails
// and republish. Status advances to WarehousePicked (unless paused).
// =============================================================================
public class WarehouseProcessor(OrderDetailsAggregator aggregator, ILogger<WarehouseProcessor> logger)
    : IMessageProcessor
{
    public async Task ProcessAsync(ConsumeResult<string, string> message, CancellationToken cancellationToken)
    {
        var orderId = message.Message.Key;
        var pickedBy = message.Message.Value;

        if (string.IsNullOrEmpty(orderId) || string.IsNullOrEmpty(pickedBy))
        {
            logger.LogWarning("Skipping warehouse-picked message with missing key or value (orderId={OrderId})", orderId);
            return;
        }

        logger.LogInformation("Order warehouse-picked: orderId={OrderId}, pickedBy={PickedBy}", orderId, pickedBy);

        await aggregator.AddWarehousePickAsync(orderId, pickedBy, cancellationToken);
    }
}
