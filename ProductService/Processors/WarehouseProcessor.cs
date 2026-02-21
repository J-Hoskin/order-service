using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using ProductService.Extensions;
using ProductService.Interfaces;
using ProductService.Proto;
using ProductService.Services;

namespace ProductService.Processors;

// =============================================================================
// WarehouseProcessor — scoped processor for orders.warehouse-picked topic
// =============================================================================
// LIFETIME: Scoped (InstancePerLifetimeScope), keyed to "orders.warehouse-picked"
//
// Message key   = order ID
// Message value = Protobuf WarehousePicked (picked_by)
//
// Calls OrderDetailsAggregator to set PickedByWarehouseStaff on the OrderDetails
// and republish. Status advances to WarehousePicked (unless paused).
// =============================================================================
public class WarehouseProcessor(OrderDetailsAggregator aggregator, ILogger<WarehouseProcessor> logger)
    : IMessageProcessor
{
    public async Task ProcessAsync(ConsumeResult<string, byte[]> message, CancellationToken cancellationToken)
    {
        var orderId = message.Message.Key;
        var bytes = message.Message.Value;

        if (string.IsNullOrEmpty(orderId) || bytes is null || bytes.Length == 0)
        {
            logger.LogWarning("Skipping warehouse-picked message with missing key or value (orderId={OrderId})", orderId);
            return;
        }

        string pickedBy;
        try
        {
            pickedBy = WarehousePicked.Parser.ParseFrom(bytes).ToDomain();
        }
        catch (InvalidProtocolBufferException ex)
        {
            logger.LogError(ex, "Skipping malformed protobuf on orders.warehouse-picked (orderId={OrderId}, offset={Offset})", orderId, message.TopicPartitionOffset);
            return;
        }

        logger.LogInformation("Order warehouse-picked: orderId={OrderId}, pickedBy={PickedBy}", orderId, pickedBy);

        await aggregator.AddWarehousePickAsync(orderId, pickedBy, cancellationToken);
    }
}
