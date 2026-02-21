using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using ProductService.Extensions;
using ProductService.Interfaces;
using ProductService.Proto;
using ProductService.Services;

namespace ProductService.Processors;

// =============================================================================
// PauseOrderProcessor — scoped processor for orders.pause topic
// =============================================================================
// LIFETIME: Scoped (InstancePerLifetimeScope), keyed to "orders.pause"
//
// Message key   = order ID
// Message value = Protobuf PauseOrder (created_at, reason)
//
// Delegates entirely to OrderPlayPauseAggregator, which validates the order
// exists and sets PauseRequested = true on the OrderDetails.
// =============================================================================
public class PauseOrderProcessor(OrderPlayPauseAggregator playPause, ILogger<PauseOrderProcessor> logger)
    : IMessageProcessor
{
    public async Task ProcessAsync(ConsumeResult<string, byte[]> message, CancellationToken cancellationToken)
    {
        var orderId = message.Message.Key;

        if (string.IsNullOrEmpty(orderId))
        {
            logger.LogWarning("Skipping orders.pause message with missing key");
            return;
        }

        var bytes = message.Message.Value;
        var createdAt = bytes is { Length: > 0 }
            ? PauseOrder.Parser.ParseFrom(bytes).ToDomain().CreatedAt
            : DateTime.UtcNow;

        await playPause.PauseAsync(orderId, createdAt, cancellationToken);
    }
}
