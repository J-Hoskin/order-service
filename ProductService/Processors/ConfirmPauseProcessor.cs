using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using ProductService.Extensions;
using ProductService.Interfaces;
using ProductService.Proto;
using ProductService.Services;

namespace ProductService.Processors;

// =============================================================================
// ConfirmPauseProcessor — scoped processor for orders.confirm-pause topic
// =============================================================================
// LIFETIME: Scoped (InstancePerLifetimeScope), keyed to "orders.confirm-pause"
//
// Message key   = order ID
// Message value = Protobuf ConfirmPause (created_at, confirmed_by)
//
// Delegates entirely to OrderPlayPauseAggregator, which validates the order
// exists and sets PauseConfirmed = true. If PauseRequested is already true,
// status becomes Paused.
// =============================================================================
public class ConfirmPauseProcessor(OrderPlayPauseAggregator playPause, ILogger<ConfirmPauseProcessor> logger)
    : IMessageProcessor
{
    public async Task ProcessAsync(ConsumeResult<string, byte[]> message, CancellationToken cancellationToken)
    {
        var orderId = message.Message.Key;

        if (string.IsNullOrEmpty(orderId))
        {
            logger.LogWarning("Skipping orders.confirm-pause message with missing key");
            return;
        }

        var bytes = message.Message.Value;
        DateTime createdAt;
        try
        {
            createdAt = bytes is { Length: > 0 }
                ? ConfirmPause.Parser.ParseFrom(bytes).ToDomain().CreatedAt
                : DateTime.UtcNow;
        }
        catch (InvalidProtocolBufferException ex)
        {
            logger.LogError(ex, "Skipping malformed protobuf on orders.confirm-pause (orderId={OrderId}, offset={Offset})", orderId, message.TopicPartitionOffset);
            return;
        }

        await playPause.ConfirmPauseAsync(orderId, createdAt, cancellationToken);
    }
}
