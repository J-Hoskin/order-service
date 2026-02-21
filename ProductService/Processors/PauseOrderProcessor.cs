using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using ProductService.Interfaces;
using ProductService.Models;
using ProductService.Services;

namespace ProductService.Processors;

// =============================================================================
// PauseOrderProcessor — scoped processor for orders.pause topic
// =============================================================================
// LIFETIME: Scoped (InstancePerLifetimeScope), keyed to "orders.pause"
//
// Message key   = order ID
// Message value = JSON PauseOrderPayload (CreatedAt, Reason)
//
// Delegates entirely to OrderPlayPauseAggregator, which validates the order
// exists and sets PauseRequested = true on the OrderDetails.
// =============================================================================
public class PauseOrderProcessor(OrderPlayPauseAggregator playPause, ILogger<PauseOrderProcessor> logger)
    : IMessageProcessor
{
    public async Task ProcessAsync(ConsumeResult<string, string> message, CancellationToken cancellationToken)
    {
        var orderId = message.Message.Key;

        if (string.IsNullOrEmpty(orderId))
        {
            logger.LogWarning("Skipping orders.pause message with missing key");
            return;
        }

        PauseOrderPayload? payload = null;
        if (!string.IsNullOrEmpty(message.Message.Value))
            payload = JsonSerializer.Deserialize<PauseOrderPayload>(message.Message.Value);

        var createdAt = payload?.CreatedAt ?? DateTime.UtcNow;
        await playPause.PauseAsync(orderId, createdAt, cancellationToken);
    }
}
