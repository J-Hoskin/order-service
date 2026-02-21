using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using ProductService.Interfaces;
using ProductService.Models;
using ProductService.Services;

namespace ProductService.Processors;

// =============================================================================
// ConfirmPauseProcessor — scoped processor for orders.confirm-pause topic
// =============================================================================
// LIFETIME: Scoped (InstancePerLifetimeScope), keyed to "orders.confirm-pause"
//
// Message key   = order ID
// Message value = JSON ConfirmPausePayload (CreatedAt, ConfirmedBy)
//
// Delegates entirely to OrderPlayPauseAggregator, which validates the order
// exists and sets PauseConfirmed = true. If PauseRequested is already true,
// status becomes Paused.
// =============================================================================
public class ConfirmPauseProcessor(OrderPlayPauseAggregator playPause, ILogger<ConfirmPauseProcessor> logger)
    : IMessageProcessor
{
    public async Task ProcessAsync(ConsumeResult<string, string> message, CancellationToken cancellationToken)
    {
        var orderId = message.Message.Key;

        if (string.IsNullOrEmpty(orderId))
        {
            logger.LogWarning("Skipping orders.confirm-pause message with missing key");
            return;
        }

        ConfirmPausePayload? payload = null;
        if (!string.IsNullOrEmpty(message.Message.Value))
            payload = JsonSerializer.Deserialize<ConfirmPausePayload>(message.Message.Value);

        var createdAt = payload?.CreatedAt ?? DateTime.UtcNow;
        await playPause.ConfirmPauseAsync(orderId, createdAt, cancellationToken);
    }
}
