using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using ProductService.Interfaces;
using ProductService.Services;

namespace ProductService.Processors;

// =============================================================================
// ResumeOrderProcessor — scoped processor for orders.resume topic
// =============================================================================
// LIFETIME: Scoped (InstancePerLifetimeScope), keyed to "orders.resume"
//
// Message key = order ID
//
// Delegates entirely to OrderPlayPauseAggregator, which validates the order
// exists and clears both pause flags. Status reverts to the fulfillment-derived
// status (Placed, PaymentConfirmed, or WarehousePicked).
// =============================================================================
public class ResumeOrderProcessor(OrderPlayPauseAggregator playPause, ILogger<ResumeOrderProcessor> logger)
    : IMessageProcessor
{
    public async Task ProcessAsync(ConsumeResult<string, byte[]> message, CancellationToken cancellationToken)
    {
        var orderId = message.Message.Key;

        if (string.IsNullOrEmpty(orderId))
        {
            logger.LogWarning("Skipping orders.resume message with missing key");
            return;
        }

        await playPause.ResumeAsync(orderId, cancellationToken);
    }
}
