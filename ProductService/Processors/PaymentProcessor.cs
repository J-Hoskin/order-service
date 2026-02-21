using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using ProductService.Interfaces;
using ProductService.Services;

namespace ProductService.Processors;

// =============================================================================
// PaymentProcessor — scoped processor for orders.payment-confirmed topic
// =============================================================================
// LIFETIME: Scoped (InstancePerLifetimeScope), keyed to "orders.payment-confirmed"
//
// Message key   = order ID
// Message value = payment reference assigned by the payment provider
//
// Calls OrderDetailsAggregator to set PaymentReference on the OrderDetails
// for this order and republish. The OrderDetails Status advances to
// PaymentConfirmed (unless the order is paused or already warehouse-picked).
// =============================================================================
public class PaymentProcessor(OrderDetailsAggregator aggregator, ILogger<PaymentProcessor> logger)
    : IMessageProcessor
{
    public async Task ProcessAsync(ConsumeResult<string, string> message, CancellationToken cancellationToken)
    {
        var orderId = message.Message.Key;
        var paymentReference = message.Message.Value;

        if (string.IsNullOrEmpty(orderId) || string.IsNullOrEmpty(paymentReference))
        {
            logger.LogWarning("Skipping payment message with missing key or value (orderId={OrderId}, paymentReference={PaymentReference})", orderId, paymentReference);
            return;
        }

        logger.LogInformation("Payment confirmed: orderId={OrderId}, paymentReference={PaymentReference}", orderId, paymentReference);

        await aggregator.AddPaymentReferenceAsync(orderId, paymentReference, cancellationToken);
    }
}
