using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using ProductService.Extensions;
using ProductService.Interfaces;
using ProductService.Proto;
using ProductService.Services;

namespace ProductService.Processors;

// =============================================================================
// PaymentProcessor — scoped processor for orders.payment-confirmed topic
// =============================================================================
// LIFETIME: Scoped (InstancePerLifetimeScope), keyed to "orders.payment-confirmed"
//
// Message key   = order ID
// Message value = Protobuf PaymentConfirmed (payment_reference)
//
// Calls OrderDetailsAggregator to set PaymentReference on the OrderDetails
// for this order and republish. The OrderDetails Status advances to
// PaymentConfirmed (unless the order is paused or already warehouse-picked).
// =============================================================================
public class PaymentProcessor(OrderDetailsAggregator aggregator, ILogger<PaymentProcessor> logger)
    : IMessageProcessor
{
    public async Task ProcessAsync(ConsumeResult<string, byte[]> message, CancellationToken cancellationToken)
    {
        var orderId = message.Message.Key;
        var bytes = message.Message.Value;

        if (string.IsNullOrEmpty(orderId) || bytes is null || bytes.Length == 0)
        {
            logger.LogWarning("Skipping payment message with missing key or value (orderId={OrderId})", orderId);
            return;
        }

        string paymentReference;
        try
        {
            paymentReference = PaymentConfirmed.Parser.ParseFrom(bytes).ToDomain();
        }
        catch (InvalidProtocolBufferException ex)
        {
            logger.LogError(ex, "Skipping malformed protobuf on orders.payment-confirmed (orderId={OrderId}, offset={Offset})", orderId, message.TopicPartitionOffset);
            return;
        }

        logger.LogInformation("Payment confirmed: orderId={OrderId}, paymentReference={PaymentReference}", orderId, paymentReference);

        await aggregator.AddPaymentReferenceAsync(orderId, paymentReference, cancellationToken);
    }
}
