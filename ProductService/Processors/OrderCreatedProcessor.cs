using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using ProductService.Extensions;
using ProductService.Interfaces;
using ProductService.Messaging;
using ProductService.Proto;
using ProductService.Services;

namespace ProductService.Processors;

// =============================================================================
// OrderCreatedProcessor — scoped processor for orders.created topic messages
// =============================================================================
// LIFETIME: Scoped (InstancePerLifetimeScope), keyed to "orders.created"
//
// Thin scoped entry point that delegates to the singleton OrderStore.
// OrderStore caches the base order payload and triggers the initial publish
// of OrderDetails to orders.details.
//
// WHY NOT MAKE OrderStore A PROCESSOR:
//   IMessageProcessor implementations are scoped — created and destroyed per
//   message. OrderStore must be a singleton to hold its cache across messages.
//   We split them: a scoped processor as the entry point, singleton for state.
// =============================================================================
public class OrderCreatedProcessor(OrderStore orderStore, KafkaProducer kafkaProducer, ILogger<OrderCreatedProcessor> logger)
    : IMessageProcessor
{
    public async Task ProcessAsync(ConsumeResult<string, byte[]> message, CancellationToken cancellationToken)
    {
        var orderId = message.Message.Key;
        var bytes = message.Message.Value;

        if (string.IsNullOrEmpty(orderId) || bytes is null || bytes.Length == 0)
        {
            logger.LogWarning("Skipping orders.created message with missing key or value (orderId={OrderId})", orderId);
            return;
        }

        logger.LogInformation("Order created: {OrderId}", orderId);

        OrderCreated proto;
        try
        {
            proto = OrderCreated.Parser.ParseFrom(bytes);
        }
        catch (InvalidProtocolBufferException ex)
        {
            logger.LogError(ex, "Skipping malformed protobuf on orders.created (orderId={OrderId}, offset={Offset})", orderId, message.TopicPartitionOffset);
            return;
        }

        var payload = proto.ToDomain();
        await orderStore.UpdateAsync(orderId, payload, cancellationToken);

        if (payload.Items.Count > 5)
        {
            await kafkaProducer.ProduceAsync("orders.whales", orderId, bytes, cancellationToken);
        }
    }
}
