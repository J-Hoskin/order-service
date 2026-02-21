using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using ProductService.Extensions;
using ProductService.Interfaces;
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
public class OrderCreatedProcessor(OrderStore orderStore, ILogger<OrderCreatedProcessor> logger)
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

        var payload = OrderCreated.Parser.ParseFrom(bytes).ToDomain();
        await orderStore.UpdateAsync(orderId, payload, cancellationToken);
    }
}
