using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using ProductService.Interfaces;
using ProductService.Services;

namespace ProductService.Processors;

// =============================================================================
// SalesProcessor — scoped processor for products.sales topic messages
// =============================================================================
// LIFETIME: Scoped (InstancePerLifetimeScope), keyed to "products.sales"
//
// A NEW instance of this class is created for EVERY Kafka message from the
// products.sales topic. Here's the full lifecycle:
//
//   1. A message arrives on "products.sales"
//   2. KafkaConsumerWorker calls _router.RouteAsync(message)
//   3. MessageRouter creates a child scope: _scope.BeginLifetimeScope()
//   4. MessageRouter resolves: childScope.ResolveKeyed<IMessageProcessor>("products.sales")
//   5. Autofac sees that "products.sales" maps to SalesProcessor
//   6. Autofac creates a NEW SalesProcessor instance, injecting:
//        - ProductDetailsService → the app-wide singleton (shared state)
//        - ILogger<SalesProcessor> → singleton logger instance
//   7. MessageRouter calls processor.ProcessAsync(message)
//   8. When RouteAsync returns, "await using" disposes the child scope
//   9. This SalesProcessor instance is garbage collected (it's no longer referenced)
//
// DEPENDENCY LIFETIMES:
//   - ProductDetailsService (singleton): The SAME instance is injected into every
//     SalesProcessor and every PurchasesProcessor across all scopes. This is how
//     both processors can contribute to the same shared product state. The
//     singleton outlives every scope — it's never disposed until app shutdown.
//   - ILogger (singleton): Loggers are singletons by default in .NET. Safe to
//     inject into scoped services.
//
// WHY SCOPED AND NOT SINGLETON:
//   If SalesProcessor were a singleton, we'd need to worry about:
//     - Thread safety if messages are ever processed concurrently
//     - Any scoped dependencies (e.g., DbContext) being captured and reused
//       incorrectly across messages
//     - No clean disposal boundary between messages
//   As a scoped service, each message gets a clean, isolated instance. Even
//   though this processor currently has no scoped dependencies, the pattern
//   is correct by default and ready for when we add them (e.g., a DbContext
//   for persisting sales records).
// =============================================================================
public class SalesProcessor : IMessageProcessor
{
    private readonly ProductDetailsAggregator _productDetails;
    private readonly ProductStore _productStores;
    private readonly ILogger<SalesProcessor> _logger;

    public SalesProcessor(
        ProductDetailsAggregator productDetails,
        ProductStore productStores,
        ILogger<SalesProcessor> logger)
    {
        _productDetails = productDetails;
        _productStores = productStores;
        _logger = logger;
    }

    public async Task ProcessAsync(ConsumeResult<string, byte[]> message, CancellationToken cancellationToken)
    {
        // message.Message.Key = product ID
        // message.Message.Value = customer ID who made the sale
        var productId = message.Message.Key;
        var customerId = message.Message.Value is { Length: > 0 } b ? System.Text.Encoding.UTF8.GetString(b) : null;

        if (string.IsNullOrEmpty(productId) || string.IsNullOrEmpty(customerId))
        {
            _logger.LogWarning("Skipping sale message with missing key or value (productId={ProductId}, customerId={CustomerId})", productId, customerId);
            return;
        }

        _logger.LogInformation("Sale: product {ProductId}, customer {CustomerId}", productId, customerId);

        // Fetch the latest cached product snapshot here, at the call site, rather than
        // inside ProductDetailsService. This keeps ProductDetailsService free of any
        // dependency on ProductManager, breaking the circular dependency that would
        // otherwise exist (ProductManager → ProductDetailsService → ProductManager).
        var product = _productStores.Get(productId);

        // Call into the singleton ProductDetailsService to record this sale.
        // ProductDetailsService is a singleton, so this updates the shared
        // in-memory state that persists across all messages/scopes.
        // It will also publish an updated combined product message to
        // the products.details topic with both sales and purchases customers.
        await _productDetails.AddSaleAsync(productId, customerId, product, cancellationToken);
    }
}
