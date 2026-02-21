using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using ProductService.Interfaces;
using ProductService.Services;

namespace ProductService.Processors;

// =============================================================================
// ProductUpdatesProcessor — scoped processor for products.updates topic
// =============================================================================
// LIFETIME: Scoped (InstancePerLifetimeScope), keyed to "products.updates"
//
// Thin scoped wrapper that delegates to the singleton ProductManager.
// This follows the same pattern as SalesProcessor → ProductDetailsService:
//   - The processor is scoped (new instance per message, disposed after)
//   - The service it calls is singleton (holds long-lived cache state)
//
// WHY NOT JUST MAKE ProductManager A PROCESSOR:
//   IMessageProcessor implementations are scoped — created and destroyed per
//   message. ProductManager needs to be a singleton to hold the product cache
//   across messages. We can't register the same class as both scoped (processor)
//   and singleton (cache owner). So we split them: a scoped processor as the
//   entry point, delegating to a singleton for the real work.
// =============================================================================
public class ProductUpdatesProcessor(ProductStore productStores, ILogger<ProductUpdatesProcessor> logger)
    : IMessageProcessor
{
    private readonly ProductStore _productStores = productStores;
    private readonly ILogger<ProductUpdatesProcessor> _logger = logger;

    public async Task ProcessAsync(ConsumeResult<string, string> message, CancellationToken cancellationToken)
    {
        var productId = message.Message.Key;
        var payload = message.Message.Value;

        if (string.IsNullOrEmpty(productId) || string.IsNullOrEmpty(payload))
        {
            _logger.LogWarning("Skipping product update message with missing key or value (productId={ProductId})", productId);
            return;
        }

        _logger.LogInformation("Product update received for {ProductId}", productId);

        // Delegate to the singleton ProductManager, which caches the product
        // and triggers a republish of the combined ProductDetails.
        await _productStores.UpdateAsync(productId, payload, cancellationToken);
    }
}
