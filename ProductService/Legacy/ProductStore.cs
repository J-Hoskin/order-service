using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using ProductService.Models;

namespace ProductService.Services;

// =============================================================================
// ProductManager — singleton that caches product models from products.updates
// =============================================================================
// LIFETIME: Singleton
//
// Maintains an in-memory cache of Product records, updated whenever a message
// arrives on the products.updates topic. The flow is:
//
//   KafkaConsumerWorker → MessageRouter → ProductUpdatesProcessor (scoped)
//                                                ↓
//                                        ProductManager.UpdateAsync() (singleton)
//                                                ↓
//                                        ProductDetailsService.UpdateProductAsync()
//                                                ↓
//                                        publishes to products.details
//
// WHY SINGLETON:
//   The product cache must persist across messages. A scoped ProductManager
//   would lose its cache when the scope ends after each message. The cache
//   is also read by ProductDetailsService (also a singleton) when building
//   ProductDetails for sales/purchases — both singletons share the same
//   app-wide lifetime.
//
// WHY NOT A HOSTED SERVICE:
//   ProductManager doesn't need its own consume loop. It receives messages
//   through the existing KafkaConsumerWorker → MessageRouter → scoped
//   ProductUpdatesProcessor pipeline. The processor is the scoped entry point
//   that delegates to this singleton. This avoids duplicating consume loop
//   logic and keeps all message consumption in one place.
//
// THREAD SAFETY:
//   Uses ConcurrentDictionary with immutable Product records. Updates are
//   atomic — the old Product is replaced entirely, never mutated.
// =============================================================================
public class ProductStore
{
    private readonly ConcurrentDictionary<string, Product> _cache = new();
    private readonly ProductDetailsAggregator _productDetailsAggregator;
    private readonly ILogger<ProductStore> _logger;

    // Both dependencies are singletons — safe to inject into a singleton.
    public ProductStore(ProductDetailsAggregator productDetailsAggregator, ILogger<ProductStore> logger)
    {
        _productDetailsAggregator = productDetailsAggregator;
        _logger = logger;
    }

    /// <summary>
    /// Called by ProductUpdatesProcessor when a products.updates message arrives.
    /// Deserializes the product, caches it, and tells ProductDetailsService to
    /// republish the combined ProductDetails with the updated product info.
    /// </summary>
    public async Task UpdateAsync(string productId, string value,
        CancellationToken cancellationToken = default)
    {
        var product = JsonSerializer.Deserialize<Product>(value);

        if (product is null)
        {
            _logger.LogWarning("Failed to deserialize product for {ProductId}", productId);
            return;
        }

        // Atomically replace the cached product. The old Product record is
        // never mutated — it's simply replaced by the new one.
        _cache[productId] = product;

        _logger.LogInformation("Cached product {ProductId}: {Name}", productId, product.Name);

        // Tell ProductDetailsService to update the product field in the
        // ProductDetails for this product ID and republish to products.details.
        await _productDetailsAggregator.UpdateProductAsync(productId, product, cancellationToken);
    }

    /// <summary>
    /// Look up a cached product by ID. Used by ProductDetailsService when
    /// building ProductDetails for sales/purchases events. Returns null if
    /// no product update has been received yet for this ID.
    /// </summary>
    public Product? Get(string productId) => _cache.GetValueOrDefault(productId);
}
