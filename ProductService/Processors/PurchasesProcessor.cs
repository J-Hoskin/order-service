using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using ProductService.Interfaces;
using ProductService.Services;

namespace ProductService.Processors;

// =============================================================================
// PurchasesProcessor — scoped processor for products.purchases topic messages
// =============================================================================
// LIFETIME: Scoped (InstancePerLifetimeScope), keyed to "products.purchases"
//
// Same scoping pattern as SalesProcessor — a new instance per message, resolved
// from the child scope that MessageRouter creates.
//
// SHARING STATE WITH SalesProcessor VIA SINGLETON:
//   Both SalesProcessor and PurchasesProcessor inject the SAME singleton
//   ProductDetailsService. When a sale comes in, SalesProcessor adds to the
//   sales customers list. When a purchase comes in (possibly seconds or hours
//   later, in a completely different scope), PurchasesProcessor adds to the
//   purchases customers list. The ProductDetailsService singleton bridges
//   these two independent scoped lifecycles.
//
//   Scope A (sale message):
//     SalesProcessor → ProductDetailsService.AddSaleAsync("prod-1", "cust-1")
//                       ↓ updates shared state, publishes combined message
//
//   Scope B (purchase message, different scope, different time):
//     PurchasesProcessor → ProductDetailsService.AddPurchaseAsync("prod-1", "cust-2")
//                          ↓ updates same shared state, publishes combined message
//
//   Both scopes A and B are interacting with the exact same ProductDetailsService
//   object in memory. Scope A's processor is long gone (disposed) by the time
//   scope B runs, but the singleton state persists.
// =============================================================================
public class PurchasesProcessor : IMessageProcessor
{
    private readonly ProductDetailsAggregator _productDetails;
    private readonly ProductStore _productStores;
    private readonly ILogger<PurchasesProcessor> _logger;

    public PurchasesProcessor(
        ProductDetailsAggregator productDetails,
        ProductStore productStores,
        ILogger<PurchasesProcessor> logger)
    {
        _productDetails = productDetails;
        _productStores = productStores;
        _logger = logger;
    }

    public async Task ProcessAsync(ConsumeResult<string, string> message, CancellationToken cancellationToken)
    {
        var productId = message.Message.Key;
        var customerId = message.Message.Value;

        if (string.IsNullOrEmpty(productId) || string.IsNullOrEmpty(customerId))
        {
            _logger.LogWarning("Skipping purchase message with missing key or value (productId={ProductId}, customerId={CustomerId})", productId, customerId);
            return;
        }

        _logger.LogInformation("Purchase: product {ProductId}, customer {CustomerId}", productId, customerId);

        // Fetch the latest cached product snapshot here, at the call site — same
        // reasoning as SalesProcessor. ProductDetailsService must not depend on
        // ProductManager to avoid a circular dependency.
        var product = _productStores.Get(productId);

        // Same singleton ProductDetailsService that SalesProcessor uses.
        // This call updates the purchases customer list for this product and
        // publishes an updated combined message to products.details.
        await _productDetails.AddPurchaseAsync(productId, customerId, product, cancellationToken);
    }
}
