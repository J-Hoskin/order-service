using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using ProductService.Messaging;
using ProductService.Models;

namespace ProductService.Services;

// =============================================================================
// ProductDetailsService — singleton that aggregates and publishes product state
// =============================================================================
// LIFETIME: Singleton
//
// This service is the bridge between the two scoped processors (SalesProcessor
// and PurchasesProcessor). It maintains a ConcurrentDictionary of ProductDetails
// records — one per product ID — and publishes an updated combined message to
// the products.details Kafka topic whenever either side adds a customer.
//
// WHY SINGLETON:
//   This service holds in-memory state (the _state dictionary) that must:
//     1. Persist across messages — a sale at time T=0 and a purchase at T=10
//        must both contribute to the same ProductDetails record.
//     2. Be shared between processors — SalesProcessor (in scope A) and
//        PurchasesProcessor (in scope B) must write to the same dictionary.
//   If this were scoped, each message would get a fresh empty dictionary.
//   The state would be lost when the scope ends.
//
// THREAD SAFETY VIA IMMUTABILITY:
//   ProductDetails is an immutable record with ImmutableList properties. We
//   never modify an existing instance. Instead, we use ConcurrentDictionary's
//   AddOrUpdate to atomically swap in a NEW record with the updated data.
//
//   Old approach (mutable, needed locks):
//     var details = _state.GetOrAdd(id, ...);
//     lock (details) { details.SalesCustomers.Add(customerId); }
//
//   New approach (immutable, no locks):
//     _state.AddOrUpdate(id,
//         addValueFactory: _ => ProductDetails.Empty(id) with { ... },
//         updateValueFactory: (_, existing) => existing with { ... }
//     );
//
//   AddOrUpdate is atomic — ConcurrentDictionary guarantees that only one
//   thread at a time can update the value for a given key. The "with"
//   expression creates a brand new record, and AddOrUpdate swaps it in.
//   The old record is never touched and is eventually garbage collected.
//   No locks needed anywhere.
//
// HOW IT'S INJECTED INTO SCOPED PROCESSORS:
//   When Autofac creates a SalesProcessor inside a child scope, it resolves
//   ProductDetailsService as a constructor dependency. Since it's registered
//   as SingleInstance, Autofac returns the one app-wide instance — it doesn't
//   create a new one in the child scope. This is the allowed direction:
//     Scoped service → depends on → Singleton service  ✓
//   The reverse would be illegal:
//     Singleton service → depends on → Scoped service   ✗ (captive dependency)
// =============================================================================
public class ProductDetailsAggregator
{
    // Per-product state. Key = product ID, Value = immutable ProductDetails record.
    // ConcurrentDictionary handles concurrent AddOrUpdate atomically per key.
    private readonly ConcurrentDictionary<string, ProductDetails> _state = new();
    private readonly KafkaProducer _producer;
    private readonly ILogger<ProductDetailsAggregator> _logger;

    // All dependencies are singletons — safe to inject into a singleton.
    public ProductDetailsAggregator(
        KafkaProducer producer,
        ILogger<ProductDetailsAggregator> logger)
    {
        _producer = producer;
        _logger = logger;
    }

    /// <summary>
    /// Called by SalesProcessor when a sale message is consumed.
    /// Creates a new immutable ProductDetails with the customer added to
    /// the sales list, atomically swaps it into the dictionary, and publishes.
    /// </summary>
    public async Task AddSaleAsync(string productId, string customerId, Product? product,
        CancellationToken cancellationToken = default)
    {
        // AddOrUpdate atomically either:
        //   - Creates a new ProductDetails if this product ID doesn't exist yet
        //     (addValueFactory)
        //   - Creates a new ProductDetails from the existing one with the customer
        //     added to SalesCustomers (updateValueFactory)
        //
        // The "with" expression does NOT mutate "existing" — it creates a new
        // record. ImmutableList.Add() also returns a new list, leaving the
        // original unchanged. The old record in the dictionary is replaced
        // atomically by the new one.
        var updated = _state.AddOrUpdate(
            productId,
            addValueFactory: _ => ProductDetails.Empty(productId) with // with creates copy of object, niceee
            {
                Product = product,
                SalesCustomers = ImmutableList.Create(customerId)
            },
            updateValueFactory: (_, existing) =>
            {
                if (existing.SalesCustomers.Contains(customerId) && existing.Product == product)
                    return existing; // no change needed, return same reference

                return existing with
                {
                    Product = product,
                    SalesCustomers = existing.SalesCustomers.Contains(customerId)
                        ? existing.SalesCustomers
                        : existing.SalesCustomers.Add(customerId)
                };
            }
        );

        await PublishAsync(updated, cancellationToken);
    }

    /// <summary>
    /// Called by PurchasesProcessor when a purchase message is consumed.
    /// Same immutable pattern as AddSaleAsync, but for the purchases list.
    /// </summary>
    public async Task AddPurchaseAsync(string productId, string customerId, Product? product,
        CancellationToken cancellationToken = default)
    {
        var updated = _state.AddOrUpdate(
            productId,
            addValueFactory: _ => ProductDetails.Empty(productId) with
            {
                Product = product,
                PurchasesCustomers = ImmutableList.Create(customerId)
            },
            updateValueFactory: (_, existing) =>
            {
                if (existing.PurchasesCustomers.Contains(customerId) && existing.Product == product)
                    return existing;

                return existing with
                {
                    Product = product,
                    PurchasesCustomers = existing.PurchasesCustomers.Contains(customerId)
                        ? existing.PurchasesCustomers
                        : existing.PurchasesCustomers.Add(customerId)
                };
            }
        );

        await PublishAsync(updated, cancellationToken);
    }

    /// <summary>
    /// Called by ProductManager when a products.updates message is consumed.
    /// Updates the Product field in the existing ProductDetails for this product
    /// and republishes. If no ProductDetails exists yet (no sales or purchases
    /// have arrived), creates one with just the product info.
    /// </summary>
    public async Task UpdateProductAsync(string productId, Product product,
        CancellationToken cancellationToken = default)
    {
        var updated = _state.AddOrUpdate(
            productId,
            addValueFactory: _ => ProductDetails.Empty(productId) with
            {
                Product = product
            },
            updateValueFactory: (_, existing) =>
            {
                if (existing.Product == product)
                    return existing; // no change

                return existing with { Product = product };
            }
        );

        await PublishAsync(updated, cancellationToken);
    }

    /// <summary>
    /// Serializes and publishes a ProductDetails to the products.details topic.
    ///
    /// No locking needed — the ProductDetails record is immutable. It was fully
    /// constructed before being passed here, and no other thread can modify it.
    /// JsonSerializer reads its properties safely without any risk of concurrent
    /// mutation.
    /// </summary>
    private async Task PublishAsync(ProductDetails details, CancellationToken cancellationToken = default)
    {
        var value = JsonSerializer.Serialize(details);

        _logger.LogInformation("Publishing product details for {ProductId}", details.ProductId);
        await _producer.ProduceAsync("products.details", details.ProductId, value, cancellationToken);
    }
}
