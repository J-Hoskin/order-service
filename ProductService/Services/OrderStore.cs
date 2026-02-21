using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using ProductService.Models;

namespace ProductService.Services;

// =============================================================================
// OrderStore — singleton cache of base Order data from orders.created
// =============================================================================
// LIFETIME: Singleton
//
// Caches the base order payload (customer ID and items) keyed by order ID.
// When orders.created arrives, OrderCreatedProcessor calls UpdateAsync here,
// which caches the data and tells OrderDetailsAggregator to set the base
// fields on the OrderDetails and republish.
//
// The payment and warehouse processors do not need this store — they only
// append a single scalar field (payment reference or picker name) directly
// to OrderDetails without needing to look up base order data.
// =============================================================================
public class OrderStore
{
    private readonly ConcurrentDictionary<string, OrderCreatedPayload> _cache = new();
    private readonly OrderDetailsAggregator _orderDetailsAggregator;
    private readonly ILogger<OrderStore> _logger;

    public OrderStore(OrderDetailsAggregator orderDetailsAggregator, ILogger<OrderStore> logger)
    {
        _orderDetailsAggregator = orderDetailsAggregator;
        _logger = logger;
    }

    /// <summary>
    /// Called by OrderCreatedProcessor when orders.created arrives.
    /// Caches the payload and triggers a publish of OrderDetails.
    /// </summary>
    public async Task UpdateAsync(string orderId, OrderCreatedPayload payload,
        CancellationToken cancellationToken = default)
    {
        _cache[orderId] = payload;
        _logger.LogInformation("Cached order {OrderId} for customer {CustomerId}", orderId, payload.CustomerId);

        await _orderDetailsAggregator.UpdateOrderAsync(orderId, payload.CustomerId, payload.Items, cancellationToken);
    }

    /// <summary>
    /// Returns true if an orders.created message has been received for this order ID.
    /// Used by pause/resume processors to guard against messages for unknown orders.
    /// </summary>
    public bool Exists(string orderId) => _cache.ContainsKey(orderId);
}

