using System.Collections.Immutable;

namespace ProductService.Models;

// =============================================================================
// OrderDetails — immutable aggregate built from multiple async event streams
// =============================================================================
// This record represents the combined state of an order, progressively enriched
// as events arrive from independent downstream systems:
//
//   orders.created          → sets OrderId, CustomerId, Items
//   orders.payment-confirmed → fills PaymentReference (null until confirmed)
//   orders.warehouse-picked  → fills PickedByWarehouseStaff (null until picked)
//   orders.pause             → sets PauseRequested = true
//   orders.confirm-pause     → sets PauseConfirmed = true → Status becomes Paused
//   orders.resume            → clears both pause flags → Status reverts
//
// STATUS DERIVATION:
//   Status is not stored by the producer — it is derived inside
//   OrderDetailsAggregator each time a new event arrives and set before
//   publishing. Consumers of orders.details can read Status directly without
//   inspecting the individual nullable fields.
//
// IMMUTABILITY:
//   All properties are init-only. Updates produce a new record via "with"
//   expressions. ImmutableList<OrderItem> ensures the items collection is
//   also immutable. No locks are required — see OrderDetailsAggregator for
//   the ConcurrentDictionary.AddOrUpdate pattern.
// =============================================================================
public record OrderDetails(
    string OrderId,
    string? CustomerId,
    OrderStatus Status,
    ImmutableList<OrderItem> Items,
    string? PaymentReference,           // null until orders.payment-confirmed arrives
    string? PickedByWarehouseStaff,     // null until orders.warehouse-picked arrives
    bool PauseRequested,                // true after orders.pause received
    bool PauseConfirmed)                // true after orders.confirm-pause received
{
    public static OrderDetails Empty(string orderId) =>
        new(orderId, null, OrderStatus.Placed, ImmutableList<OrderItem>.Empty,
            null, null, false, false);
}
