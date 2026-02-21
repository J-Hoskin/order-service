using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using ProductService.Messaging;
using ProductService.Models;

namespace ProductService.Services;

// =============================================================================
// OrderDetailsAggregator — singleton that aggregates and publishes order state
// =============================================================================
// LIFETIME: Singleton
//
// Maintains a ConcurrentDictionary<string, OrderDetails> keyed by order ID.
// Each incoming event from any of the six order topics updates a field on the
// OrderDetails for that order and triggers a republish to orders.details.
//
// THREAD SAFETY VIA IMMUTABILITY:
//   OrderDetails is an immutable record. Updates use ConcurrentDictionary's
//   AddOrUpdate to atomically swap in a new record instance. The "with"
//   expression creates a copy — the old record is never mutated. No locks needed.
//
// STATUS DERIVATION:
//   OrderStatus is not stored externally. DeriveStatus() computes it from the
//   current field values each time a new record is built, and it is set on the
//   record before publishing. This keeps status consistent automatically.
// =============================================================================
public class OrderDetailsAggregator
{
    private readonly ConcurrentDictionary<string, OrderDetails> _state = new();
    private readonly KafkaProducer _producer;
    private readonly ILogger<OrderDetailsAggregator> _logger;

    public OrderDetailsAggregator(KafkaProducer producer, ILogger<OrderDetailsAggregator> logger)
    {
        _producer = producer;
        _logger = logger;
    }

    /// <summary>
    /// Called by OrderCreatedProcessor. Sets the base order fields.
    /// </summary>
    public async Task UpdateOrderAsync(string orderId, string? customerId, ImmutableList<OrderItem> items,
        CancellationToken cancellationToken = default)
    {
        var updated = _state.AddOrUpdate(
            orderId,
            addValueFactory: _ => OrderDetails.Empty(orderId) with
            {
                CustomerId = customerId,
                Items = items
            },
            updateValueFactory: (_, existing) => existing with
            {
                CustomerId = customerId,
                Items = items,
                Status = DeriveStatus(existing)
            }
        );

        updated = updated with { Status = DeriveStatus(updated) };
        _state[orderId] = updated;

        await PublishAsync(updated, cancellationToken);
    }

    /// <summary>
    /// Called by PaymentProcessor when orders.payment-confirmed arrives.
    /// </summary>
    public async Task AddPaymentReferenceAsync(string orderId, string paymentReference,
        CancellationToken cancellationToken = default)
    {
        var updated = _state.AddOrUpdate(
            orderId,
            addValueFactory: _ =>
            {
                var d = OrderDetails.Empty(orderId) with { PaymentReference = paymentReference };
                return d with { Status = DeriveStatus(d) };
            },
            updateValueFactory: (_, existing) =>
            {
                var d = existing with { PaymentReference = paymentReference };
                return d with { Status = DeriveStatus(d) };
            }
        );

        await PublishAsync(updated, cancellationToken);
    }

    /// <summary>
    /// Called by WarehouseProcessor when orders.warehouse-picked arrives.
    /// </summary>
    public async Task AddWarehousePickAsync(string orderId, string pickedBy,
        CancellationToken cancellationToken = default)
    {
        var updated = _state.AddOrUpdate(
            orderId,
            addValueFactory: _ =>
            {
                var d = OrderDetails.Empty(orderId) with { PickedByWarehouseStaff = pickedBy };
                return d with { Status = DeriveStatus(d) };
            },
            updateValueFactory: (_, existing) =>
            {
                var d = existing with { PickedByWarehouseStaff = pickedBy };
                return d with { Status = DeriveStatus(d) };
            }
        );

        await PublishAsync(updated, cancellationToken);
    }

    /// <summary>
    /// Called by PauseOrderProcessor when orders.pause arrives.
    /// Sets PauseRequested = true. Status only becomes Paused once
    /// PauseConfirmed is also true.
    /// </summary>
    public async Task SetPauseRequestedAsync(string orderId,
        CancellationToken cancellationToken = default)
    {
        var updated = _state.AddOrUpdate(
            orderId,
            addValueFactory: _ =>
            {
                var d = OrderDetails.Empty(orderId) with { PauseRequested = true };
                return d with { Status = DeriveStatus(d) };
            },
            updateValueFactory: (_, existing) =>
            {
                var d = existing with { PauseRequested = true };
                return d with { Status = DeriveStatus(d) };
            }
        );

        await PublishAsync(updated, cancellationToken);
    }

    /// <summary>
    /// Called by ConfirmPauseProcessor when orders.confirm-pause arrives.
    /// Sets PauseConfirmed = true. If PauseRequested is already true, Status
    /// becomes Paused.
    /// </summary>
    public async Task SetPauseConfirmedAsync(string orderId,
        CancellationToken cancellationToken = default)
    {
        var updated = _state.AddOrUpdate(
            orderId,
            addValueFactory: _ =>
            {
                var d = OrderDetails.Empty(orderId) with { PauseConfirmed = true };
                return d with { Status = DeriveStatus(d) };
            },
            updateValueFactory: (_, existing) =>
            {
                var d = existing with { PauseConfirmed = true };
                return d with { Status = DeriveStatus(d) };
            }
        );

        await PublishAsync(updated, cancellationToken);
    }

    /// <summary>
    /// Called by ResumeOrderProcessor when orders.resume arrives.
    /// Clears both pause flags regardless of their current state.
    /// Status reverts to whatever DeriveStatus returns without the pause flags.
    /// </summary>
    public async Task ResumeOrderAsync(string orderId,
        CancellationToken cancellationToken = default)
    {
        var updated = _state.AddOrUpdate(
            orderId,
            addValueFactory: _ => OrderDetails.Empty(orderId),
            updateValueFactory: (_, existing) =>
            {
                var d = existing with { PauseRequested = false, PauseConfirmed = false };
                return d with { Status = DeriveStatus(d) };
            }
        );

        await PublishAsync(updated, cancellationToken);
    }

    /// <summary>
    /// Derives the current OrderStatus from the fields of an OrderDetails record.
    /// Pause takes priority over fulfillment stage — an order is Paused if and
    /// only if both pause messages have been received.
    /// </summary>
    private static OrderStatus DeriveStatus(OrderDetails d) => d switch
    {
        { PauseRequested: true, PauseConfirmed: true } => OrderStatus.Paused,
        { PickedByWarehouseStaff: not null }            => OrderStatus.WarehousePicked,
        { PaymentReference: not null }                  => OrderStatus.PaymentConfirmed,
        _                                               => OrderStatus.Placed
    };

    private async Task PublishAsync(OrderDetails details, CancellationToken cancellationToken = default)
    {
        var value = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(details));
        _logger.LogInformation("Publishing order details for {OrderId} (Status={Status})", details.OrderId, details.Status);
        await _producer.ProduceAsync("orders.details", details.OrderId, value, cancellationToken);
    }
}
