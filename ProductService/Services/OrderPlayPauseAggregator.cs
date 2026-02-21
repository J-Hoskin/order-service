using System.Text.Json;
using Microsoft.Extensions.Logging;
using ProductService.Messaging;
using ProductService.Models;

namespace ProductService.Services;

// =============================================================================
// OrderPlayPauseAggregator — singleton that coordinates pause and resume logic
// =============================================================================
// LIFETIME: Singleton
//
// Owns all pause/resume concerns for an order:
//   - Validates that the order exists in OrderStore before acting
//   - Delegates the actual state mutation and republish to OrderDetailsAggregator
//   - Publishes an OrderAlert to orders.alerts on successful pause confirmation
//   - Publishes a tombstone to orders.pause on resume so that any stale
//     orders.confirm-pause arriving after the resume is rejected by guard 2
//
// PAUSE TIMESTAMP VALIDATION:
//   Pause timestamps are read from the orders.pause GlobalTable rather than
//   held locally. This is the correct HA design: all instances consume the
//   full orders.pause topic on startup (via a dedicated consumer group), so
//   every instance has a complete view of all pause timestamps at all times.
//   When an instance fails mid-pause and Kafka rebalances to a surviving
//   instance, that instance already has the pause timestamp in its GlobalTable —
//   no state is lost and confirm-pause validation continues to work correctly.
//
// AT-LEAST-ONCE ALERT DELIVERY:
//   ConfirmPauseAsync produces the OrderAlert to orders.alerts before returning.
//   The caller (KafkaConsumerWorker, via MessageRouter) only commits the
//   orders.confirm-pause offset AFTER ProcessAsync returns. This means:
//     - If this instance dies before ProduceAsync completes, the offset is
//       never committed, and the next instance reprocesses the message,
//       re-validates, and re-sends the alert.
//     - If this instance dies after ProduceAsync but before the offset is
//       committed, the same reprocessing occurs — the alert may be sent
//       twice (at-least-once). Consumers of orders.alerts must be idempotent.
//
// DEPENDENCY DIRECTION:
//   OrderPlayPauseAggregator → OrderDetailsAggregator  ✓ (singleton → singleton)
//   OrderPlayPauseAggregator → OrderStore              ✓ (singleton → singleton)
//   OrderPlayPauseAggregator → KafkaGlobalTable        ✓ (singleton → singleton)
//   OrderPlayPauseAggregator → KafkaProducer           ✓ (singleton → singleton)
//   OrderStore → OrderDetailsAggregator                ✓
//   OrderDetailsAggregator has no dependency on either  ✓
// =============================================================================
public class OrderPlayPauseAggregator(
    OrderDetailsAggregator orderDetailsAggregator,
    OrderStore orderStore,
    KafkaGlobalTable<string, string> pauseGlobalTable,
    KafkaProducer kafkaProducer,
    ILogger<OrderPlayPauseAggregator> logger)
{
    private const string AlertsTopic = "orders.alerts";

    /// <summary>
    /// Handles orders.pause. Sets PauseRequested = true on the order.
    /// Status only becomes Paused once orders.confirm-pause also arrives.
    /// </summary>
    public async Task PauseAsync(string orderId, DateTime createdAt, CancellationToken cancellationToken = default)
    {
        if (!orderStore.Exists(orderId))
        {
            logger.LogWarning("Skipping orders.pause for unknown order {OrderId} — no orders.created received", orderId);
            return;
        }

        logger.LogInformation("Pause requested for order {OrderId}", orderId);
        await orderDetailsAggregator.SetPauseRequestedAsync(orderId, cancellationToken);
    }

    /// <summary>
    /// Handles orders.confirm-pause. Validates both timestamps, sets the order
    /// to Paused, then publishes an OrderAlert to orders.alerts.
    ///
    /// The alert is produced inside this method so that the caller's offset
    /// commit (in KafkaConsumerWorker) only happens after both the state
    /// mutation and the alert production have completed. This gives at-least-once
    /// delivery of the alert across instance failures and Kafka rebalances.
    /// </summary>
    public async Task ConfirmPauseAsync(string orderId, DateTime createdAt, CancellationToken cancellationToken = default)
    {
        if (!orderStore.Exists(orderId))
        {
            logger.LogWarning("Skipping orders.confirm-pause for unknown order {OrderId} — no orders.created received", orderId);
            return;
        }

        // Guard 1: confirm-pause must be recent (within 10 seconds of now).
        // Safe under Kafka rebalance because rebalance typically completes in
        // 2-3 seconds, well within the 10-second window.
        if (DateTime.UtcNow - createdAt > TimeSpan.FromSeconds(10))
        {
            logger.LogWarning("Skipping orders.confirm-pause for {OrderId} — message is stale", orderId);
            return;
        }

        // Guard 2: pause and confirm-pause timestamps must be within 10 seconds
        // of each other. The pause timestamp is read from the GlobalTable (not a
        // local dict) so it survives instance failure — see class comment above.
        var pauseJson = pauseGlobalTable.Get(orderId);
        var pausePayload = pauseJson is null ? null : JsonSerializer.Deserialize<PauseOrderPayload>(pauseJson);
        if (pausePayload is null || Math.Abs((createdAt - pausePayload.CreatedAt).TotalSeconds) > 10)
        {
            logger.LogWarning("Skipping orders.confirm-pause for {OrderId} — timestamps too far apart or no pause found", orderId);
            return;
        }

        logger.LogInformation("Pause confirmed for order {OrderId}", orderId);
        await orderDetailsAggregator.SetPauseConfirmedAsync(orderId, cancellationToken);

        // Publish the alert BEFORE returning to the caller. The consumer worker
        // commits the orders.confirm-pause offset only after this method returns,
        // so a crash between SetPauseConfirmedAsync and here will cause the
        // message to be reprocessed and the alert to be re-sent on the next instance.
        var alert = new OrderAlert(orderId, "OrderPaused", DateTime.UtcNow);
        var alertJson = JsonSerializer.Serialize(alert);
        await kafkaProducer.ProduceAsync(AlertsTopic, orderId, alertJson, cancellationToken);
        logger.LogInformation("OrderPaused alert sent for order {OrderId}", orderId);
    }

    /// <summary>
    /// Handles orders.resume. Clears both pause flags regardless of their current state.
    /// Status reverts to the fulfillment-derived status (Placed, PaymentConfirmed, WarehousePicked).
    /// </summary>
    public async Task ResumeAsync(string orderId, CancellationToken cancellationToken = default)
    {
        if (!orderStore.Exists(orderId))
        {
            logger.LogWarning("Skipping orders.resume for unknown order {OrderId} — no orders.created received", orderId);
            return;
        }

        logger.LogInformation("Order resumed: {OrderId}", orderId);
        await orderDetailsAggregator.ResumeOrderAsync(orderId, cancellationToken);

        // Tombstone the orders.pause GlobalTable entry so that any stale
        // orders.confirm-pause arriving after this resume is rejected by guard 2
        // (pauseGlobalTable.Get returns null → "no pause found").
        await kafkaProducer.ProduceAsync("orders.pause", orderId, null, cancellationToken);
        logger.LogInformation("Tombstoned orders.pause entry for resumed order {OrderId}", orderId);
    }
}
