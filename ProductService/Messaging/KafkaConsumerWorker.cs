using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ProductService.Messaging;

// =============================================================================
// KafkaConsumerWorker — hosted BackgroundService that runs the consume loop
// =============================================================================
// This is the entry point for all incoming Kafka messages. It extends
// BackgroundService, which is an abstract class implementing IHostedService.
// The .NET host calls StartAsync on app startup, which kicks off ExecuteAsync
// as a long-running background task.
//
// LIFETIME: Singleton (registered as IHostedService)
//   There's one consume loop for this consumer group member. The worker holds
//   references to other singletons (MessageRouter, IConsumer).
//
// WHY BackgroundService INSTEAD OF IHostedService:
//   BackgroundService is a convenience base class for services that need a
//   continuous loop. It handles:
//     - Running ExecuteAsync on a background thread
//     - Cancelling the CancellationToken on shutdown
//     - Awaiting graceful completion
//   If we used raw IHostedService, we'd have to manage the Task and
//   CancellationTokenSource ourselves in StartAsync/StopAsync.
//
// SEPARATION OF CONCERNS:
//   This worker ONLY owns the consume loop. It does not know about processors,
//   scopes, or business logic. It delegates every message to the MessageRouter,
//   which handles scope creation and processor resolution. This means we could
//   change the consumption strategy (e.g., batch consumption, parallel
//   processing) without touching routing or processing logic.
//
// MANUAL OFFSET COMMIT — AT-LEAST-ONCE DELIVERY:
//   The main consumer is configured with EnableAutoCommit = false (see Program.cs).
//   This worker calls consumer.Commit(result) AFTER RouteAsync returns, which
//   means the offset is only saved to Kafka once all processing — including any
//   Kafka produces (e.g. the OrderPaused alert) — has fully completed.
//
//   Why this matters: if the instance dies mid-processing (after consuming but
//   before committing), Kafka has no record that the message was handled. When
//   the partition is rebalanced to a surviving instance, it re-reads from the
//   last committed offset and reprocesses the message — re-running all side
//   effects (state mutations, alert production). This is at-least-once delivery.
//
//   The tradeoff: if the instance dies AFTER RouteAsync returns but BEFORE
//   Commit() completes, the message is reprocessed and side effects run twice.
//   Downstream consumers of orders.alerts must therefore be idempotent.
//
//   If RouteAsync throws, Commit() is skipped and the offset is not advanced —
//   the message will be reprocessed. This is intentional: a processing error
//   does not silently skip the message.
// =============================================================================
public class KafkaConsumerWorker : BackgroundService
{
    private readonly MessageRouter _router;
    private readonly IConsumer<string, string> _consumer;
    private readonly ILogger<KafkaConsumerWorker> _logger;

    // All dependencies are singletons — safe to inject into a singleton worker.
    // MessageRouter is singleton (dispatches via child scopes internally).
    // IConsumer is singleton (one Kafka consumer per consumer group member).
    public KafkaConsumerWorker(
        MessageRouter router,
        IConsumer<string, string> consumer,
        ILogger<KafkaConsumerWorker> logger)
    {
        _router = router;
        _consumer = consumer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Subscribe to the topics that have registered processors.
        // These topic names must match the keyed registrations in Program.cs.
        _consumer.Subscribe(new[]
        {
            "orders.created",
            "orders.payment-confirmed",
            "orders.warehouse-picked",
            "orders.pause",
            "orders.confirm-pause",
            "orders.resume"
        });

        // Main consume loop — runs until the host signals shutdown via the token.
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Consume() blocks until a message is available or the token is cancelled.
                // This is a synchronous call — Confluent.Kafka's consumer is not async.
                var result = _consumer.Consume(stoppingToken);

                // Create a per-message timeout linked to the shutdown token.
                // If processing exceeds 30 seconds, cts fires and cancels any awaitable
                // in the processor chain (including ProduceAsync to Kafka). The message
                // is skipped and the loop continues to the next one.
                // If the host is shutting down, stoppingToken fires which also fires cts
                // — the OperationCanceledException catch below breaks the loop cleanly.
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                cts.CancelAfter(TimeSpan.FromSeconds(30));

                // Hand the message to the router. The router will:
                //   1. Create a new child DI scope
                //   2. Resolve the correct keyed IMessageProcessor
                //   3. Call ProcessAsync on it (including any Kafka produces)
                //   4. Dispose the scope (cleaning up scoped services)
                // All of that happens inside RouteAsync — the worker doesn't know or care.
                await _router.RouteAsync(result, cts.Token);

                // Commit the offset only after full processing completes.
                // This is what makes delivery at-least-once: Kafka only records
                // our progress here, so a crash before this line causes the next
                // instance to reprocess the message from scratch.
                // See the class comment above for the full at-least-once explanation.
                _consumer.Commit(result);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // Thrown by Consume() or RouteAsync when stoppingToken is cancelled.
                // Expected on shutdown — exit the loop cleanly without logging an error.
                break;
            }
            catch (ConsumeException ex)
            {
                // Kafka-level errors (deserialization failures, broker errors, etc).
                // Log and continue — one bad message shouldn't kill the consume loop.
                _logger.LogError(ex, "Consume error");
            }
            catch (Exception ex)
            {
                // Unexpected errors from RouteAsync or processors, including
                // OperationCanceledException from a per-message timeout expiry
                // (stoppingToken is NOT cancelled in that case, so the when guard
                // above does not match and it falls through to here).
                _logger.LogError(ex, "Unhandled error processing message");
            }
        }

        // Cleanly close the consumer on shutdown. This commits final offsets
        // and leaves the consumer group, triggering a rebalance so other
        // consumers in the group can pick up this member's partitions.
        _consumer.Close();
    }
}
