using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ProductService.Messaging;

// =============================================================================
// KafkaGlobalTable — singleton hosted service for in-memory state from Kafka
// =============================================================================
// LIFETIME: Singleton (registered as both .AsSelf() and .As<IHostedService>())
//
// This is modeled after Kafka Streams' GlobalKTable concept. It continuously
// consumes a compacted Kafka topic and maintains an in-memory key-value lookup
// table. Other services inject it and call Get(key) to look up values.
//
// WHY BackgroundService (IHostedService):
//   Unlike the MessageRouter (which is just a method that gets called), the
//   global table needs its OWN independent consume loop running in the
//   background. It's self-contained — it starts consuming on app startup,
//   continuously updates its in-memory state, and stops on shutdown. This
//   lifecycle management is exactly what IHostedService provides.
//
// WHY SINGLETON:
//   The table holds long-lived in-memory state that other services read from.
//   If it were scoped, each scope would get an empty table with no data.
//
// DUAL REGISTRATION:
//   - .AsSelf() → so services can inject KafkaGlobalTable<string, string>
//     and call .Get(key) to look up values
//   - .As<IHostedService>() → so the .NET host starts the consume loop
//   Both resolve to the SAME singleton instance.
//
// OWN CONSUMER:
//   This table has its own dedicated IConsumer, separate from the main consumer
//   in KafkaConsumerWorker. Kafka consumers are single-threaded — one consumer
//   can't be shared between two loops. The global table's consumer has its own
//   consumer group ID so it independently tracks its own offsets.
//
// COMPACTED TOPICS:
//   Global tables are designed for compacted topics where Kafka retains only
//   the latest value per key. A null value is a tombstone (key deletion).
//   On startup, the table consumes from the beginning to rebuild full state.
// =============================================================================
public class KafkaGlobalTable<TKey, TValue> : BackgroundService
    where TKey : notnull
{
    // ConcurrentDictionary because the consume loop writes to it while other
    // services (processors) may read from it concurrently via Get().
    private readonly ConcurrentDictionary<TKey, TValue> _state = new();
    private readonly IConsumer<TKey, TValue> _consumer;
    private readonly string _topic;
    private readonly ILogger<KafkaGlobalTable<TKey, TValue>> _logger;

    public KafkaGlobalTable(
        IConsumer<TKey, TValue> consumer,
        string topic,
        ILogger<KafkaGlobalTable<TKey, TValue>> logger)
    {
        _consumer = consumer;
        _topic = topic;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_topic);

        // Task.Run moves the blocking consume loop off the startup thread.
        // BackgroundService.StartAsync needs to return promptly so other
        // hosted services can start. If we blocked here with a while loop,
        // the app would hang during startup.
        return Task.Run(() =>
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // Blocking call — waits for the next message from the topic.
                    var result = _consumer.Consume(stoppingToken);

                    if (result.Message.Value is null)
                        // Tombstone — the key has been deleted from the compacted topic.
                        _state.TryRemove(result.Message.Key, out _);
                    else
                        // Upsert — add or update the key with the latest value.
                        _state[result.Message.Key] = result.Message.Value;
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "GlobalTable consume error on {Topic}", _topic);
                }
            }

            _consumer.Close();
        }, stoppingToken);
    }

    /// <summary>
    /// Look up a value by key. Called by processors and other services.
    /// Thread-safe — ConcurrentDictionary handles concurrent reads while
    /// the consume loop writes.
    /// </summary>
    public TValue? Get(TKey key) => _state.GetValueOrDefault(key);

    public bool ContainsKey(TKey key) => _state.ContainsKey(key);
}
