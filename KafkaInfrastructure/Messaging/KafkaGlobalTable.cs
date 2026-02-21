using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaInfrastructure.Messaging;

// =============================================================================
// KafkaGlobalTable — singleton hosted service for in-memory state from Kafka
// =============================================================================
// LIFETIME: Singleton (registered as both .AsSelf() and .As<IHostedService>())
//
// Continuously consumes a compacted Kafka topic and maintains an in-memory
// key-value lookup. Modeled after Kafka Streams' GlobalKTable concept.
// Has its own dedicated IConsumer with an independent consumer group so it
// tracks offsets separately from the main consume loop.
// =============================================================================
public class KafkaGlobalTable<TKey, TValue> : BackgroundService
    where TKey : notnull
{
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

        return Task.Run(() =>
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = _consumer.Consume(stoppingToken);

                    if (result.Message.Value is null)
                        _state.TryRemove(result.Message.Key, out _);
                    else
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

    public TValue? Get(TKey key) => _state.GetValueOrDefault(key);

    public bool ContainsKey(TKey key) => _state.ContainsKey(key);
}
