using Confluent.Kafka;

namespace KafkaInfrastructure.Messaging;

// =============================================================================
// KafkaProducer — singleton wrapper around Confluent.Kafka's IProducer
// =============================================================================
// LIFETIME: Singleton
//
// Thin wrapper around IProducer<string, byte[]>. Thread-safe, shared across
// all scopes. Flushes buffered messages on dispose to prevent message loss.
// =============================================================================
public class KafkaProducer : IDisposable
{
    private readonly IProducer<string, byte[]> _producer;

    public KafkaProducer(IProducer<string, byte[]> producer)
    {
        _producer = producer;
    }

    public async Task ProduceAsync(string topic, string key, byte[]? value,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await _producer.ProduceAsync(topic, new Message<string, byte[]>
            {
                Key = key,
                Value = value!
            }, cancellationToken);
        }
        catch (ProduceException<string, byte[]> ex)
        {
            throw new Exception($"Failed to produce message to {topic}: [{ex.Error.Code}] {ex.Error.Reason}", ex);
        }
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
