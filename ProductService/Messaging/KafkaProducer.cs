using Confluent.Kafka;

namespace ProductService.Messaging;

// =============================================================================
// KafkaProducer — singleton wrapper around Confluent.Kafka's IProducer
// =============================================================================
// LIFETIME: Singleton
//
// This is a thin wrapper around IProducer<string, string>. It's singleton because:
//   1. The underlying IProducer is thread-safe and designed to be shared.
//      It maintains internal buffers and a background thread for batching
//      and sending messages to Kafka brokers.
//   2. Creating producers is expensive (broker connections, buffer allocation).
//   3. This wrapper has no per-message state — it just forwards calls.
//
// INJECTED INTO: Both singletons (ProductDetailsService) and scoped services
// (processors, via ProductDetailsService). Since it's a singleton, the same
// instance is used everywhere, which is exactly what we want.
//
// DISPOSAL: Implements IDisposable. When the app shuts down, the DI container
// disposes singletons. Flush() ensures any buffered messages are sent to Kafka
// before the producer is destroyed.
// =============================================================================
public class KafkaProducer : IDisposable
{
    private readonly IProducer<string, string> _producer;

    public KafkaProducer(IProducer<string, string> producer)
    {
        _producer = producer;
    }

    // cancellationToken defaults to default so existing call sites that don't have a
    // token (e.g. in tests or other contexts) continue to compile without changes.
    // When provided, it is forwarded to Confluent's ProduceAsync — if the token fires
    // (per-message deadline or shutdown), the produce operation is cancelled promptly
    // rather than waiting for a broker that may be unreachable.
    public async Task ProduceAsync(string topic, string key, string value,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await _producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = key,
                Value = value
            }, cancellationToken);
        }
        catch (ProduceException<string, string> ex)
        {
            throw new Exception($"Failed to produce message to {topic}: [{ex.Error.Code}] {ex.Error.Reason}", ex);
        }
    }

    public void Dispose()
    {
        // Flush ensures any messages still in the internal buffer are sent
        // to Kafka before we dispose the producer. Without this, messages
        // could be lost on shutdown.
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
