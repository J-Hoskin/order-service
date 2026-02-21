using Confluent.Kafka;

namespace ProductService.Interfaces;

// =============================================================================
// IMessageProcessor — the contract for all Kafka message handlers
// =============================================================================
// Each implementation handles messages from a specific topic. Implementations
// are registered as SCOPED (InstancePerLifetimeScope) and KEYED by topic name
// in Autofac. The MessageRouter creates a child scope per message and resolves
// the correct implementation using the topic as the key.
//
// Because processors are scoped:
//   - A new instance is created for every Kafka message
//   - Any scoped dependencies (e.g., DbContext) are also fresh per message
//   - When processing completes, the scope is disposed, cleaning up everything
//   - Singleton dependencies (KafkaProducer, ProductDetailsService) are shared
//     across all scopes — they are NOT created fresh per message
public interface IMessageProcessor
{
    // cancellationToken is cancelled if the per-message deadline fires or the host is
    // shutting down. Implementations must forward it to all async operations so the
    // processor can be aborted promptly in either case.
    Task ProcessAsync(ConsumeResult<string, byte[]> message, CancellationToken cancellationToken);
}
