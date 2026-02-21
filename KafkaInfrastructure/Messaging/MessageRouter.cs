using Autofac;
using Confluent.Kafka;
using KafkaInfrastructure.Interfaces;
using Microsoft.Extensions.Logging;

namespace KafkaInfrastructure.Messaging;

// =============================================================================
// MessageRouter — singleton dispatcher that creates a DI scope per message
// =============================================================================
// LIFETIME: Singleton
//
// Creates a new Autofac child scope for each incoming message, resolves the
// keyed IMessageProcessor matching the topic, delegates processing, then
// disposes the scope. Mirrors ASP.NET's request-scoped DI without an HTTP host.
// =============================================================================
public class MessageRouter
{
    private readonly ILifetimeScope _scope;
    private readonly ILogger<MessageRouter> _logger;

    public MessageRouter(ILifetimeScope scope, ILogger<MessageRouter> logger)
    {
        _scope = scope;
        _logger = logger;
    }

    public async Task RouteAsync(ConsumeResult<string, byte[]> message, CancellationToken cancellationToken)
    {
        var processorKey = message.Topic;

        await using var childScope = _scope.BeginLifetimeScope();

        if (!childScope.TryResolveKeyed<IMessageProcessor>(processorKey, out var processor))
        {
            _logger.LogWarning("No processor registered for key '{Key}'", processorKey);
            return;
        }

        await processor.ProcessAsync(message, cancellationToken);
    }
}
