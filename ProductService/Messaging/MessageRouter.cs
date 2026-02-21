using Autofac;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using ProductService.Interfaces;

namespace ProductService.Messaging;

// =============================================================================
// MessageRouter — singleton dispatcher that creates a DI scope per message
// =============================================================================
// This is the core of the scope-per-message pattern. The router sits between
// the KafkaConsumerWorker (which owns the consume loop) and the processors
// (which handle business logic). Its job is to:
//   1. Create a new DI child scope for each message
//   2. Resolve the correct keyed IMessageProcessor from that scope
//   3. Call ProcessAsync on it
//   4. Dispose the scope when done
//
// LIFETIME: Singleton
//   The router itself has no per-message state. It's a pure dispatcher that
//   can safely handle every message from the same instance.
//
// WHY IT'S NOT A HOSTED SERVICE:
//   The router has no background loop or lifecycle to manage. It doesn't
//   consume from Kafka — it just gets called by KafkaConsumerWorker.RouteAsync()
//   is a regular method call, not a long-running task. Hosted services are for
//   things that need to run independently in the background.
//
// =============================================================================
// SCOPE FLOW — what happens when RouteAsync is called:
// =============================================================================
//
//   KafkaConsumerWorker calls: await _router.RouteAsync(consumeResult)
//
//   Step 1: _scope.BeginLifetimeScope()
//     Creates a new child scope from the root Autofac container. This child
//     scope is an isolated DI container that will track all scoped services
//     resolved within it.
//
//   Step 2: childScope.TryResolveKeyed<IMessageProcessor>(topic, out processor)
//     Autofac looks up the keyed registration matching the topic string:
//       "products.sales"     → creates a new SalesProcessor
//       "products.purchases" → creates a new PurchasesProcessor
//     The processor is created INSIDE the child scope. Autofac also resolves
//     its constructor dependencies:
//       - ProductDetailsService → singleton, same instance for all scopes
//       - ILogger<T>           → typically singleton in default config
//     If the processor had scoped dependencies (e.g., DbContext), those would
//     also be created inside this child scope and shared within it.
//
//   Step 3: processor.ProcessAsync(message)
//     The processor does its work — updates ProductDetailsService, produces
//     output messages, etc.
//
//   Step 4: "await using" disposes the child scope
//     When the method exits (whether normally or via exception), the child
//     scope is disposed. This means:
//       - The SalesProcessor/PurchasesProcessor instance is disposed (if IDisposable)
//       - Any scoped dependencies resolved in that scope are disposed
//       - Singleton dependencies (ProductDetailsService, KafkaProducer) are NOT
//         disposed — they belong to the root scope and live until app shutdown
//     This cleanup is automatic thanks to "await using". Even if ProcessAsync
//     throws an exception, the scope is still properly disposed.
//
// =============================================================================
// WHY NOT INJECT IMessageProcessor DIRECTLY?
// =============================================================================
//   If MessageRouter took IMessageProcessor as a constructor parameter, it would
//   be resolved once when the singleton router is created, from the root scope.
//   Problems:
//     1. Which implementation? There are multiple keyed registrations.
//     2. The processor would live as long as the router (forever), so any scoped
//        dependencies it has would also live forever — this is called a "captive
//        dependency" and defeats the purpose of scoping.
//     3. The same processor instance would handle every message, so any per-message
//        state would leak between messages.
//   By injecting ILifetimeScope and creating child scopes manually, we get fresh
//   processors with proper scoping for every message.
// =============================================================================
public class MessageRouter
{
    // ILifetimeScope is Autofac's equivalent of IServiceScopeFactory.
    // When injected into a singleton, this is the ROOT scope (the app-level container).
    // We call BeginLifetimeScope() on it to create child scopes.
    private readonly ILifetimeScope _scope;
    private readonly ILogger<MessageRouter> _logger;

    public MessageRouter(ILifetimeScope scope, ILogger<MessageRouter> logger)
    {
        _scope = scope;
        _logger = logger;
    }

    // cancellationToken carries both the per-message deadline and the shutdown signal
    // (it is a linked token created in KafkaConsumerWorker). Forwarding it to the
    // processor ensures any awaitable inside the processor chain can be cancelled
    // promptly if either fires.
    public async Task RouteAsync(ConsumeResult<string, string> message, CancellationToken cancellationToken)
    {
        // The message's topic determines which processor to resolve.
        // "products.sales" → SalesProcessor, "products.purchases" → PurchasesProcessor
        var processorKey = message.Topic;

        // Create a child scope for this message. "await using" ensures the scope
        // (and all scoped services resolved from it) is disposed when we're done,
        // even if an exception is thrown.
        await using var childScope = _scope.BeginLifetimeScope();

        // Resolve the keyed processor from the child scope. TryResolveKeyed
        // returns false if no processor is registered for this topic, rather
        // than throwing an exception.
        if (!childScope.TryResolveKeyed<IMessageProcessor>(processorKey, out var processor))
        {
            _logger.LogWarning("No processor registered for key '{Key}'", processorKey);
            return;
        }

        // Delegate to the processor. The processor is scoped to this message —
        // after this call returns, the "await using" above disposes the scope
        // and cleans up the processor and its scoped dependencies.
        await processor.ProcessAsync(message, cancellationToken);
    }
}
