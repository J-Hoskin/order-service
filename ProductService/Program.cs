using Autofac;
using Autofac.Extensions.DependencyInjection;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ProductService;
using ProductService.Interfaces;
using ProductService.Messaging;
using ProductService.Processors;
using ProductService.Services;
using ProductService.Models;

// =============================================================================
// PROGRAM.CS — Application composition root
// =============================================================================
// This is where we wire up the entire dependency injection container using Autofac.
// Every service's LIFETIME (singleton, scoped, transient) is declared here, and
// understanding why each lifetime was chosen is critical to understanding how
// scoping works throughout the application.
//
// LIFETIME RECAP:
//   - SingleInstance (Singleton): One object for the entire app. Lives until shutdown.
//   - InstancePerLifetimeScope (Scoped): One object per DI scope. In our case,
//     we create one scope per Kafka message, so scoped = one per message.
//   - InstancePerDependency (Transient): New object every time it's resolved.
//
// WHY AUTOFAC?
//   Autofac replaces the built-in .NET DI container. We call
//   UseServiceProviderFactory(new AutofacServiceProviderFactory()) to tell the
//   host to use Autofac's ContainerBuilder instead of IServiceCollection.
//   Autofac gives us keyed services (Keyed<T>), which let us register multiple
//   implementations of IMessageProcessor distinguished by a string key (the topic name).
//   .NET 8+ has built-in keyed services, but Autofac also provides modules,
//   decorators, and advanced lifetime control if needed later.
// =============================================================================

var builder = Host.CreateDefaultBuilder(args);

// Replace the default .NET DI container with Autofac.
builder.UseServiceProviderFactory(new AutofacServiceProviderFactory());

builder.ConfigureContainer<ContainerBuilder>(container =>
{
    // =========================================================================
    // KAFKA CLIENTS — all singletons
    // =========================================================================
    // Kafka consumers and producers are thread-safe and expensive to create
    // (they establish broker connections, manage internal buffers, etc).
    // We create them once and reuse them for the entire app lifetime.

    // Main consumer — used by KafkaConsumerWorker to poll messages from Kafka.
    // Registered as SingleInstance because there should only be one consumer
    // per consumer group member. The consume loop in KafkaConsumerWorker calls
    // _consumer.Consume() sequentially, so there's no concurrent access issue.
    //
    // EnableAutoCommit = false: offsets are committed manually in KafkaConsumerWorker
    // AFTER each message is fully processed (including any Kafka produces such as
    // the OrderPaused alert). This gives at-least-once delivery — if the instance
    // dies mid-processing, the next instance reprocesses from the last committed
    // offset and re-runs all side effects (state mutations, alert production).
    // See KafkaConsumerWorker for where consumer.Commit(result) is called.
    container.Register(_ => new ConsumerBuilder<string, byte[]>(new ConsumerConfig
    {
        BootstrapServers = "localhost:9092",
        GroupId = "my-service",
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false
    }).SetValueDeserializer(Deserializers.ByteArray).Build())
    .As<IConsumer<string, byte[]>>()
    .SingleInstance();

    // Producer — used by KafkaProducer wrapper to send messages to Kafka.
    // SingleInstance because IProducer<TKey, TValue> from Confluent.Kafka is
    // designed to be shared. It batches and sends messages internally.
    container.Register(_ => new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = "localhost:9092"
    }).Build())
    .As<IProducer<string, string>>()
    .SingleInstance();

    // KafkaProducer — our thin wrapper around IProducer. Singleton because it
    // only holds a reference to the singleton IProducer. It has no per-message
    // state — it just forwards ProduceAsync calls. Safe to share across scopes.
    container.RegisterType<KafkaProducer>().SingleInstance();

    // =========================================================================
    // ORDER SERVICES — singletons
    // =========================================================================
    // OrderDetailsAggregator accumulates state across all six order topics.
    // It holds a ConcurrentDictionary<string, OrderDetails> keyed by order ID
    // and republishes to orders.details on every update.
    //
    // OrderStore caches the base order payload from orders.created and
    // delegates to OrderDetailsAggregator to set the initial order fields.
    //
    // Both must be singletons — state must persist across messages and scopes.
    container.RegisterType<OrderDetailsAggregator>().SingleInstance();
    container.RegisterType<OrderStore>().SingleInstance();
    container.Register(ctx => new OrderPlayPauseAggregator(
        ctx.Resolve<OrderDetailsAggregator>(),
        ctx.Resolve<OrderStore>(),
        ctx.ResolveKeyed<KafkaGlobalTable<string, byte[]>>("orders.pause"),
        ctx.Resolve<KafkaProducer>(),
        ctx.Resolve<ILogger<OrderPlayPauseAggregator>>()
    )).SingleInstance();

    // =========================================================================
    // MESSAGE PROCESSORS — scoped, keyed by topic name
    // =========================================================================
    // Processors are registered as InstancePerLifetimeScope (scoped) with a
    // key that matches their Kafka topic. The MessageRouter creates a new child
    // scope for each incoming message, then uses the topic name to resolve the
    // correct processor from that scope.
    //
    // WHY SCOPED (InstancePerLifetimeScope):
    //   1. Isolation — each message gets its own processor instance. If a
    //      processor had any per-message state (e.g., a DbContext for database
    //      operations), it wouldn't leak between messages.
    //   2. Disposal — when the scope ends (after the message is processed),
    //      the processor and all its scoped dependencies are disposed. This
    //      prevents resource leaks (open DB connections, uncommitted transactions).
    //   3. Safety — if processing fails, the scope is disposed and any partial
    //      state is discarded cleanly. No half-processed state lingers.
    //
    // WHY KEYED:
    //   Instead of a switch/if-else in the router to pick a processor, we
    //   register each processor with a string key. The router just does:
    //     scope.ResolveKeyed<IMessageProcessor>(message.Topic)
    //   This is extensible — adding a new topic/processor is just one more
    //   registration here, no router code changes needed.
    //
    // HOW SCOPED PROCESSORS USE SINGLETON SERVICES:
    //   When Autofac creates a SalesProcessor inside a child scope, it sees
    //   that SalesProcessor depends on ProductDetailsService. Since
    //   ProductDetailsService is a singleton, Autofac returns the single
    //   app-wide instance. The processor is scoped (created and destroyed per
    //   message), but the ProductDetailsService it calls into lives forever.
    //   This is the correct pattern — short-lived workers calling into
    //   long-lived shared state.

    container.RegisterType<OrderCreatedProcessor>()
        .Keyed<IMessageProcessor>("orders.created")
        .InstancePerLifetimeScope();

    container.RegisterType<PaymentProcessor>()
        .Keyed<IMessageProcessor>("orders.payment-confirmed")
        .InstancePerLifetimeScope();

    container.RegisterType<WarehouseProcessor>()
        .Keyed<IMessageProcessor>("orders.warehouse-picked")
        .InstancePerLifetimeScope();

    container.RegisterType<PauseOrderProcessor>()
        .Keyed<IMessageProcessor>("orders.pause")
        .InstancePerLifetimeScope();

    container.RegisterType<ConfirmPauseProcessor>()
        .Keyed<IMessageProcessor>("orders.confirm-pause")
        .InstancePerLifetimeScope();

    container.RegisterType<ResumeOrderProcessor>()
        .Keyed<IMessageProcessor>("orders.resume")
        .InstancePerLifetimeScope();

    // =========================================================================
    // MESSAGE ROUTER — singleton
    // =========================================================================
    // The router is a singleton that lives for the entire app. It receives
    // Autofac's ILifetimeScope (the root scope) via constructor injection.
    //
    // WHY SINGLETON: The router has no per-message state. It's a pure
    // dispatching mechanism — receive message, create child scope, resolve
    // processor, call ProcessAsync, dispose scope. The same router instance
    // handles every message.
    //
    // CRITICAL SCOPE PATTERN: The router does NOT directly depend on
    // IMessageProcessor. If it did, the processor would be resolved from the
    // root scope (singleton lifetime), defeating the purpose of scoping.
    // Instead, the router depends on ILifetimeScope and creates a NEW child
    // scope per message. This child scope is where the processor (and any
    // scoped dependencies it has) gets resolved and lives.
    //
    // SCOPE LIFECYCLE PER MESSAGE:
    //   1. Router receives a ConsumeResult from the worker
    //   2. Router calls _scope.BeginLifetimeScope() → creates child scope
    //   3. Router calls childScope.ResolveKeyed<IMessageProcessor>(topic)
    //      → Autofac creates a new SalesProcessor (or PurchasesProcessor)
    //        inside this child scope, injecting singleton dependencies
    //   4. Router calls processor.ProcessAsync(message)
    //   5. "await using" disposes the child scope → processor is disposed
    //      along with any other scoped services resolved in that scope
    container.RegisterType<MessageRouter>().SingleInstance();

    // =========================================================================
    // KAFKA GLOBAL TABLE — singleton + hosted service
    // =========================================================================
    // A KafkaGlobalTable continuously consumes a compacted Kafka topic to
    // maintain an in-memory key-value lookup. It's like a read-only cache
    // that stays in sync with Kafka.
    //
    // WHY SINGLETON: The table holds long-lived in-memory state (the
    // ConcurrentDictionary). Other services (like processors) inject it to
    // do lookups. The data must persist for the entire app lifetime.
    //
    // WHY HOSTED SERVICE (BackgroundService): It needs its own independent
    // consume loop running in the background to continuously update its state.
    // Registering as IHostedService tells the .NET host to call StartAsync on
    // app startup, which kicks off the consume loop via ExecuteAsync.
    //
    // DUAL REGISTRATION (.AsSelf() + .As<IHostedService>()):
    //   - .AsSelf() lets other services inject KafkaGlobalTable<string, string>
    //     directly to call .Get(key) for lookups.
    //   - .As<IHostedService>() tells the host to manage its lifecycle (start
    //     the consume loop on startup, stop it on shutdown).
    //   Both registrations point to the SAME singleton instance.
    //
    // SEPARATE CONSUMER: This table gets its own IConsumer (created in the
    // factory lambda below), separate from the main consumer used by
    // KafkaConsumerWorker. Each Kafka consumer can only be used by one thread,
    // and this table needs its own independent consume loop.
    container.Register(ctx =>
    {
        var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "my-service-global-customers",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        }).Build();

        var logger = ctx.Resolve<ILogger<KafkaGlobalTable<string, string>>>();
        return new KafkaGlobalTable<string, string>(consumer, "customers", logger);
    })
    .AsSelf()
    .As<IHostedService>()
    .SingleInstance();

    // GlobalTable for orders.pause — all instances consume all partitions so
    // pause timestamps survive instance failure and Kafka rebalance.
    // Consumer group: my-service-global-pause (independent from main consumer).
    container.Register(ctx =>
    {
        var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "my-service-global-pause",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        }).SetValueDeserializer(Deserializers.ByteArray).Build();

        var logger = ctx.Resolve<ILogger<KafkaGlobalTable<string, byte[]>>>();
        return new KafkaGlobalTable<string, byte[]>(consumer, "orders.pause", logger);
    })
    .Keyed<KafkaGlobalTable<string, byte[]>>("orders.pause")
    .As<IHostedService>()
    .SingleInstance();

    // =========================================================================
    // CONSUMER WORKER — singleton + hosted service
    // =========================================================================
    // The main Kafka consume loop. This is a BackgroundService that polls
    // Kafka for messages and delegates each one to the MessageRouter.
    //
    // WHY SINGLETON: There's one consume loop for this consumer group member.
    // The worker holds references to the singleton consumer and singleton router.
    //
    // WHY HOSTED SERVICE: Like the global table, it needs to run a long-lived
    // background loop. The host starts it on app startup and stops it on shutdown.
    //
    // NOTE: This is NOT registered .AsSelf() because nothing else injects it.
    // It only needs .As<IHostedService>() so the host knows to start it.
    container.RegisterType<KafkaConsumerWorker>()
        .As<IHostedService>()
        .SingleInstance();
});

var host = builder.Build();
await host.RunAsync();
