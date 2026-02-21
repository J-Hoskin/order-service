using Autofac;
using Confluent.Kafka;
using KafkaInfrastructure.Configuration;
using KafkaInfrastructure.Messaging;
using Microsoft.Extensions.Hosting;

namespace KafkaInfrastructure.Registration;

// =============================================================================
// KafkaInfrastructureModule — Autofac module for one-call infrastructure setup
// =============================================================================
// Registers the main Kafka consumer, producer, KafkaProducer wrapper,
// MessageRouter, and KafkaConsumerWorker. Microservices call this once instead
// of copy-pasting the infrastructure registrations.
//
// Usage:
//   container.RegisterModule(new KafkaInfrastructureModule(new KafkaOptions
//   {
//       BootstrapServers = "localhost:9092",
//       ConsumerGroupId  = "my-service",
//       Topics           = ["topic.a", "topic.b"]
//   }));
// =============================================================================
public class KafkaInfrastructureModule : Module
{
    private readonly KafkaOptions _options;

    public KafkaInfrastructureModule(KafkaOptions options) => _options = options;

    protected override void Load(ContainerBuilder builder)
    {
        builder.RegisterInstance(_options);

        builder.Register(_ => new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.ConsumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        }).SetValueDeserializer(Deserializers.ByteArray).Build())
        .As<IConsumer<string, byte[]>>()
        .SingleInstance();

        builder.Register(_ => new ProducerBuilder<string, byte[]>(new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers
        }).SetValueSerializer(Serializers.ByteArray).Build())
        .As<IProducer<string, byte[]>>()
        .SingleInstance();

        builder.RegisterType<KafkaProducer>().SingleInstance();
        builder.RegisterType<MessageRouter>().SingleInstance();

        builder.RegisterType<KafkaConsumerWorker>()
            .As<IHostedService>()
            .SingleInstance();
    }
}
