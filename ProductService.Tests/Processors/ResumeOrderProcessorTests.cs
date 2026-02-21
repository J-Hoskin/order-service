using System.Collections.Immutable;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using ProductService.Messaging;
using ProductService.Models;
using ProductService.Processors;
using ProductService.Services;
using ProductService.Tests.Helpers;
using Xunit;

namespace ProductService.Tests.Processors;

public class ResumeOrderProcessorTests
{
    private readonly Mock<IProducer<string, string>> _producerMock;
    private readonly OrderStore _store;
    private readonly ResumeOrderProcessor _sut;

    public ResumeOrderProcessorTests()
    {
        _producerMock = new Mock<IProducer<string, string>>();
        _producerMock
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, string>());

        var kafkaProducer = new KafkaProducer(_producerMock.Object);
        var aggregator = new OrderDetailsAggregator(kafkaProducer, NullLogger<OrderDetailsAggregator>.Instance);
        _store = new OrderStore(aggregator, NullLogger<OrderStore>.Instance);

        var mockConsumer = new Mock<IConsumer<string, byte[]>>();
        mockConsumer.Setup(c => c.Consume(It.IsAny<CancellationToken>())).Throws(new OperationCanceledException());
        var globalTable = new KafkaGlobalTable<string, byte[]>(
            mockConsumer.Object, "orders.pause", NullLogger<KafkaGlobalTable<string, byte[]>>.Instance);

        var playPause = new OrderPlayPauseAggregator(
            aggregator, _store, globalTable, kafkaProducer,
            NullLogger<OrderPlayPauseAggregator>.Instance);

        _sut = new ResumeOrderProcessor(playPause, NullLogger<ResumeOrderProcessor>.Instance);
    }

    [Fact]
    public async Task NullKey_DoesNotPublish()
    {
        var msg = MessageFactory.Build("orders.resume", null, Array.Empty<byte>());
        await _sut.ProcessAsync(msg, CancellationToken.None);
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task UnknownOrder_DoesNotPublish()
    {
        var msg = MessageFactory.Build("orders.resume", "ghost-order", Array.Empty<byte>());
        await _sut.ProcessAsync(msg, CancellationToken.None);
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task HappyPath_PublishesToOrdersDetailsAndTombstonesOrdersPause()
    {
        await SeedOrderAsync("order-D");

        var msg = MessageFactory.Build("orders.resume", "order-D", Array.Empty<byte>());
        await _sut.ProcessAsync(msg, CancellationToken.None);

        // Expects: orders.details publish + tombstone to orders.pause
        _producerMock.Verify(
            p => p.ProduceAsync(
                "orders.details",
                It.Is<Message<string, string>>(m => m.Key == "order-D"),
                It.IsAny<CancellationToken>()),
            Times.Once);

        _producerMock.Verify(
            p => p.ProduceAsync(
                "orders.pause",
                It.Is<Message<string, string>>(m => m.Key == "order-D" && m.Value == null),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    private async Task SeedOrderAsync(string orderId)
    {
        var payload = new OrderCreatedPayload("cust-seed", ImmutableList<OrderItem>.Empty);
        await _store.UpdateAsync(orderId, payload, CancellationToken.None);
        _producerMock.Invocations.Clear();
    }
}
