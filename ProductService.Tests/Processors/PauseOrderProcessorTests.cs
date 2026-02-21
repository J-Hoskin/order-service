using System.Collections.Immutable;
using Confluent.Kafka;
using WellKnownTimestamp = Google.Protobuf.WellKnownTypes.Timestamp;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using ProductService.Messaging;
using ProductService.Models;
using ProductService.Processors;
using ProductService.Proto;
using ProductService.Services;
using ProductService.Tests.Helpers;
using Xunit;

namespace ProductService.Tests.Processors;

public class PauseOrderProcessorTests
{
    private readonly Mock<IProducer<string, byte[]>> _producerMock;
    private readonly OrderStore _store;
    private readonly PauseOrderProcessor _sut;

    public PauseOrderProcessorTests()
    {
        _producerMock = new Mock<IProducer<string, byte[]>>();
        _producerMock
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, byte[]>());

        var kafkaProducer = new KafkaProducer(_producerMock.Object);
        var aggregator = new OrderDetailsAggregator(kafkaProducer, NullLogger<OrderDetailsAggregator>.Instance);
        _store = new OrderStore(aggregator, NullLogger<OrderStore>.Instance);

        var globalTable = BuildEmptyGlobalTable();
        var playPause = new OrderPlayPauseAggregator(
            aggregator, _store, globalTable, kafkaProducer,
            NullLogger<OrderPlayPauseAggregator>.Instance);

        _sut = new PauseOrderProcessor(playPause, NullLogger<PauseOrderProcessor>.Instance);
    }

    private static KafkaGlobalTable<string, byte[]> BuildEmptyGlobalTable()
    {
        var mockConsumer = new Mock<IConsumer<string, byte[]>>();
        mockConsumer.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
            .Throws(new OperationCanceledException());
        return new KafkaGlobalTable<string, byte[]>(
            mockConsumer.Object, "orders.pause", NullLogger<KafkaGlobalTable<string, byte[]>>.Instance);
    }

    [Fact]
    public async Task NullKey_DoesNotPublish()
    {
        var msg = MessageFactory.Build("orders.pause", null, Array.Empty<byte>());
        await _sut.ProcessAsync(msg, CancellationToken.None);
        _producerMock.Verify(
            p => p.ProduceAsync("orders.details", It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task EmptyKey_DoesNotPublish()
    {
        var msg = MessageFactory.Build("orders.pause", "", Array.Empty<byte>());
        await _sut.ProcessAsync(msg, CancellationToken.None);
        _producerMock.Verify(
            p => p.ProduceAsync("orders.details", It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task UnknownOrder_DoesNotPublish()
    {
        var proto = new PauseOrder { CreatedAt = WellKnownTimestamp.FromDateTime(DateTime.UtcNow) };
        var msg = MessageFactory.Build("orders.pause", "unknown-order", proto);

        await _sut.ProcessAsync(msg, CancellationToken.None);

        _producerMock.Verify(
            p => p.ProduceAsync("orders.details", It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task NullBytes_UsesUtcNowFallback_AndCallsAggregator()
    {
        await SeedOrderAsync("order-A");

        var msg = MessageFactory.Build("orders.pause", "order-A", (byte[]?)null);
        await _sut.ProcessAsync(msg, CancellationToken.None);

        _producerMock.Verify(
            p => p.ProduceAsync("orders.details", It.Is<Message<string, byte[]>>(m => m.Key == "order-A"), It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
    }

    [Fact]
    public async Task HappyPath_PublishesPauseRequestedState()
    {
        await SeedOrderAsync("order-B");

        var proto = new PauseOrder { CreatedAt = WellKnownTimestamp.FromDateTime(DateTime.UtcNow) };
        var msg = MessageFactory.Build("orders.pause", "order-B", proto);

        await _sut.ProcessAsync(msg, CancellationToken.None);

        _producerMock.Verify(
            p => p.ProduceAsync(
                "orders.details",
                It.Is<Message<string, byte[]>>(m => m.Key == "order-B"),
                It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
    }

    private async Task SeedOrderAsync(string orderId)
    {
        var payload = new OrderCreatedPayload("cust-seed", ImmutableList<OrderItem>.Empty);
        await _store.UpdateAsync(orderId, payload, CancellationToken.None);
        _producerMock.Invocations.Clear();
    }
}
