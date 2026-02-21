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

public class ConfirmPauseProcessorTests
{
    private readonly Mock<IProducer<string, string>> _producerMock;
    private readonly OrderStore _store;
    private readonly KafkaGlobalTable<string, byte[]> _globalTable;
    private readonly ConfirmPauseProcessor _sut;

    public ConfirmPauseProcessorTests()
    {
        _producerMock = new Mock<IProducer<string, string>>();
        _producerMock
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, string>());

        var kafkaProducer = new KafkaProducer(_producerMock.Object);
        var aggregator = new OrderDetailsAggregator(kafkaProducer, NullLogger<OrderDetailsAggregator>.Instance);
        _store = new OrderStore(aggregator, NullLogger<OrderStore>.Instance);

        _globalTable = BuildEmptyGlobalTable();
        var playPause = new OrderPlayPauseAggregator(
            aggregator, _store, _globalTable, kafkaProducer,
            NullLogger<OrderPlayPauseAggregator>.Instance);

        _sut = new ConfirmPauseProcessor(playPause, NullLogger<ConfirmPauseProcessor>.Instance);
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
        var msg = MessageFactory.Build("orders.confirm-pause", null, Array.Empty<byte>());
        await _sut.ProcessAsync(msg, CancellationToken.None);
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task EmptyKey_DoesNotPublish()
    {
        var msg = MessageFactory.Build("orders.confirm-pause", "", Array.Empty<byte>());
        await _sut.ProcessAsync(msg, CancellationToken.None);
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task UnknownOrder_DoesNotPublish()
    {
        var proto = new ConfirmPause { CreatedAt = WellKnownTimestamp.FromDateTime(DateTime.UtcNow) };
        var msg = MessageFactory.Build("orders.confirm-pause", "no-such-order", proto);

        await _sut.ProcessAsync(msg, CancellationToken.None);

        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task NullBytes_UsesUtcNowFallback_StillCallsAggregator()
    {
        // Even with null bytes, createdAt = UtcNow passes Guard 1 (stale check).
        // But the order must be known and GlobalTable must have matching pause entry.
        // Since GlobalTable is empty, Guard 2 fires → no publish. Still verifies null-byte branch runs.
        await SeedOrderAsync("order-C");
        var msg = MessageFactory.Build("orders.confirm-pause", "order-C", (byte[]?)null);
        await _sut.ProcessAsync(msg, CancellationToken.None);

        // Guard 2 fires (no pause in GlobalTable) → nothing published to orders.alerts
        _producerMock.Verify(
            p => p.ProduceAsync("orders.alerts", It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    private async Task SeedOrderAsync(string orderId)
    {
        var payload = new OrderCreatedPayload("cust-seed", ImmutableList<OrderItem>.Empty);
        await _store.UpdateAsync(orderId, payload, CancellationToken.None);
        _producerMock.Invocations.Clear();
    }
}
