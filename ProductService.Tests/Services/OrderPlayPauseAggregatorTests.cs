using System.Collections.Immutable;
using Confluent.Kafka;
using Google.Protobuf;
using WellKnownTimestamp = Google.Protobuf.WellKnownTypes.Timestamp;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using ProductService.Messaging;
using ProductService.Models;
using ProductService.Proto;
using ProductService.Services;
using Xunit;

namespace ProductService.Tests.Services;

public class OrderPlayPauseAggregatorTests
{
    private readonly Mock<IProducer<string, string>> _producerMock;
    private readonly OrderStore _store;
    private readonly OrderDetailsAggregator _aggregator;
    private readonly Mock<IConsumer<string, byte[]>> _tableConsumerMock;
    private KafkaGlobalTable<string, byte[]> _globalTable;
    private OrderPlayPauseAggregator _sut;

    public OrderPlayPauseAggregatorTests()
    {
        _producerMock = new Mock<IProducer<string, string>>();
        _producerMock
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, string>());

        var kafkaProducer = new KafkaProducer(_producerMock.Object);
        _aggregator = new OrderDetailsAggregator(kafkaProducer, NullLogger<OrderDetailsAggregator>.Instance);
        _store = new OrderStore(_aggregator, NullLogger<OrderStore>.Instance);

        _tableConsumerMock = new Mock<IConsumer<string, byte[]>>();
        _tableConsumerMock.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
            .Throws(new OperationCanceledException());

        _globalTable = new KafkaGlobalTable<string, byte[]>(
            _tableConsumerMock.Object, "orders.pause", NullLogger<KafkaGlobalTable<string, byte[]>>.Instance);

        _sut = BuildSut();
    }

    private OrderPlayPauseAggregator BuildSut() =>
        new(_aggregator, _store, _globalTable,
            new KafkaProducer(_producerMock.Object),
            NullLogger<OrderPlayPauseAggregator>.Instance);

    private async Task SeedOrderAsync(string orderId)
    {
        await _store.UpdateAsync(orderId, new OrderCreatedPayload("cust-seed", ImmutableList<OrderItem>.Empty));
        _producerMock.Invocations.Clear();
    }

    // ── PauseAsync ───────────────────────────────────────────────────────────

    [Fact]
    public async Task PauseAsync_UnknownOrder_DoesNotPublish()
    {
        await _sut.PauseAsync("ghost", DateTime.UtcNow);
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task PauseAsync_KnownOrder_SetsAndPublishesPauseRequested()
    {
        await SeedOrderAsync("order-P1");
        await _sut.PauseAsync("order-P1", DateTime.UtcNow);
        _producerMock.Verify(
            p => p.ProduceAsync("orders.details", It.Is<Message<string, string>>(m => m.Key == "order-P1"), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    // ── ConfirmPauseAsync ────────────────────────────────────────────────────

    [Fact]
    public async Task ConfirmPauseAsync_UnknownOrder_DoesNotPublish()
    {
        await _sut.ConfirmPauseAsync("ghost", DateTime.UtcNow);
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ConfirmPauseAsync_StaleCreatedAt_DoesNotPublish()
    {
        await SeedOrderAsync("order-P2");
        var staleTime = DateTime.UtcNow.AddSeconds(-20); // older than 10-second window
        await _sut.ConfirmPauseAsync("order-P2", staleTime);
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ConfirmPauseAsync_NoPauseInGlobalTable_DoesNotPublish()
    {
        await SeedOrderAsync("order-P3");
        // GlobalTable is empty → Guard 2 fires
        await _sut.ConfirmPauseAsync("order-P3", DateTime.UtcNow);
        _producerMock.Verify(
            p => p.ProduceAsync("orders.alerts", It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ConfirmPauseAsync_TimestampsTooFarApart_DoesNotPublish()
    {
        await SeedOrderAsync("order-P4");

        // Seed GlobalTable with a PauseOrder that is 20 seconds before confirmCreatedAt
        var pauseTime = DateTime.UtcNow.AddSeconds(-20);
        var pauseProto = new PauseOrder { CreatedAt = WellKnownTimestamp.FromDateTime(pauseTime) };
        var pauseBytes = pauseProto.ToByteArray();
        await PreloadGlobalTable("order-P4", pauseBytes);

        var confirmTime = DateTime.UtcNow; // 20 seconds apart → > 10s threshold
        await _sut.ConfirmPauseAsync("order-P4", confirmTime);

        _producerMock.Verify(
            p => p.ProduceAsync("orders.alerts", It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ConfirmPauseAsync_AllGuardsPass_PublishesAlertAndSetsStatus()
    {
        await SeedOrderAsync("order-P5");
        await _sut.PauseAsync("order-P5", DateTime.UtcNow);
        _producerMock.Invocations.Clear();

        var now = DateTime.UtcNow;
        var pauseProto = new PauseOrder { CreatedAt = WellKnownTimestamp.FromDateTime(now) };
        await PreloadGlobalTable("order-P5", pauseProto.ToByteArray());

        await _sut.ConfirmPauseAsync("order-P5", now);

        _producerMock.Verify(
            p => p.ProduceAsync(
                "orders.alerts",
                It.Is<Message<string, string>>(m => m.Key == "order-P5" && m.Value.Contains("OrderPaused")),
                It.IsAny<CancellationToken>()),
            Times.Once);

        _producerMock.Verify(
            p => p.ProduceAsync(
                "orders.details",
                It.Is<Message<string, string>>(m => m.Key == "order-P5"),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    // ── ResumeAsync ──────────────────────────────────────────────────────────

    [Fact]
    public async Task ResumeAsync_UnknownOrder_DoesNotPublish()
    {
        await _sut.ResumeAsync("ghost");
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ResumeAsync_KnownOrder_PublishesDetailsAndTombstone()
    {
        await SeedOrderAsync("order-R1");
        await _sut.ResumeAsync("order-R1");

        _producerMock.Verify(
            p => p.ProduceAsync("orders.details", It.Is<Message<string, string>>(m => m.Key == "order-R1"), It.IsAny<CancellationToken>()),
            Times.Once);
        _producerMock.Verify(
            p => p.ProduceAsync("orders.pause", It.Is<Message<string, string>>(m => m.Key == "order-R1" && m.Value == null), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    /// <summary>
    /// Rebuilds the GlobalTable with a mock consumer that returns one message then stops,
    /// then rebuilds _sut with the new GlobalTable.
    /// </summary>
    private async Task PreloadGlobalTable(string key, byte[] value)
    {
        var tcs = new TaskCompletionSource();
        int callCount = 0;

        var mockConsumer = new Mock<IConsumer<string, byte[]>>();
        mockConsumer.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                if (callCount++ == 0)
                    return new ConsumeResult<string, byte[]>
                    {
                        Topic = "orders.pause",
                        Message = new Message<string, byte[]> { Key = key, Value = value }
                    };
                tcs.TrySetResult();
                throw new OperationCanceledException();
            });

        _globalTable = new KafkaGlobalTable<string, byte[]>(
            mockConsumer.Object, "orders.pause", NullLogger<KafkaGlobalTable<string, byte[]>>.Instance);

        await _globalTable.StartAsync(CancellationToken.None);
        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Rebuild sut with the new GlobalTable
        _sut = BuildSut();
    }
}
