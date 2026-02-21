using System.Collections.Immutable;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using ProductService.Messaging;
using ProductService.Models;
using ProductService.Services;
using Xunit;

namespace ProductService.Tests.Services;

public class OrderDetailsAggregatorTests
{
    private readonly Mock<IProducer<string, string>> _producerMock;
    private readonly OrderDetailsAggregator _sut;

    public OrderDetailsAggregatorTests()
    {
        _producerMock = new Mock<IProducer<string, string>>();
        _producerMock
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, string>());

        var kafkaProducer = new KafkaProducer(_producerMock.Object);
        _sut = new OrderDetailsAggregator(kafkaProducer, NullLogger<OrderDetailsAggregator>.Instance);
    }

    private OrderDetails CaptureLastPublished()
    {
        var lastCall = _producerMock.Invocations
            .Where(i => i.Method.Name == nameof(IProducer<string, string>.ProduceAsync))
            .Last();
        var msg = (Message<string, string>)lastCall.Arguments[1];
        return JsonSerializer.Deserialize<OrderDetails>(msg.Value)!;
    }

    // ── UpdateOrderAsync ────────────────────────────────────────────────────

    [Fact]
    public async Task UpdateOrderAsync_NewOrder_StatusIsPlaced()
    {
        await _sut.UpdateOrderAsync("ord-1", "cust-1", ImmutableList<OrderItem>.Empty);
        var published = CaptureLastPublished();
        Assert.Equal(OrderStatus.Placed, published.Status);
        Assert.Equal("cust-1", published.CustomerId);
    }

    [Fact]
    public async Task UpdateOrderAsync_PublishesToOrdersDetails()
    {
        await _sut.UpdateOrderAsync("ord-2", "c", ImmutableList<OrderItem>.Empty);
        _producerMock.Verify(p => p.ProduceAsync("orders.details",
            It.Is<Message<string, string>>(m => m.Key == "ord-2"),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    // ── AddPaymentReferenceAsync ─────────────────────────────────────────────

    [Fact]
    public async Task AddPaymentReferenceAsync_StatusBecomesPaymentConfirmed()
    {
        await _sut.AddPaymentReferenceAsync("ord-3", "PAY-001");
        var published = CaptureLastPublished();
        Assert.Equal(OrderStatus.PaymentConfirmed, published.Status);
        Assert.Equal("PAY-001", published.PaymentReference);
    }

    // ── AddWarehousePickAsync ────────────────────────────────────────────────

    [Fact]
    public async Task AddWarehousePickAsync_StatusBecomesWarehousePicked()
    {
        await _sut.AddWarehousePickAsync("ord-4", "Alice");
        var published = CaptureLastPublished();
        Assert.Equal(OrderStatus.WarehousePicked, published.Status);
        Assert.Equal("Alice", published.PickedByWarehouseStaff);
    }

    [Fact]
    public async Task PaymentThenWarehouse_StatusIsWarehousePicked()
    {
        await _sut.AddPaymentReferenceAsync("ord-5", "PAY-002");
        await _sut.AddWarehousePickAsync("ord-5", "Bob");
        var published = CaptureLastPublished();
        Assert.Equal(OrderStatus.WarehousePicked, published.Status);
    }

    // ── SetPauseRequestedAsync ───────────────────────────────────────────────

    [Fact]
    public async Task SetPauseRequestedAsync_Only_StatusRemainsPlaced()
    {
        await _sut.SetPauseRequestedAsync("ord-6");
        var published = CaptureLastPublished();
        // PauseRequested = true but PauseConfirmed = false → not Paused yet
        Assert.NotEqual(OrderStatus.Paused, published.Status);
        Assert.True(published.PauseRequested);
    }

    // ── SetPauseConfirmedAsync ───────────────────────────────────────────────

    [Fact]
    public async Task PauseRequestedThenConfirmed_StatusIsPaused()
    {
        await _sut.SetPauseRequestedAsync("ord-7");
        await _sut.SetPauseConfirmedAsync("ord-7");
        var published = CaptureLastPublished();
        Assert.Equal(OrderStatus.Paused, published.Status);
    }

    [Fact]
    public async Task ConfirmedThenRequested_StatusIsPaused()
    {
        // Order is independent — both flags must be set, regardless of which arrived first
        await _sut.SetPauseConfirmedAsync("ord-8");
        await _sut.SetPauseRequestedAsync("ord-8");
        var published = CaptureLastPublished();
        Assert.Equal(OrderStatus.Paused, published.Status);
    }

    // ── ResumeOrderAsync ─────────────────────────────────────────────────────

    [Fact]
    public async Task ResumeOrderAsync_AfterPaused_StatusRevertsToPlaced()
    {
        await _sut.SetPauseRequestedAsync("ord-9");
        await _sut.SetPauseConfirmedAsync("ord-9");
        await _sut.ResumeOrderAsync("ord-9");
        var published = CaptureLastPublished();
        Assert.Equal(OrderStatus.Placed, published.Status);
        Assert.False(published.PauseRequested);
        Assert.False(published.PauseConfirmed);
    }

    [Fact]
    public async Task ResumeOrderAsync_AfterWarehousePicked_StatusRevertsToWarehousePicked()
    {
        await _sut.AddWarehousePickAsync("ord-10", "Eve");
        await _sut.SetPauseRequestedAsync("ord-10");
        await _sut.SetPauseConfirmedAsync("ord-10");
        await _sut.ResumeOrderAsync("ord-10");
        var published = CaptureLastPublished();
        Assert.Equal(OrderStatus.WarehousePicked, published.Status);
    }

    // ── Unknown orderId ──────────────────────────────────────────────────────

    [Fact]
    public async Task UnknownOrderId_StillPublishes()
    {
        await _sut.AddPaymentReferenceAsync("new-ord", "PAY-NEW");
        _producerMock.Verify(
            p => p.ProduceAsync("orders.details", It.Is<Message<string, string>>(m => m.Key == "new-ord"), It.IsAny<CancellationToken>()),
            Times.Once);
    }
}
