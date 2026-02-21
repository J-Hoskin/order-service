using System.Collections.Immutable;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using ProductService.Messaging;
using ProductService.Models;
using ProductService.Services;
using Xunit;

namespace ProductService.Tests.Services;

public class OrderStoreTests
{
    private readonly Mock<IProducer<string, byte[]>> _producerMock;
    private readonly OrderStore _sut;

    public OrderStoreTests()
    {
        _producerMock = new Mock<IProducer<string, byte[]>>();
        _producerMock
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, byte[]>());

        var kafkaProducer = new KafkaProducer(_producerMock.Object);
        var aggregator = new OrderDetailsAggregator(kafkaProducer, NullLogger<OrderDetailsAggregator>.Instance);
        _sut = new OrderStore(aggregator, NullLogger<OrderStore>.Instance);
    }

    [Fact]
    public void Exists_ReturnsFalse_WhenOrderNotSeen()
    {
        Assert.False(_sut.Exists("unknown-order"));
    }

    [Fact]
    public async Task Exists_ReturnsTrue_AfterUpdateAsync()
    {
        var payload = new OrderCreatedPayload("cust-1", ImmutableList<OrderItem>.Empty);
        await _sut.UpdateAsync("order-X", payload);
        Assert.True(_sut.Exists("order-X"));
    }

    [Fact]
    public async Task UpdateAsync_CallsAggregator_PublishesToOrdersDetails()
    {
        var items = ImmutableList.Create(new OrderItem("p1", "Widget", 2, 9.99m));
        var payload = new OrderCreatedPayload("cust-42", items);

        await _sut.UpdateAsync("order-Y", payload);

        _producerMock.Verify(
            p => p.ProduceAsync(
                "orders.details",
                It.Is<Message<string, byte[]>>(m => m.Key == "order-Y" && System.Text.Encoding.UTF8.GetString(m.Value).Contains("cust-42")),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task UpdateAsync_SecondCall_OverwritesCacheAndCallsAggregatorAgain()
    {
        var payload1 = new OrderCreatedPayload("cust-A", ImmutableList<OrderItem>.Empty);
        var payload2 = new OrderCreatedPayload("cust-B", ImmutableList<OrderItem>.Empty);

        await _sut.UpdateAsync("order-Z", payload1);
        await _sut.UpdateAsync("order-Z", payload2);

        _producerMock.Verify(
            p => p.ProduceAsync("orders.details", It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()),
            Times.Exactly(2));

        Assert.True(_sut.Exists("order-Z"));
    }
}
