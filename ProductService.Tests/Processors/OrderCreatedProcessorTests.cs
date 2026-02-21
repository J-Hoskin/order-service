using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using ProductService.Messaging;
using ProductService.Processors;
using ProductService.Proto;
using ProductService.Services;
using ProductService.Tests.Helpers;
using Xunit;

namespace ProductService.Tests.Processors;

/// <summary>
/// Tests for OrderCreatedProcessor. Uses a real service chain and verifies
/// behaviour at the IProducer boundary (the only mockable seam).
/// </summary>
public class OrderCreatedProcessorTests
{
    private readonly Mock<IProducer<string, byte[]>> _producerMock;
    private readonly OrderCreatedProcessor _sut;

    public OrderCreatedProcessorTests()
    {
        _producerMock = new Mock<IProducer<string, byte[]>>();
        _producerMock
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, byte[]>());

        var kafkaProducer = new KafkaProducer(_producerMock.Object);
        var aggregator = new OrderDetailsAggregator(kafkaProducer, NullLogger<OrderDetailsAggregator>.Instance);
        var store = new OrderStore(aggregator, NullLogger<OrderStore>.Instance);

        _sut = new OrderCreatedProcessor(store, kafkaProducer, NullLogger<OrderCreatedProcessor>.Instance);
    }

    [Fact]
    public async Task NullKey_DoesNotPublish()
    {
        var msg = MessageFactory.Build("orders.created", null, Array.Empty<byte>());
        await _sut.ProcessAsync(msg, CancellationToken.None);
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task EmptyKey_DoesNotPublish()
    {
        var msg = MessageFactory.Build("orders.created", "", Array.Empty<byte>());
        await _sut.ProcessAsync(msg, CancellationToken.None);
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task NullValue_DoesNotPublish()
    {
        var msg = MessageFactory.Build("orders.created", "order-1", (byte[]?)null);
        await _sut.ProcessAsync(msg, CancellationToken.None);
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task EmptyValue_DoesNotPublish()
    {
        var msg = MessageFactory.Build("orders.created", "order-1", Array.Empty<byte>());
        await _sut.ProcessAsync(msg, CancellationToken.None);
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task HappyPath_PublishesToOrdersDetails()
    {
        var proto = new OrderCreated
        {
            CustomerId = "cust-42",
            Items = { new ProtoOrderItem { ProductId = "p1", ProductName = "Widget", Quantity = 2, Price = "5.00" } }
        };
        var msg = MessageFactory.Build("orders.created", "order-99", proto);

        await _sut.ProcessAsync(msg, CancellationToken.None);

        _producerMock.Verify(
            p => p.ProduceAsync(
                "orders.details",
                It.Is<Message<string, byte[]>>(m => m.Key == "order-99"),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task MoreThanFiveItems_PublishesToOrdersWhales()
    {
        var proto = new OrderCreated
        {
            CustomerId = "cust-whale",
            Items =
            {
                new ProtoOrderItem { ProductId = "p1", ProductName = "A", Quantity = 1, Price = "1.00" },
                new ProtoOrderItem { ProductId = "p2", ProductName = "B", Quantity = 1, Price = "1.00" },
                new ProtoOrderItem { ProductId = "p3", ProductName = "C", Quantity = 1, Price = "1.00" },
                new ProtoOrderItem { ProductId = "p4", ProductName = "D", Quantity = 1, Price = "1.00" },
                new ProtoOrderItem { ProductId = "p5", ProductName = "E", Quantity = 1, Price = "1.00" },
                new ProtoOrderItem { ProductId = "p6", ProductName = "F", Quantity = 1, Price = "1.00" },
            }
        };
        var expectedBytes = proto.ToByteArray();
        var msg = MessageFactory.Build("orders.created", "order-whale", proto);

        await _sut.ProcessAsync(msg, CancellationToken.None);

        _producerMock.Verify(
            p => p.ProduceAsync(
                "orders.whales",
                It.Is<Message<string, byte[]>>(m => m.Key == "order-whale" && m.Value.SequenceEqual(expectedBytes)),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task FiveOrFewerItems_DoesNotPublishToOrdersWhales()
    {
        var proto = new OrderCreated
        {
            CustomerId = "cust-small",
            Items =
            {
                new ProtoOrderItem { ProductId = "p1", ProductName = "A", Quantity = 1, Price = "1.00" },
                new ProtoOrderItem { ProductId = "p2", ProductName = "B", Quantity = 1, Price = "1.00" },
                new ProtoOrderItem { ProductId = "p3", ProductName = "C", Quantity = 1, Price = "1.00" },
                new ProtoOrderItem { ProductId = "p4", ProductName = "D", Quantity = 1, Price = "1.00" },
                new ProtoOrderItem { ProductId = "p5", ProductName = "E", Quantity = 1, Price = "1.00" },
            }
        };
        var msg = MessageFactory.Build("orders.created", "order-small", proto);

        await _sut.ProcessAsync(msg, CancellationToken.None);

        _producerMock.Verify(
            p => p.ProduceAsync(
                "orders.whales",
                It.IsAny<Message<string, byte[]>>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }
}
