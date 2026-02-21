using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using ProductService.Messaging;
using ProductService.Processors;
using ProductService.Proto;
using ProductService.Services;
using ProductService.Tests.Helpers;
using Xunit;

namespace ProductService.Tests.Processors;

public class WarehouseProcessorTests
{
    private readonly Mock<IProducer<string, string>> _producerMock;
    private readonly WarehouseProcessor _sut;

    public WarehouseProcessorTests()
    {
        _producerMock = new Mock<IProducer<string, string>>();
        _producerMock
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, string>());

        var kafkaProducer = new KafkaProducer(_producerMock.Object);
        var aggregator = new OrderDetailsAggregator(kafkaProducer, NullLogger<OrderDetailsAggregator>.Instance);

        _sut = new WarehouseProcessor(aggregator, NullLogger<WarehouseProcessor>.Instance);
    }

    [Fact]
    public async Task NullKey_DoesNotPublish()
    {
        var msg = MessageFactory.Build("orders.warehouse-picked", null, Array.Empty<byte>());
        await _sut.ProcessAsync(msg, CancellationToken.None);
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task EmptyValue_DoesNotPublish()
    {
        var msg = MessageFactory.Build("orders.warehouse-picked", "order-1", Array.Empty<byte>());
        await _sut.ProcessAsync(msg, CancellationToken.None);
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task HappyPath_PublishesToOrdersDetails()
    {
        var proto = new WarehousePicked { PickedBy = "Dave" };
        var msg = MessageFactory.Build("orders.warehouse-picked", "order-7", proto);

        await _sut.ProcessAsync(msg, CancellationToken.None);

        _producerMock.Verify(
            p => p.ProduceAsync(
                "orders.details",
                It.Is<Message<string, string>>(m => m.Key == "order-7" && m.Value.Contains("Dave")),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }
}
