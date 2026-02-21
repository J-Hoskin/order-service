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

public class PaymentProcessorTests
{
    private readonly Mock<IProducer<string, string>> _producerMock;
    private readonly PaymentProcessor _sut;

    public PaymentProcessorTests()
    {
        _producerMock = new Mock<IProducer<string, string>>();
        _producerMock
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, string>());

        var kafkaProducer = new KafkaProducer(_producerMock.Object);
        var aggregator = new OrderDetailsAggregator(kafkaProducer, NullLogger<OrderDetailsAggregator>.Instance);

        _sut = new PaymentProcessor(aggregator, NullLogger<PaymentProcessor>.Instance);
    }

    [Fact]
    public async Task NullKey_DoesNotPublish()
    {
        var msg = MessageFactory.Build("orders.payment-confirmed", null, Array.Empty<byte>());
        await _sut.ProcessAsync(msg, CancellationToken.None);
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task EmptyValue_DoesNotPublish()
    {
        var msg = MessageFactory.Build("orders.payment-confirmed", "order-1", Array.Empty<byte>());
        await _sut.ProcessAsync(msg, CancellationToken.None);
        _producerMock.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task HappyPath_PublishesToOrdersDetails()
    {
        var proto = new PaymentConfirmed { PaymentReference = "PAY-XYZ" };
        var msg = MessageFactory.Build("orders.payment-confirmed", "order-5", proto);

        await _sut.ProcessAsync(msg, CancellationToken.None);

        _producerMock.Verify(
            p => p.ProduceAsync(
                "orders.details",
                It.Is<Message<string, string>>(m => m.Key == "order-5" && m.Value.Contains("PAY-XYZ")),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }
}
