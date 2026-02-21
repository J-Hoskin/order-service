using Confluent.Kafka;
using Moq;
using ProductService.Messaging;
using Xunit;

namespace ProductService.Tests.Messaging;

public class KafkaProducerTests
{
    private readonly Mock<IProducer<string, string>> _innerMock;
    private readonly KafkaProducer _sut;

    public KafkaProducerTests()
    {
        _innerMock = new Mock<IProducer<string, string>>();
        _innerMock
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, string>());

        _sut = new KafkaProducer(_innerMock.Object);
    }

    [Fact]
    public async Task ProduceAsync_ForwardsTopicKeyAndValue()
    {
        await _sut.ProduceAsync("test-topic", "my-key", "my-value");

        _innerMock.Verify(p => p.ProduceAsync(
            "test-topic",
            It.Is<Message<string, string>>(m => m.Key == "my-key" && m.Value == "my-value"),
            It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ProduceAsync_NullValue_ForwardsNull()
    {
        await _sut.ProduceAsync("test-topic", "tombstone-key", null);

        _innerMock.Verify(p => p.ProduceAsync(
            "test-topic",
            It.Is<Message<string, string>>(m => m.Key == "tombstone-key" && m.Value == null),
            It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ProduceAsync_WhenInnerThrowsProduceException_WrapsInException()
    {
        var error = new Error(ErrorCode.Local_AllBrokersDown, "all brokers down");
        var deliveryResult = new DeliveryResult<string, string>();
        _innerMock
            .Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, string>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ProduceException<string, string>(error, deliveryResult));

        var ex = await Assert.ThrowsAsync<Exception>(() =>
            _sut.ProduceAsync("my-topic", "k", "v"));

        Assert.Contains("my-topic", ex.Message);
        Assert.Contains(ErrorCode.Local_AllBrokersDown.ToString(), ex.Message);
    }

    [Fact]
    public void Dispose_FlushesAndDisposesInnerProducer()
    {
        _sut.Dispose();

        _innerMock.Verify(p => p.Flush(TimeSpan.FromSeconds(5)), Times.Once);
        _innerMock.Verify(p => p.Dispose(), Times.Once);
    }
}
