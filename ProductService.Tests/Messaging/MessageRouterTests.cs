using Autofac;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using ProductService.Interfaces;
using ProductService.Messaging;
using ProductService.Tests.Helpers;
using Xunit;

namespace ProductService.Tests.Messaging;

public class MessageRouterTests
{
    private static MessageRouter BuildRouter(Action<ContainerBuilder>? configure = null)
    {
        var builder = new ContainerBuilder();
        configure?.Invoke(builder);
        var container = builder.Build();
        return new MessageRouter(container, NullLogger<MessageRouter>.Instance);
    }

    [Fact]
    public async Task KnownTopic_ResolvesProcessorAndCallsProcessAsync()
    {
        var mockProcessor = new Mock<IMessageProcessor>();
        mockProcessor.Setup(p => p.ProcessAsync(It.IsAny<ConsumeResult<string, byte[]>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var router = BuildRouter(b =>
            b.RegisterInstance(mockProcessor.Object).Keyed<IMessageProcessor>("orders.created"));

        var msg = MessageFactory.Build("orders.created", "k1", Array.Empty<byte>());
        await router.RouteAsync(msg, CancellationToken.None);

        mockProcessor.Verify(p => p.ProcessAsync(msg, CancellationToken.None), Times.Once);
    }

    [Fact]
    public async Task UnknownTopic_DoesNotCallAnyProcessor()
    {
        var mockProcessor = new Mock<IMessageProcessor>();
        var router = BuildRouter(b =>
            b.RegisterInstance(mockProcessor.Object).Keyed<IMessageProcessor>("orders.created"));

        var msg = MessageFactory.Build("orders.no-such-topic", "k1", Array.Empty<byte>());
        await router.RouteAsync(msg, CancellationToken.None); // should not throw

        mockProcessor.Verify(p => p.ProcessAsync(It.IsAny<ConsumeResult<string, byte[]>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ProcessorThrows_ExceptionPropagates()
    {
        var mockProcessor = new Mock<IMessageProcessor>();
        mockProcessor.Setup(p => p.ProcessAsync(It.IsAny<ConsumeResult<string, byte[]>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("boom"));

        var router = BuildRouter(b =>
            b.RegisterInstance(mockProcessor.Object).Keyed<IMessageProcessor>("orders.created"));

        var msg = MessageFactory.Build("orders.created", "k1", Array.Empty<byte>());
        await Assert.ThrowsAsync<InvalidOperationException>(() => router.RouteAsync(msg, CancellationToken.None));
    }

    [Fact]
    public async Task CancellationToken_IsForwardedToProcessor()
    {
        var cts = new CancellationTokenSource();
        var capturedToken = CancellationToken.None;

        var mockProcessor = new Mock<IMessageProcessor>();
        mockProcessor.Setup(p => p.ProcessAsync(It.IsAny<ConsumeResult<string, byte[]>>(), It.IsAny<CancellationToken>()))
            .Callback<ConsumeResult<string, byte[]>, CancellationToken>((_, ct) => capturedToken = ct)
            .Returns(Task.CompletedTask);

        var router = BuildRouter(b =>
            b.RegisterInstance(mockProcessor.Object).Keyed<IMessageProcessor>("orders.payment-confirmed"));

        var msg = MessageFactory.Build("orders.payment-confirmed", "k1", Array.Empty<byte>());
        await router.RouteAsync(msg, cts.Token);

        Assert.Equal(cts.Token, capturedToken);
    }
}
