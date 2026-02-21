using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using ProductService.Messaging;
using Xunit;

namespace ProductService.Tests.Messaging;

public class KafkaGlobalTableTests
{
    private static KafkaGlobalTable<string, byte[]> Build(IConsumer<string, byte[]> consumer) =>
        new(consumer, "test-topic", NullLogger<KafkaGlobalTable<string, byte[]>>.Instance);

    private static ConsumeResult<string, byte[]> MakeResult(string key, byte[]? value) =>
        new()
        {
            Topic = "test-topic",
            Message = new Message<string, byte[]> { Key = key, Value = value! }
        };

    [Fact]
    public void Get_ReturnsNull_WhenKeyNotPresent()
    {
        var mockConsumer = new Mock<IConsumer<string, byte[]>>();
        mockConsumer.Setup(c => c.Consume(It.IsAny<CancellationToken>())).Throws(new OperationCanceledException());
        var table = Build(mockConsumer.Object);
        Assert.Null(table.Get("missing-key"));
    }

    [Fact]
    public void ContainsKey_ReturnsFalse_WhenEmpty()
    {
        var mockConsumer = new Mock<IConsumer<string, byte[]>>();
        mockConsumer.Setup(c => c.Consume(It.IsAny<CancellationToken>())).Throws(new OperationCanceledException());
        var table = Build(mockConsumer.Object);
        Assert.False(table.ContainsKey("k"));
    }

    [Fact]
    public async Task Get_ReturnsValue_AfterConsumeLoopProcessesMessage()
    {
        var payload = new byte[] { 1, 2, 3 };
        var tcs = new TaskCompletionSource();
        int callCount = 0;

        var mockConsumer = new Mock<IConsumer<string, byte[]>>();
        mockConsumer.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                if (callCount++ == 0)
                    return MakeResult("key-A", payload);
                tcs.TrySetResult();
                throw new OperationCanceledException();
            });

        var table = Build(mockConsumer.Object);
        await table.StartAsync(CancellationToken.None);
        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(payload, table.Get("key-A"));
        Assert.True(table.ContainsKey("key-A"));

        await table.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Tombstone_RemovesKeyFromTable()
    {
        var payload = new byte[] { 7, 8, 9 };
        int callCount = 0;
        var tcs = new TaskCompletionSource();

        var mockConsumer = new Mock<IConsumer<string, byte[]>>();
        mockConsumer.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                callCount++;
                if (callCount == 1) return MakeResult("key-B", payload);    // upsert
                if (callCount == 2) return MakeResult("key-B", null);         // tombstone
                tcs.TrySetResult();
                throw new OperationCanceledException();
            });

        var table = Build(mockConsumer.Object);
        await table.StartAsync(CancellationToken.None);
        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Null(table.Get("key-B"));
        Assert.False(table.ContainsKey("key-B"));

        await table.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ConsumeException_LoggedAndLoopContinues()
    {
        int callCount = 0;
        var tcs = new TaskCompletionSource();

        var mockConsumer = new Mock<IConsumer<string, byte[]>>();
        mockConsumer.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                callCount++;
                if (callCount == 1)
                    throw new ConsumeException(
                        new ConsumeResult<byte[], byte[]>(),
                        new Error(ErrorCode.Local_AllBrokersDown));
                // Second call succeeds
                if (callCount == 2) return MakeResult("key-C", new byte[] { 1 });
                tcs.TrySetResult();
                throw new OperationCanceledException();
            });

        var table = Build(mockConsumer.Object);
        await table.StartAsync(CancellationToken.None);
        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Loop continued after exception and processed the second message
        Assert.True(table.ContainsKey("key-C"));

        await table.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Shutdown_LoopExitsCleanly()
    {
        // When stoppingToken is cancelled, Consume throws OperationCanceledException
        // which propagates out of the while loop. The background task completes cleanly.
        var mockConsumer = new Mock<IConsumer<string, byte[]>>();
        mockConsumer.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
            .Throws(new OperationCanceledException());

        var table = Build(mockConsumer.Object);
        await table.StartAsync(CancellationToken.None);

        // StopAsync should return (not hang) because the background task exited
        var stopTask = table.StopAsync(CancellationToken.None);
        await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.True(stopTask.IsCompleted, "StopAsync should complete within 5 seconds");
    }
}
