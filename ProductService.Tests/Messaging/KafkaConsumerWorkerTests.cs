// using Autofac;
// using Confluent.Kafka;
// using Microsoft.Extensions.Logging.Abstractions;
// using Moq;
// using ProductService.Interfaces;
// using ProductService.Messaging;
// using ProductService.Tests.Helpers;
// using Xunit;
//
// namespace ProductService.Tests.Messaging;
//
// public class KafkaConsumerWorkerTests
// {
//     private static readonly string[] ExpectedTopics =
//     [
//         "orders.created",
//         "orders.payment-confirmed",
//         "orders.warehouse-picked",
//         "orders.pause",
//         "orders.confirm-pause",
//         "orders.resume"
//     ];
//
//     private static (KafkaConsumerWorker worker, Mock<IConsumer<string, byte[]>> consumerMock, Mock<IMessageProcessor> processorMock)
//         BuildWorker()
//     {
//         var processorMock = new Mock<IMessageProcessor>();
//         processorMock.Setup(p => p.ProcessAsync(It.IsAny<ConsumeResult<string, byte[]>>(), It.IsAny<CancellationToken>()))
//             .Returns(Task.CompletedTask);
//
//         var builder = new ContainerBuilder();
//         foreach (var topic in ExpectedTopics)
//             builder.RegisterInstance(processorMock.Object).Keyed<IMessageProcessor>(topic);
//         var container = builder.Build();
//
//         var router = new MessageRouter(container, NullLogger<MessageRouter>.Instance);
//         var consumerMock = new Mock<IConsumer<string, byte[]>>();
//         var worker = new KafkaConsumerWorker(router, consumerMock.Object, NullLogger<KafkaConsumerWorker>.Instance);
//
//         return (worker, consumerMock, processorMock);
//     }
//
//     [Fact]
//     public async Task ExecuteAsync_SubscribesToAllSixTopics()
//     {
//         var (worker, consumerMock, _) = BuildWorker();
//         consumerMock.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
//             .Throws(new OperationCanceledException());
//
//         using var cts = new CancellationTokenSource();
//         cts.Cancel();
//
//         await worker.StartAsync(cts.Token);
//         await worker.StopAsync(CancellationToken.None);
//
//         consumerMock.Verify(c => c.Subscribe(
//             It.Is<IEnumerable<string>>(topics => !ExpectedTopics.Except(topics).Any())),
//             Times.Once);
//     }
//
//     [Fact]
//     public async Task HappyPath_CommitsAfterSuccessfulRouting()
//     {
//         var (worker, consumerMock, _) = BuildWorker();
//         var message = MessageFactory.Build("orders.created", "k1", Array.Empty<byte>());
//         int callCount = 0;
//
//         consumerMock.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
//             .Returns(() =>
//             {
//                 if (callCount++ == 0) return message;
//                 throw new OperationCanceledException();
//             });
//
//         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
//         await worker.StartAsync(cts.Token);
//         await Task.Delay(200, CancellationToken.None);
//         await worker.StopAsync(CancellationToken.None);
//
//         consumerMock.Verify(c => c.Commit(message), Times.Once);
//     }
//
//     [Fact]
//     public async Task Shutdown_CallsConsumerClose()
//     {
//         var (worker, consumerMock, _) = BuildWorker();
//         consumerMock.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
//             .Throws(new OperationCanceledException());
//
//         using var cts = new CancellationTokenSource();
//         await worker.StartAsync(cts.Token);
//         await worker.StopAsync(CancellationToken.None);
//
//         consumerMock.Verify(c => c.Close(), Times.Once);
//     }
//
//     [Fact]
//     public async Task ConsumeException_LoggedAndLoopContinues()
//     {
//         var (worker, consumerMock, _) = BuildWorker();
//         int callCount = 0;
//
//         consumerMock.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
//             .Returns(() =>
//             {
//                 callCount++;
//                 if (callCount == 1)
//                     throw new ConsumeException(
//                         new ConsumeResult<byte[], byte[]>(),
//                         new Error(ErrorCode.Local_AllBrokersDown));
//                 throw new OperationCanceledException();
//             });
//
//         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
//         await worker.StartAsync(cts.Token);
//         await Task.Delay(200, CancellationToken.None);
//         await worker.StopAsync(CancellationToken.None);
//
//         // Two Consume calls: first threw ConsumeException, second threw OperationCanceledException
//         Assert.True(callCount >= 2);
//     }
//
//     [Fact]
//     public async Task RouteAsyncThrows_CommitNotCalled()
//     {
//         var processorMock = new Mock<IMessageProcessor>();
//         processorMock.Setup(p => p.ProcessAsync(It.IsAny<ConsumeResult<string, byte[]>>(), It.IsAny<CancellationToken>()))
//             .ThrowsAsync(new InvalidOperationException("processor boom"));
//
//         var builder = new ContainerBuilder();
//         builder.RegisterInstance(processorMock.Object).Keyed<IMessageProcessor>("orders.created");
//         var container = builder.Build();
//         var router = new MessageRouter(container, NullLogger<MessageRouter>.Instance);
//
//         var consumerMock = new Mock<IConsumer<string, byte[]>>();
//         var message = MessageFactory.Build("orders.created", "k1", Array.Empty<byte>());
//         int callCount = 0;
//         consumerMock.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
//             .Returns(() =>
//             {
//                 if (callCount++ == 0) return message;
//                 throw new OperationCanceledException();
//             });
//
//         var worker = new KafkaConsumerWorker(router, consumerMock.Object, NullLogger<KafkaConsumerWorker>.Instance);
//
//         using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
//         await worker.StartAsync(cts.Token);
//         await Task.Delay(200, CancellationToken.None);
//         await worker.StopAsync(CancellationToken.None);
//
//         consumerMock.Verify(c => c.Commit(It.IsAny<ConsumeResult<string, byte[]>>()), Times.Never);
//     }
//
//     [Fact]
//     public async Task PerMessageTimeout_CommitNotCalled()
//     {
//         var processorMock = new Mock<IMessageProcessor>();
//         // Simulate a slow processor that runs beyond the 30-second timeout by returning a cancelled task
//         processorMock.Setup(p => p.ProcessAsync(It.IsAny<ConsumeResult<string, byte[]>>(), It.IsAny<CancellationToken>()))
//             .Returns<ConsumeResult<string, byte[]>, CancellationToken>(async (_, ct) =>
//             {
//                 // Wait until the cancellation token fires (per-message timeout or stoppingToken)
//                 await Task.Delay(Timeout.Infinite, ct);
//             });
//
//         var builder = new ContainerBuilder();
//         builder.RegisterInstance(processorMock.Object).Keyed<IMessageProcessor>("orders.created");
//         var container = builder.Build();
//         var router = new MessageRouter(container, NullLogger<MessageRouter>.Instance);
//
//         var consumerMock = new Mock<IConsumer<string, byte[]>>();
//         var message = MessageFactory.Build("orders.created", "k1", Array.Empty<byte>());
//         int callCount = 0;
//         consumerMock.Setup(c => c.Consume(It.IsAny<CancellationToken>()))
//             .Returns(() =>
//             {
//                 if (callCount++ == 0) return message;
//                 throw new OperationCanceledException();
//             });
//
//         var worker = new KafkaConsumerWorker(router, consumerMock.Object, NullLogger<KafkaConsumerWorker>.Instance);
//
//         // Start and stop quickly — stoppingToken fires, which also cancels the per-message CTS
//         await worker.StartAsync(CancellationToken.None);
//         await Task.Delay(100, CancellationToken.None);
//         await worker.StopAsync(CancellationToken.None);
//
//         consumerMock.Verify(c => c.Commit(It.IsAny<ConsumeResult<string, byte[]>>()), Times.Never);
//     }
// }
