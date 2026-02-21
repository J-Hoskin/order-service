using Confluent.Kafka;
using KafkaInfrastructure.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaInfrastructure.Messaging;

// =============================================================================
// KafkaConsumerWorker — hosted BackgroundService that runs the consume loop
// =============================================================================
// This is the entry point for all incoming Kafka messages. It extends
// BackgroundService, which is an abstract class implementing IHostedService.
// The .NET host calls StartAsync on app startup, which kicks off ExecuteAsync
// as a long-running background task.
//
// LIFETIME: Singleton (registered as IHostedService)
//
// MANUAL OFFSET COMMIT — AT-LEAST-ONCE DELIVERY:
//   The main consumer is configured with EnableAutoCommit = false.
//   This worker calls consumer.Commit(result) AFTER RouteAsync returns, which
//   means the offset is only saved to Kafka once all processing has fully completed.
//   If the instance dies mid-processing, Kafka reprocesses the message from the
//   last committed offset — at-least-once delivery.
// =============================================================================
public class KafkaConsumerWorker : BackgroundService
{
    private readonly MessageRouter _router;
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly ILogger<KafkaConsumerWorker> _logger;
    private readonly KafkaOptions _options;

    public KafkaConsumerWorker(
        MessageRouter router,
        IConsumer<string, byte[]> consumer,
        ILogger<KafkaConsumerWorker> logger,
        KafkaOptions options)
    {
        _router = router;
        _consumer = consumer;
        _logger = logger;
        _options = options;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_options.Topics);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(stoppingToken);

                using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                cts.CancelAfter(TimeSpan.FromSeconds(30));

                await _router.RouteAsync(result, cts.Token);

                _consumer.Commit(result);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Consume error");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unhandled error processing message");
            }
        }

        _consumer.Close();
    }
}
