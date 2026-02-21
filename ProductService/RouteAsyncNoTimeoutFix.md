Plan: Add per-message processing timeout (Problem 6)

     Context

     KafkaConsumerWorker awaits _router.RouteAsync(result) with no deadline. If a processor hangs
     (e.g. KafkaProducer.ProduceAsync waiting on an unreachable broker), the consume loop freezes
     indefinitely. After ~30 s the Kafka broker declares the consumer dead, triggers a rebalance, and
     another instance re-processes the same message — causing duplicate processing on top of the hang.

     The fix threads a per-message CancellationToken (with a 30-second deadline) through the full
     call chain: KafkaConsumerWorker → MessageRouter → IMessageProcessor → ProductDetailsService
     / ProductManager → KafkaProducer. The token is linked to stoppingToken so graceful shutdown
     still works. KafkaProducer.ProduceAsync already accepts a CancellationToken in the Confluent
     API, so cancellation propagates all the way to the network layer.

     Files to modify

     1. ProductService/Interfaces/IMessageProcessor.cs
     2. ProductService/Messaging/MessageRouter.cs
     3. ProductService/Messaging/KafkaConsumerWorker.cs
     4. ProductService/Messaging/KafkaProducer.cs
     5. ProductService/Services/ProductDetailsService.cs
     6. ProductService/Services/ProductManager.cs
     7. ProductService/Processors/SalesProcessor.cs
     8. ProductService/Processors/PurchasesProcessor.cs
     9. ProductService/Processors/ProductUpdatesProcessor.cs

     Changes

     1. IMessageProcessor.cs

     Add CancellationToken to the contract:

     // cancellationToken is cancelled if the per-message deadline fires or the host
     // is shutting down. Implementations must forward it to all async operations so
     // the processor can be aborted promptly in either case.
     Task ProcessAsync(ConsumeResult<string, string> message, CancellationToken cancellationToken);

     2. MessageRouter.cs

     Accept and forward the token:

     public async Task RouteAsync(ConsumeResult<string, string> message, CancellationToken cancellationToken)
     {
         ...
         await processor.ProcessAsync(message, cancellationToken);
     }

     3. KafkaConsumerWorker.cs

     Create a per-message deadline token and fix the OperationCanceledException catch so it only
     breaks on shutdown — not on a per-message timeout:

     var result = _consumer.Consume(stoppingToken);

     // Create a per-message timeout linked to the shutdown token.
     // If processing exceeds 30 seconds, cts fires and cancels any awaitable in the
     // processor chain. The message is skipped and the loop continues.
     // If the host is shutting down, stoppingToken fires and cts fires too — the
     // catch below breaks the loop cleanly.
     using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
     cts.CancelAfter(TimeSpan.FromSeconds(30));

     await _router.RouteAsync(result, cts.Token);

     Change the OperationCanceledException catch to use a when guard:

     catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
     {
         // Host is shutting down — exit the loop cleanly.
         break;
     }
     // A per-message timeout expiry also throws OperationCanceledException, but
     // stoppingToken.IsCancellationRequested is false, so it falls through to here.
     catch (Exception ex)
     {
         _logger.LogError(ex, "Unhandled error processing message");
     }

     4. KafkaProducer.cs

     Add CancellationToken parameter and forward to Confluent's API (which already accepts one):

     public async Task ProduceAsync(string topic, string key, string value,
         CancellationToken cancellationToken = default)
     {
         try
         {
             await _producer.ProduceAsync(topic, new Message<string, string>
             {
                 Key = key,
                 Value = value
             }, cancellationToken);
         }
         catch (ProduceException<string, string> ex)
         {
             throw new Exception($"Failed to produce message to {topic}: [{ex.Error.Code}] {ex.Error.Reason}", ex);
         }
     }

     default keeps the existing call site in Dispose/tests working without a token.

     5. ProductDetailsService.cs

     Add CancellationToken cancellationToken = default to AddSaleAsync, AddPurchaseAsync, and
     UpdateProductAsync, forwarding it to _producer.ProduceAsync via PublishAsync:

     public async Task AddSaleAsync(string productId, string customerId,
         CancellationToken cancellationToken = default)
     {
         ...
         await PublishAsync(updated, cancellationToken);
     }

     private async Task PublishAsync(ProductDetails details, CancellationToken cancellationToken = default)
     {
         var value = JsonSerializer.Serialize(details);
         await _producer.ProduceAsync("products.details", details.ProductId, value, cancellationToken);
     }

     6. ProductManager.cs

     Add CancellationToken cancellationToken = default to UpdateAsync and forward to
     ProductDetailsService.UpdateProductAsync:

     public async Task UpdateAsync(string productId, string json,
         CancellationToken cancellationToken = default)
     {
         ...
         await _productDetailsService.UpdateProductAsync(productId, product, cancellationToken);
     }

     7–9. Processors

     Each processor passes cancellationToken through to its service call:

     // SalesProcessor
     public async Task ProcessAsync(ConsumeResult<string, string> message, CancellationToken cancellationToken)
     {
         ...
         await _productDetails.AddSaleAsync(productId, customerId, cancellationToken);
     }

     Same pattern for PurchasesProcessor (AddPurchaseAsync) and ProductUpdatesProcessor
     (UpdateAsync).

     Verification

     Run the service locally. Block port 9092 mid-run to simulate a hung broker. After 30 seconds the
     worker should log an OperationCanceledException error for that message and continue to the next
     one rather than hanging indefinitely.




Clean build. Here's a summary of what was changed and why:

  ---
IMessageProcessor — added CancellationToken cancellationToken to the contract. All processors must now accept and forward it.

KafkaConsumerWorker — creates a CancellationTokenSource per message, linked to stoppingToken and set to cancel after 30 seconds. The
OperationCanceledException catch now has a when (stoppingToken.IsCancellationRequested) guard so only a genuine shutdown breaks the loop — a per-message
timeout expiry falls through to the general catch (Exception ex) block, logs the error, and the loop continues to the next message.

MessageRouter — accepts and forwards the token to processor.ProcessAsync.

KafkaProducer — forwards the token to Confluent's ProduceAsync, which respects it at the network level. Uses default so any call sites without a token
still compile.

ProductDetailsService / ProductManager — each async method gains CancellationToken cancellationToken = default and passes it down the chain to
PublishAsync → KafkaProducer.ProduceAsync.

All three processors — pass cancellationToken through to their respective service calls.