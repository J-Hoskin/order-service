# Product Service

A C# Kafka microservice that consumes product updates, sales, and purchase events, aggregates them per product, and publishes combined product details.

## Architecture

### Message Flow

```
products.updates topic →  ProductUpdatesProcessor →  ProductManager.UpdateAsync()
                                                            ↓ (caches product, then calls)
                                                     ProductDetailsService.UpdateProductAsync()

products.sales topic   →  SalesProcessor           →  ProductDetailsService.AddSaleAsync()

products.purchases     →  PurchasesProcessor        →  ProductDetailsService.AddPurchaseAsync()

All three paths publish combined ProductDetails to products.details topic (key = product ID)
ProductDetails includes: Product info + SalesCustomers + PurchasesCustomers
```

### Components

| Component | DI Lifetime | Role |
|---|---|---|
| `KafkaConsumerWorker` | Singleton (HostedService) | Consume loop — polls Kafka, delegates to MessageRouter |
| `MessageRouter` | Singleton | Creates a DI child scope per message, resolves keyed processor |
| `ProductUpdatesProcessor` | Scoped (keyed: "products.updates") | Handles product update events, delegates to ProductManager |
| `SalesProcessor` | Scoped (keyed: "products.sales") | Handles sale events |
| `PurchasesProcessor` | Scoped (keyed: "products.purchases") | Handles purchase events |
| `ProductManager` | Singleton | Caches Product models in-memory, triggers republish on update |
| `ProductDetailsService` | Singleton | Aggregates product info + sales + purchases, publishes combined message |
| `KafkaProducer` | Singleton | Wrapper around Confluent.Kafka IProducer |
| `KafkaGlobalTable` | Singleton (HostedService) | Consumes a compacted topic into an in-memory lookup table |

### Scope-Per-Message Pattern

There is no automatic DI scope in a `BackgroundService` (unlike ASP.NET where each HTTP request creates one). The `MessageRouter` creates a child scope for every Kafka message:

1. `KafkaConsumerWorker` consumes a message and calls `MessageRouter.RouteAsync()`
2. `MessageRouter` creates a child scope via `ILifetimeScope.BeginLifetimeScope()`
3. The correct `IMessageProcessor` is resolved from the child scope using the topic name as a key
4. `ProcessAsync` runs — the processor and its scoped dependencies live inside this child scope
5. The child scope is disposed — the processor and all scoped dependencies are cleaned up

Scoped processors can depend on singleton services (e.g., `ProductDetailsService`), but not the reverse.

## High Availability

### Strategy: Co-Partitioned Topics (Active-Active with Partitioned Ownership)

This service runs active-active at the **service level** — all instances are running and processing simultaneously. However, at the **product level**, each product is owned by exactly one instance at a time. This is active-active with partitioned ownership, not true active-active (where any instance can handle any product at any time).

Multiple instances share the same consumer group, and Kafka distributes partitions across them. When an instance fails, Kafka rebalances its partitions to surviving instances. There is a brief consumption pause during rebalancing for the affected partitions.

### Why Co-Partitioning

`ProductDetailsService` holds in-memory state that aggregates both sales and purchases per product. For the combined message to be correct, a single instance must see **all** sales and purchases for a given product.

Co-partitioning guarantees this: when `products.sales`, `products.purchases`, and `products.updates` have the same partition count and the same key (product ID), Kafka's default partitioner hashes the key identically for all topics. Partition N of each topic contains the same set of product IDs. Since all topics are in the same `Subscribe()` call and consumer group, Kafka assigns matching partition numbers to the same instance.

```
Instance A: partition 0 of products.sales + partition 0 of products.purchases + partition 0 of products.updates
Instance B: partition 1 of products.sales + partition 1 of products.purchases + partition 1 of products.updates
```

All events for a product land on the same instance. The in-memory state is complete per-instance.

### Co-Partitioning Requirements

These are **mandatory** for correctness. Violating them will cause instances to produce incomplete product details.

1. **`products.sales`, `products.purchases`, and `products.updates` must have the same number of partitions.** If you change one, you must change the others.
2. **All topics must use product ID as the message key.** The default partitioner hashes the key to assign partitions — same key means same partition number.
3. **All topics must use the same partitioner.** Use Kafka's default (murmur2) for all. Do not override the partitioner on producers for any topic.

### Alternatives Considered

**Externalized state (Redis/database):** All instances read/write shared external state. Rejected because it adds a network dependency on every message and a new infrastructure component to manage, when co-partitioning solves the problem with zero code changes and zero additional infrastructure.

**KafkaGlobalTable for product details:** Each instance publishes partial updates and consumes the full topic to rebuild complete state. Rejected because it introduces eventual consistency (stale reads during propagation), race conditions on concurrent merges, amplified Kafka I/O (every update consumed by every instance), and full dataset memory usage per instance. Co-partitioning gives immediate consistency with simpler logic.

### Scaling

Set the partition count equal to your expected maximum instance count. Each partition can be consumed by at most one instance, so extra instances beyond the partition count will be idle. Partitions can be added later but **cannot be removed** — avoid over-provisioning.

### Consumer Count vs Partition Count

Having fewer consumers than partitions is valid — a single consumer will read all assigned partitions, polling them round-robin. This is the standard scale-out model: partitions represent reserved capacity that becomes useful as you add instances.

Having more consumers than partitions means the excess consumers sit idle, receiving no partition assignment. This is wasteful but not harmful.

**Example:** 1 instance with 3 partitions — the single instance owns all 3 partitions and processes all messages. Throughput is limited to that one instance, but you can scale to 3 instances without any topic reconfiguration.
