using System.Collections.Immutable;

namespace ProductService.Models;

public record OrderCreatedPayload(
    string CustomerId,
    ImmutableList<OrderItem> Items);
