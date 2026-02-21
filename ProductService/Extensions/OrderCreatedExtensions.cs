using System.Collections.Immutable;
using ProductService.Models;
using ProductService.Proto;

namespace ProductService.Extensions;

public static class OrderCreatedExtensions
{
    public static OrderCreatedPayload ToDomain(this OrderCreated proto) =>
        new(proto.CustomerId,
            proto.Items.Select(i => i.ToDomain()).ToImmutableList());

    public static OrderItem ToDomain(this ProtoOrderItem proto) =>
        new(proto.ProductId, proto.ProductName, proto.Quantity,
            decimal.Parse(proto.Price));
}
