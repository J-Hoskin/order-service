using WellKnownTimestamp = Google.Protobuf.WellKnownTypes.Timestamp;
using ProductService.Extensions;
using ProductService.Proto;
using Xunit;

namespace ProductService.Tests.Extensions;

public class ProtobufExtensionsTests
{
    // ── OrderCreated ────────────────────────────────────────────────────────

    [Fact]
    public void OrderCreated_ToDomain_MapsCustomerIdAndItems()
    {
        var proto = new OrderCreated
        {
            CustomerId = "cust-1",
            Items =
            {
                new ProtoOrderItem { ProductId = "prod-1", ProductName = "Widget", Quantity = 3, Price = "9.99" }
            }
        };

        var domain = proto.ToDomain();

        Assert.Equal("cust-1", domain.CustomerId);
        Assert.Single(domain.Items);
        Assert.Equal("prod-1", domain.Items[0].ProductId);
        Assert.Equal("Widget", domain.Items[0].ProductName);
        Assert.Equal(3, domain.Items[0].Quantity);
        Assert.Equal(9.99m, domain.Items[0].Price);
    }

    [Fact]
    public void OrderCreated_ToDomain_EmptyItems()
    {
        var proto = new OrderCreated { CustomerId = "cust-2" };
        var domain = proto.ToDomain();
        Assert.Empty(domain.Items);
    }

    // ── ProtoOrderItem ──────────────────────────────────────────────────────

    [Fact]
    public void ProtoOrderItem_ToDomain_ParsesPriceString()
    {
        var proto = new ProtoOrderItem
        {
            ProductId = "p1", ProductName = "Gadget", Quantity = 1, Price = "19.95"
        };
        var item = proto.ToDomain();
        Assert.Equal(19.95m, item.Price);
    }

    [Fact]
    public void ProtoOrderItem_ToDomain_MapsAllFields()
    {
        var proto = new ProtoOrderItem
        {
            ProductId = "p2", ProductName = "Thingamajig", Quantity = 7, Price = "0.50"
        };
        var item = proto.ToDomain();
        Assert.Equal("p2", item.ProductId);
        Assert.Equal("Thingamajig", item.ProductName);
        Assert.Equal(7, item.Quantity);
        Assert.Equal(0.50m, item.Price);
    }

    // ── PaymentConfirmed ────────────────────────────────────────────────────

    [Fact]
    public void PaymentConfirmed_ToDomain_ReturnsPaymentReference()
    {
        var proto = new PaymentConfirmed { PaymentReference = "PAY-123" };
        Assert.Equal("PAY-123", proto.ToDomain());
    }

    // ── WarehousePicked ─────────────────────────────────────────────────────

    [Fact]
    public void WarehousePicked_ToDomain_ReturnsPickedBy()
    {
        var proto = new WarehousePicked { PickedBy = "Alice" };
        Assert.Equal("Alice", proto.ToDomain());
    }

    // ── PauseOrder ──────────────────────────────────────────────────────────

    [Fact]
    public void PauseOrder_ToDomain_ConvertsTimestampToDateTime()
    {
        var dt = new DateTime(2024, 6, 1, 12, 0, 0, DateTimeKind.Utc);
        var proto = new PauseOrder { CreatedAt = WellKnownTimestamp.FromDateTime(dt), Reason = "stock issue" };
        var domain = proto.ToDomain();
        Assert.Equal(dt, domain.CreatedAt);
        Assert.Equal("stock issue", domain.Reason);
    }

    [Fact]
    public void PauseOrder_ToDomain_EmptyReason_ReturnsNull()
    {
        var proto = new PauseOrder { CreatedAt = WellKnownTimestamp.FromDateTime(DateTime.UtcNow), Reason = "" };
        Assert.Null(proto.ToDomain().Reason);
    }

    [Fact]
    public void PauseOrder_ToDomain_NonEmptyReason_Preserved()
    {
        var proto = new PauseOrder { CreatedAt = WellKnownTimestamp.FromDateTime(DateTime.UtcNow), Reason = "out of stock" };
        Assert.Equal("out of stock", proto.ToDomain().Reason);
    }

    // ── ConfirmPause ────────────────────────────────────────────────────────

    [Fact]
    public void ConfirmPause_ToDomain_ConvertsTimestamp()
    {
        var dt = new DateTime(2024, 7, 15, 8, 30, 0, DateTimeKind.Utc);
        var proto = new ConfirmPause { CreatedAt = WellKnownTimestamp.FromDateTime(dt), ConfirmedBy = "Bob" };
        var domain = proto.ToDomain();
        Assert.Equal(dt, domain.CreatedAt);
        Assert.Equal("Bob", domain.ConfirmedBy);
    }

    [Fact]
    public void ConfirmPause_ToDomain_EmptyConfirmedBy_ReturnsNull()
    {
        var proto = new ConfirmPause { CreatedAt = WellKnownTimestamp.FromDateTime(DateTime.UtcNow), ConfirmedBy = "" };
        Assert.Null(proto.ToDomain().ConfirmedBy);
    }

    [Fact]
    public void ConfirmPause_ToDomain_NonEmptyConfirmedBy_Preserved()
    {
        var proto = new ConfirmPause { CreatedAt = WellKnownTimestamp.FromDateTime(DateTime.UtcNow), ConfirmedBy = "Carol" };
        Assert.Equal("Carol", proto.ToDomain().ConfirmedBy);
    }
}
