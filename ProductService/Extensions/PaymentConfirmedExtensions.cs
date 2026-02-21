using ProductService.Proto;

namespace ProductService.Extensions;

public static class PaymentConfirmedExtensions
{
    public static string ToDomain(this PaymentConfirmed proto) => proto.PaymentReference;
}
