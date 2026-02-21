using ProductService.Models;
using ProductService.Proto;

namespace ProductService.Extensions;

public static class ConfirmPauseExtensions
{
    public static ConfirmPausePayload ToDomain(this ConfirmPause proto) =>
        new(proto.CreatedAt.ToDateTime(), proto.ConfirmedBy == "" ? null : proto.ConfirmedBy);
}
