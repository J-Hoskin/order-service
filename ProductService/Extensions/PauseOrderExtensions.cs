using ProductService.Models;
using ProductService.Proto;

namespace ProductService.Extensions;

public static class PauseOrderExtensions
{
    public static PauseOrderPayload ToDomain(this PauseOrder proto) =>
        new(proto.CreatedAt.ToDateTime(), proto.Reason == "" ? null : proto.Reason);
}
