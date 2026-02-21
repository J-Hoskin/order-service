using ProductService.Proto;

namespace ProductService.Extensions;

public static class WarehousePickedExtensions
{
    public static string ToDomain(this WarehousePicked proto) => proto.PickedBy;
}
