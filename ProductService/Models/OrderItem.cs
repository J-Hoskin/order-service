namespace ProductService.Models;

public record OrderItem(
    string ProductId,
    string ProductName,
    int Quantity,
    decimal Price);
