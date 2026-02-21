namespace ProductService.Models;

// =============================================================================
// Product — immutable data model for product information
// =============================================================================
// Represents the core product data received from the products.updates topic.
// Immutable record — once created, cannot be modified. When a product update
// arrives, a new Product instance replaces the old one in ProductManager's cache.
// =============================================================================
public record Product(
    string Id,
    string Name,
    decimal Price,
    string Description,
    bool Availability);
