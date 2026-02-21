using System.Collections.Immutable;

namespace ProductService.Models;

// =============================================================================
// ProductDetails — immutable data model for the combined product message
// =============================================================================
// This is a C# record, which is immutable by default. Once created, a
// ProductDetails instance can never be modified — its properties are init-only.
//
// WHY IMMUTABLE:
//   The previous version used a mutable class with List<string> properties.
//   ProductDetailsService had to lock on the ProductDetails object whenever it
//   added a customer or serialized for publishing, because another thread could
//   be modifying the lists concurrently.
//
//   With immutable records, there is nothing to lock — you never modify an
//   existing instance. Instead, you create a NEW instance with the updated data
//   and atomically swap it into the ConcurrentDictionary using AddOrUpdate.
//   The old instance is untouched and eventually garbage collected.
//
// THE "with" EXPRESSION:
//   C# records support "with" expressions that create a copy with some
//   properties changed:
//     var updated = existing with { SalesCustomers = newList };
//   This doesn't mutate "existing" — it creates a brand new record.
//
// ImmutableList<string>:
//   We use ImmutableList instead of List to enforce immutability all the way
//   down. A regular List<string> inside a record would still be mutable
//   (the reference is immutable, but the list contents aren't). ImmutableList
//   returns a new list when you call .Add(), leaving the original unchanged.
// =============================================================================

public record ProductDetails(
    string ProductId,
    Product? Product,
    ImmutableList<string> SalesCustomers,
    ImmutableList<string> PurchasesCustomers)
{
    public static ProductDetails Empty(string productId) =>
        new(productId, null, ImmutableList<string>.Empty, ImmutableList<string>.Empty);
}
