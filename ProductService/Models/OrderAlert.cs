namespace ProductService.Models;

// =============================================================================
// OrderAlert — payload published to orders.alerts
// =============================================================================
// Represents a notable state transition on an order that downstream consumers
// (notifications, audit, analytics) should be informed about.
//
// Published to: orders.alerts
// Message key:  OrderId (so all alerts for a given order go to the same partition,
//               preserving ordering for downstream consumers)
//
// OccurredAt is set to UTC now at the moment the alert is produced, not the
// CreatedAt from the source message. This records when the transition was
// confirmed by the service, not when the upstream system requested it.
// =============================================================================
public record OrderAlert(string OrderId, string Event, DateTime OccurredAt);
