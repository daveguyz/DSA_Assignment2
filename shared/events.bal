// =====================================================================
//  NamDeliver - shared EVENT CONTRACT
//  ---------------------------------------------------------------------
//  This file is the single source of truth for every Kafka topic name and
//  event payload used on the platform. It is copied verbatim into each
//  microservice by scripts/sync-shared.sh so that every service compiles
//  independently (no shared runtime library = no deployment coupling).
//
//  Conventions
//   * Every event carries EventMeta (eventId is used for idempotency).
//   * Records are OPEN (`record { ... }`) - consumers follow the
//     "tolerant reader" pattern and ignore fields they do not know.
//   * Message KEY = orderId (driverId for location updates) so that all
//     events of one order land on the same partition and stay ordered.
// =====================================================================

// ---------- Topic names -------------------------------------------------
const TOPIC_ORDERS_CREATED = "orders.created";
const TOPIC_ORDERS_VALIDATED = "orders.validated";
const TOPIC_ORDERS_REJECTED = "orders.rejected";
const TOPIC_ORDERS_CONFIRMED = "orders.confirmed";
const TOPIC_ORDERS_CANCELLED = "orders.cancelled";
const TOPIC_ORDER_STATUS = "orders.status.changed";
const TOPIC_PAYMENTS_REQUESTED = "payments.requested";
const TOPIC_PAYMENTS_COMPLETED = "payments.completed";
const TOPIC_PAYMENTS_FAILED = "payments.failed";
const TOPIC_PAYMENTS_REFUNDED = "payments.refunded";
const TOPIC_KITCHEN_PREPARING = "kitchen.preparing";
const TOPIC_KITCHEN_READY = "kitchen.ready";
const TOPIC_DELIVERY_ASSIGNED = "delivery.assigned";
const TOPIC_DELIVERY_PICKED_UP = "delivery.picked_up";
const TOPIC_DELIVERY_COMPLETED = "delivery.completed";
const TOPIC_DRIVER_LOCATION = "delivery.location.updated";
const TOPIC_DLQ = "dlq.events";

// ---------- Order life-cycle states ------------------------------------
const CREATED = "CREATED";
const CONFIRMED = "CONFIRMED";
const PREPARING = "PREPARING";
const READY = "READY";
const OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY";
const DELIVERED = "DELIVERED";
const CANCELLED = "CANCELLED";

// ---------- Common envelope ---------------------------------------------
type EventMeta record {|
    string eventId;
    string eventType;
    string occurredAt;
    string 'source;
|};

type OrderItemRequest record {
    int menuItemId;
    int quantity;
};

type PricedItem record {
    int menuItemId;
    string name;
    int quantity;
    decimal unitPrice;
};

// orders.created  (Order Service -> Restaurant, Customer, Admin)
type OrderCreatedEvent record {
    *EventMeta;
    string orderId;
    int customerId;
    int restaurantId;
    OrderItemRequest[] items;
    string paymentMethod;
    string deliveryAddress;
    float deliveryLat;
    float deliveryLng;
};

// orders.validated  (Restaurant -> Order) : stock reserved, prices attached
type OrderValidatedEvent record {
    *EventMeta;
    string orderId;
    int restaurantId;
    string restaurantName;
    float restaurantLat;
    float restaurantLng;
    PricedItem[] items;
    decimal subtotal;
};

// orders.rejected  (Restaurant -> Order) : closed / out of stock / unknown item
type OrderRejectedEvent record {
    *EventMeta;
    string orderId;
    int restaurantId;
    string reason;
};

// payments.requested  (Order -> Payment)
type PaymentRequestedEvent record {
    *EventMeta;
    string orderId;
    int customerId;
    decimal amount;
    string paymentMethod;
};

// payments.completed | payments.failed | payments.refunded  (Payment -> *)
type PaymentResultEvent record {
    *EventMeta;
    string orderId;
    string paymentId;
    int customerId;
    decimal amount;
    string paymentMethod;
    string status;
    string? reason;
    string? transactionRef;
};

// orders.confirmed  (Order -> Restaurant, Delivery, Notification)
type OrderConfirmedEvent record {
    *EventMeta;
    string orderId;
    int customerId;
    int restaurantId;
    string restaurantName;
    float restaurantLat;
    float restaurantLng;
    string deliveryAddress;
    float deliveryLat;
    float deliveryLng;
    decimal total;
    PricedItem[] items;
};

// orders.cancelled  (Order -> Restaurant [restock], Payment [refund], Delivery)
type OrderCancelledEvent record {
    *EventMeta;
    string orderId;
    int customerId;
    int restaurantId;
    string previousStatus;
    string reason;
};

// orders.status.changed  (Order -> Customer, Delivery, Notification, Admin)
type OrderStatusChangedEvent record {
    *EventMeta;
    string orderId;
    int customerId;
    int restaurantId;
    string? restaurantName;
    int? driverId;
    string? fromStatus;
    string toStatus;
    string? reason;
    decimal total;
};

// kitchen.preparing | kitchen.ready  (Restaurant -> Order)
type KitchenEvent record {
    *EventMeta;
    string orderId;
    int restaurantId;
    string kitchenStatus;
};

// delivery.assigned | delivery.picked_up | delivery.completed  (Delivery -> *)
type DeliveryEvent record {
    *EventMeta;
    string orderId;
    string deliveryId;
    int customerId;
    int restaurantId;
    int? driverId;
    string? driverName;
    string deliveryStatus;
    float? distanceKm;
    int? etaMinutes;
};

// delivery.location.updated  (Delivery -> UI / analytics)  key = driverId
type DriverLocationEvent record {
    *EventMeta;
    int driverId;
    string? orderId;
    float latitude;
    float longitude;
    string phase;
};

// dlq.events  (any service) - poison messages after retries are exhausted
type DeadLetterEvent record {
    *EventMeta;
    string originalTopic;
    string failedBy;
    string errorMessage;
    string rawPayload;
};
