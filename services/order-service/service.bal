// =====================================================================
//  Order Service - owner of the central ORDER STATE MACHINE
//
//    CREATED -> CONFIRMED -> PREPARING -> READY -> OUT_FOR_DELIVERY -> DELIVERED
//       |           |
//       +-----------+--------> CANCELLED   (customer may cancel until the kitchen starts)
//
//  Every transition is validated against TRANSITIONS, persisted with an
//  optimistic guard (UPDATE ... WHERE status = <expected>), written to the
//  status history table and broadcast on orders.status.changed.
//
//  REST  : /orders/**, /pricing/surge
//  Kafka : consumes orders.validated, orders.rejected, payments.completed,
//          payments.failed, kitchen.preparing, kitchen.ready,
//          delivery.assigned, delivery.picked_up, delivery.completed
//          produces orders.created, payments.requested, orders.confirmed,
//          orders.cancelled, orders.status.changed
// =====================================================================
import ballerina/http;
import ballerina/log;
import ballerina/sql;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;

const SERVICE_NAME = "order-service";

configurable int httpPort = 9093;
configurable string dbName = "order_db";
configurable string dbUser = "order_svc";
configurable string dbPassword = "order_pass";
configurable string customerServiceUrl = "http://localhost:9091";
configurable string deliveryServiceUrl = "http://localhost:9095";
configurable decimal baseDeliveryFee = 15.00;
configurable decimal perKmFee = 5.00;
configurable decimal maxSurgeMultiplier = 2.5;
configurable int utcOffsetHours = 2;
configurable int maxItemsPerLine = 20;

final http:Client customerClient = check new (customerServiceUrl, {timeout: 3});
final http:Client deliveryClient = check new (deliveryServiceUrl, {timeout: 2});

// ---------- State machine ---------------------------------------------------
final readonly & map<string[]> TRANSITIONS = {
    "CREATED": [CONFIRMED, CANCELLED],
    "CONFIRMED": [PREPARING, CANCELLED],
    "PREPARING": [READY],
    "READY": [OUT_FOR_DELIVERY],
    "OUT_FOR_DELIVERY": [DELIVERED],
    "DELIVERED": [],
    "CANCELLED": []
};

isolated function canTransition(string fromStatus, string toStatus) returns boolean {
    string[]? allowed = TRANSITIONS[fromStatus];
    return allowed is string[] && allowed.indexOf(toStatus) !is ();
}

// ---------- Types ----------------------------------------------------------
type OrderInput record {|
    int customerId;
    int restaurantId;
    int addressId;
    OrderItemRequest[] items;
    string paymentMethod = "CARD";
    string? notes = ();
|};

type CancelInput record {|
    string reason = "Cancelled by customer";
|};

type CustomerAddress record {
    int id;
    int customerId;
    string label;
    string street;
    string suburb;
    string city;
    float latitude;
    float longitude;
};

type OrderRow record {|
    string id;
    int customerId;
    int restaurantId;
    string? restaurantName;
    string status;
    string paymentMethod;
    decimal subtotal;
    decimal deliveryFee;
    decimal surgeMultiplier;
    decimal total;
    string deliveryAddress;
    float deliveryLat;
    float deliveryLng;
    float? restaurantLat;
    float? restaurantLng;
    int? driverId;
    string? notes;
    string? cancelReason;
    string createdAt;
    string updatedAt;
|};

type OrderItemRow record {|
    int menuItemId;
    string? name;
    int quantity;
    decimal unitPrice;
|};

type StatusHistoryRow record {|
    string? fromStatus;
    string toStatus;
    string? reason;
    string changedAt;
|};

type OrderDetails record {|
    *OrderRow;
    OrderItemRow[] items;
    StatusHistoryRow[] history;
|};

type DriverStats record {
    int available;
    int busy;
    int offline;
    int pendingDeliveries;
};

type SurgeInfo record {|
    decimal multiplier;
    int activeOrders;
    int availableDrivers;
    int pendingDeliveries;
    boolean peakHour;
    string reason;
|};

isolated function orderSelect() returns sql:ParameterizedQuery =>
    `SELECT id, customer_id AS customerId, restaurant_id AS restaurantId, restaurant_name AS restaurantName,
            status, payment_method AS paymentMethod, subtotal, delivery_fee AS deliveryFee,
            surge_multiplier AS surgeMultiplier, total, delivery_address AS deliveryAddress,
            delivery_lat AS deliveryLat, delivery_lng AS deliveryLng,
            restaurant_lat AS restaurantLat, restaurant_lng AS restaurantLng, driver_id AS driverId,
            notes, cancel_reason AS cancelReason,
            DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ') AS createdAt,
            DATE_FORMAT(updated_at, '%Y-%m-%dT%H:%i:%sZ') AS updatedAt
       FROM orders`;

// ---------- REST API --------------------------------------------------------
@http:ServiceConfig {
    cors: {allowOrigins: ["*"], allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"]}
}
isolated service /orders on httpListener {

    // Place an order. Returns 202 Accepted: the rest of the life-cycle is
    // driven asynchronously by Kafka events.
    isolated resource function post .(OrderInput input) returns http:Accepted|http:BadRequest|http:ServiceUnavailable|error {
        string? problem = validateOrderInput(input);
        if problem is string {
            return errBadRequest(problem);
        }
        CustomerAddress|error addr = customerClient->get(string `/customers/${input.customerId}/addresses/${input.addressId}`);
        if addr is http:ClientRequestError {
            return errBadRequest(string `Unknown customer ${input.customerId} or address ${input.addressId}`);
        }
        if addr is error {
            log:printError("customer-service unreachable", 'error = addr);
            return <http:ServiceUnavailable>{body: <ErrorBody>{message: "Customer service unavailable, please retry"}};
        }
        string orderId = uuid:createType4AsString();
        string address = string `${addr.street}, ${addr.suburb}, ${addr.city}`;
        transaction {
            _ = check db->execute(`
                INSERT INTO orders (id, customer_id, restaurant_id, status, payment_method, delivery_address,
                                    delivery_lat, delivery_lng, notes)
                VALUES (${orderId}, ${input.customerId}, ${input.restaurantId}, ${CREATED}, ${input.paymentMethod},
                        ${address}, ${addr.latitude}, ${addr.longitude}, ${input.notes})`);
            foreach OrderItemRequest item in input.items {
                _ = check db->execute(`
                    INSERT INTO order_items (order_id, menu_item_id, quantity)
                    VALUES (${orderId}, ${item.menuItemId}, ${item.quantity})`);
            }
            _ = check db->execute(`
                INSERT INTO order_status_history (order_id, from_status, to_status, reason)
                VALUES (${orderId}, NULL, ${CREATED}, 'Order placed')`);
            OrderCreatedEvent created = {
                ...newMeta("OrderCreated"),
                orderId,
                customerId: input.customerId,
                restaurantId: input.restaurantId,
                items: input.items,
                paymentMethod: input.paymentMethod,
                deliveryAddress: address,
                deliveryLat: addr.latitude,
                deliveryLng: addr.longitude
            };
            check publish(TOPIC_ORDERS_CREATED, orderId, created);
            check commit;
        }
        OrderRow? row = check findOrder(orderId);
        if row is OrderRow {
            check emitStatusChanged(row, (), CREATED, "Order placed");
        }
        log:printInfo("order placed", orderId = orderId, customerId = input.customerId, restaurantId = input.restaurantId);
        return <http:Accepted>{body: check getDetails(orderId)};
    }

    isolated resource function get .(int? customerId, int? restaurantId, int? driverId, string? status, int 'limit = 50)
            returns OrderRow[]|error {
        sql:ParameterizedQuery q = sql:queryConcat(orderSelect(), ` WHERE 1 = 1`);
        if customerId is int {
            q = sql:queryConcat(q, ` AND customer_id = ${customerId}`);
        }
        if restaurantId is int {
            q = sql:queryConcat(q, ` AND restaurant_id = ${restaurantId}`);
        }
        if driverId is int {
            q = sql:queryConcat(q, ` AND driver_id = ${driverId}`);
        }
        if status is string {
            q = sql:queryConcat(q, ` AND status = ${status}`);
        }
        int lim = 'limit < 1 || 'limit > 500 ? 50 : 'limit;
        stream<OrderRow, sql:Error?> rows = db->query(sql:queryConcat(q, ` ORDER BY created_at DESC LIMIT ${lim}`));
        return from OrderRow o in rows select o;
    }

    isolated resource function get [string id]() returns OrderDetails|http:NotFound|error {
        OrderDetails|error d = getDetails(id);
        if d is sql:NoRowsError {
            return errNotFound(string `Order ${id} not found`);
        }
        return d;
    }

    // Customer cancellation is allowed until the kitchen starts preparing.
    isolated resource function post [string id]/cancel(CancelInput? input) returns OrderDetails|http:NotFound|http:Conflict|error {
        OrderRow? o = check findOrder(id);
        if o is () {
            return errNotFound(string `Order ${id} not found`);
        }
        if o.status != CREATED && o.status != CONFIRMED {
            return errConflict(string `Order is ${o.status} and can no longer be cancelled`);
        }
        string reason = input is CancelInput ? input.reason : "Cancelled by customer";
        _ = check transition(id, CANCELLED, reason);
        return getDetails(id);
    }
}

@http:ServiceConfig {
    cors: {allowOrigins: ["*"]}
}
isolated service /pricing on httpListener {
    // Current surge multiplier (Bonus: dynamic surge pricing)
    isolated resource function get surge() returns SurgeInfo {
        return computeSurge();
    }
}

// ---------- Persistence helpers ------------------------------------------------
isolated function findOrder(string id) returns OrderRow?|error {
    OrderRow|sql:Error row = db->queryRow(sql:queryConcat(orderSelect(), ` WHERE id = ${id}`));
    if row is sql:NoRowsError {
        return ();
    }
    return row;
}

isolated function getDetails(string id) returns OrderDetails|error {
    OrderRow o = check db->queryRow(sql:queryConcat(orderSelect(), ` WHERE id = ${id}`));
    stream<OrderItemRow, sql:Error?> itemRows = db->query(`
        SELECT menu_item_id AS menuItemId, name, quantity, unit_price AS unitPrice
          FROM order_items WHERE order_id = ${id} ORDER BY id`);
    OrderItemRow[] items = check from OrderItemRow i in itemRows select i;
    stream<StatusHistoryRow, sql:Error?> histRows = db->query(`
        SELECT from_status AS fromStatus, to_status AS toStatus, reason,
               DATE_FORMAT(changed_at, '%Y-%m-%dT%H:%i:%s.%fZ') AS changedAt
          FROM order_status_history WHERE order_id = ${id} ORDER BY id`);
    StatusHistoryRow[] history = check from StatusHistoryRow h in histRows select h;
    return {...o, items, history};
}

isolated function validateOrderInput(OrderInput input) returns string? {
    if input.items.length() == 0 {
        return "an order needs at least one item";
    }
    foreach OrderItemRequest i in input.items {
        if i.quantity < 1 || i.quantity > maxItemsPerLine {
            return string `quantity must be between 1 and ${maxItemsPerLine}`;
        }
    }
    if input.paymentMethod != "CARD" && input.paymentMethod != "WALLET" && input.paymentMethod != "CASH" {
        return "paymentMethod must be CARD, WALLET or CASH";
    }
    return ();
}

// ---------- The state machine engine ---------------------------------------------
// Returns the updated order, or () if the transition was ignored
// (unknown order, duplicate or illegal transition).
isolated function transition(string orderId, string toStatus, string? reason = ()) returns OrderRow?|error {
    OrderRow? found = check findOrder(orderId);
    if found is () {
        log:printWarn("transition requested for unknown order", orderId = orderId, toStatus = toStatus);
        return ();
    }
    OrderRow o = found;
    string fromStatus = o.status;
    if fromStatus == toStatus {
        return (); // idempotent replay
    }
    if !canTransition(fromStatus, toStatus) {
        log:printWarn("illegal transition ignored", orderId = orderId, fromStatus = fromStatus, toStatus = toStatus);
        return ();
    }
    transaction {
        sql:ExecutionResult res = check db->execute(`
            UPDATE orders SET status = ${toStatus},
                   cancel_reason = IF(${toStatus} = 'CANCELLED', ${reason}, cancel_reason)
             WHERE id = ${orderId} AND status = ${fromStatus}`);
        if res.affectedRowCount == 0 {
            // someone else moved the order - the consumer retry will re-evaluate
            fail error(string `Optimistic lock failed for order ${orderId}`);
        }
        _ = check db->execute(`
            INSERT INTO order_status_history (order_id, from_status, to_status, reason)
            VALUES (${orderId}, ${fromStatus}, ${toStatus}, ${reason})`);
        check commit;
    }
    o.status = toStatus;
    if toStatus == CANCELLED {
        o.cancelReason = reason;
    }
    log:printInfo("order transitioned", orderId = orderId, fromStatus = fromStatus, toStatus = toStatus);
    check emitStatusChanged(o, fromStatus, toStatus, reason);

    if toStatus == CONFIRMED {
        OrderDetails d = check getDetails(orderId);
        PricedItem[] items = from OrderItemRow i in d.items
            select {menuItemId: i.menuItemId, name: i.name ?: "", quantity: i.quantity, unitPrice: i.unitPrice};
        OrderConfirmedEvent confirmed = {
            ...newMeta("OrderConfirmed"),
            orderId,
            customerId: o.customerId,
            restaurantId: o.restaurantId,
            restaurantName: o.restaurantName ?: "",
            restaurantLat: o.restaurantLat ?: 0.0,
            restaurantLng: o.restaurantLng ?: 0.0,
            deliveryAddress: o.deliveryAddress,
            deliveryLat: o.deliveryLat,
            deliveryLng: o.deliveryLng,
            total: o.total,
            items
        };
        check publish(TOPIC_ORDERS_CONFIRMED, orderId, confirmed);
    } else if toStatus == CANCELLED {
        OrderCancelledEvent cancelled = {
            ...newMeta("OrderCancelled"),
            orderId,
            customerId: o.customerId,
            restaurantId: o.restaurantId,
            previousStatus: fromStatus,
            reason: reason ?: "Cancelled"
        };
        check publish(TOPIC_ORDERS_CANCELLED, orderId, cancelled);
    }
    return o;
}

isolated function emitStatusChanged(OrderRow o, string? fromStatus, string toStatus, string? reason) returns error? {
    OrderStatusChangedEvent ev = {
        ...newMeta("OrderStatusChanged"),
        orderId: o.id,
        customerId: o.customerId,
        restaurantId: o.restaurantId,
        restaurantName: o.restaurantName,
        driverId: o.driverId,
        fromStatus,
        toStatus,
        reason,
        total: o.total
    };
    check publish(TOPIC_ORDER_STATUS, o.id, ev);
}

// ---------- Surge pricing (bonus) -------------------------------------------------
// multiplier is driven by demand/supply ratio (active orders per available
// driver) plus a peak-meal-time premium, capped at maxSurgeMultiplier.
isolated function computeSurge() returns SurgeInfo {
    int active = 0;
    int|error c = db->queryRow(`
        SELECT COUNT(*) FROM orders
         WHERE status IN ('CREATED', 'CONFIRMED', 'PREPARING', 'READY')
           AND created_at >= (UTC_TIMESTAMP() - INTERVAL 30 MINUTE)`);
    if c is int {
        active = c;
    }
    int available = 0;
    int pending = 0;
    boolean driverDataKnown = true;
    DriverStats|error ds = deliveryClient->get("/drivers/stats");
    if ds is DriverStats {
        available = ds.available;
        pending = ds.pendingDeliveries;
    } else {
        driverDataKnown = false;
        log:printWarn("delivery-service unreachable; surge computed without driver data", 'error = ds);
    }
    decimal multiplier = 1.0d;
    string[] reasons = [];
    if driverDataKnown {
        if available == 0 {
            multiplier = 2.0d;
            reasons.push("no drivers available");
        } else {
            decimal ratio = <decimal>(active + pending) / <decimal>available;
            if ratio > 3d {
                multiplier = 1.8d;
            } else if ratio > 2d {
                multiplier = 1.5d;
            } else if ratio > 1d {
                multiplier = 1.2d;
            }
            if multiplier > 1d {
                reasons.push(string `high demand (${active + pending} orders for ${available} drivers)`);
            }
        }
    }
    boolean peak = isPeakHour();
    if peak {
        multiplier += 0.2d;
        reasons.push("peak meal time");
    }
    if multiplier > maxSurgeMultiplier {
        multiplier = maxSurgeMultiplier;
    }
    return {
        multiplier: multiplier.round(2),
        activeOrders: active,
        availableDrivers: available,
        pendingDeliveries: pending,
        peakHour: peak,
        reason: reasons.length() == 0 ? "normal demand" : string:'join(", ", ...reasons)
    };
}

// Peak meal times in Windhoek local time: 11:30-14:00 and 17:30-20:30
isolated function isPeakHour() returns boolean {
    int secondsOfDay = (time:utcNow()[0] + utcOffsetHours * 3600) % 86400;
    int minuteOfDay = secondsOfDay / 60;
    return (minuteOfDay >= 690 && minuteOfDay < 840) || (minuteOfDay >= 1050 && minuteOfDay < 1230);
}

// ---------- Kafka consumer -----------------------------------------------------------
listener kafka:Listener eventListener = new (kafkaBootstrap, {
    groupId: SERVICE_NAME,
    topics: [
        TOPIC_ORDERS_VALIDATED,
        TOPIC_ORDERS_REJECTED,
        TOPIC_PAYMENTS_COMPLETED,
        TOPIC_PAYMENTS_FAILED,
        TOPIC_KITCHEN_PREPARING,
        TOPIC_KITCHEN_READY,
        TOPIC_DELIVERY_ASSIGNED,
        TOPIC_DELIVERY_PICKED_UP,
        TOPIC_DELIVERY_COMPLETED
    ],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: false,
    pollingInterval: 0.5
});

service on eventListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        check processRecords(records, handleEvent);
        check caller->'commit();
    }

    remote function onError(kafka:Error err) {
        log:printError("kafka consumer error", 'error = err);
    }
}

isolated function handleEvent(string topic, json payload) returns error? {
    match topic {
        TOPIC_ORDERS_VALIDATED => {
            check onOrderValidated(check payload.cloneWithType());
        }
        TOPIC_ORDERS_REJECTED => {
            OrderRejectedEvent e = check payload.cloneWithType();
            _ = check transition(e.orderId, CANCELLED, "Rejected by restaurant: " + e.reason);
        }
        TOPIC_PAYMENTS_COMPLETED => {
            PaymentResultEvent e = check payload.cloneWithType();
            _ = check transition(e.orderId, CONFIRMED, string `Payment ${e.transactionRef ?: e.paymentId} received`);
        }
        TOPIC_PAYMENTS_FAILED => {
            PaymentResultEvent e = check payload.cloneWithType();
            _ = check transition(e.orderId, CANCELLED, "Payment failed: " + (e.reason ?: "unknown"));
        }
        TOPIC_KITCHEN_PREPARING => {
            KitchenEvent e = check payload.cloneWithType();
            _ = check transition(e.orderId, PREPARING, "Kitchen started preparing");
        }
        TOPIC_KITCHEN_READY => {
            KitchenEvent e = check payload.cloneWithType();
            _ = check transition(e.orderId, READY, "Food ready for pickup");
        }
        TOPIC_DELIVERY_ASSIGNED => {
            DeliveryEvent e = check payload.cloneWithType();
            _ = check db->execute(`UPDATE orders SET driver_id = ${e.driverId} WHERE id = ${e.orderId}`);
            log:printInfo("driver attached to order", orderId = e.orderId, driverId = e.driverId);
        }
        TOPIC_DELIVERY_PICKED_UP => {
            DeliveryEvent e = check payload.cloneWithType();
            _ = check transition(e.orderId, OUT_FOR_DELIVERY, string `Picked up by ${e.driverName ?: "driver"}`);
        }
        TOPIC_DELIVERY_COMPLETED => {
            DeliveryEvent e = check payload.cloneWithType();
            _ = check transition(e.orderId, DELIVERED, "Delivered to customer");
        }
    }
}

// Restaurant accepted + priced the order: compute delivery fee with surge
// and ask the Payment Service to charge the customer.
isolated function onOrderValidated(OrderValidatedEvent e) returns error? {
    OrderRow? found = check findOrder(e.orderId);
    if found is () || found.status != CREATED || found.total > 0d {
        log:printInfo("validation ignored (order not awaiting pricing)", orderId = e.orderId);
        return;
    }
    OrderRow o = found;
    SurgeInfo surge = computeSurge();
    float distanceKm = haversineKm(e.restaurantLat, e.restaurantLng, o.deliveryLat, o.deliveryLng) * 1.3;
    decimal fee = ((baseDeliveryFee + perKmFee * <decimal>distanceKm) * surge.multiplier).round(2);
    decimal total = e.subtotal + fee;
    transaction {
        _ = check db->execute(`
            UPDATE orders SET restaurant_name = ${e.restaurantName}, restaurant_lat = ${e.restaurantLat},
                   restaurant_lng = ${e.restaurantLng}, subtotal = ${e.subtotal}, delivery_fee = ${fee},
                   surge_multiplier = ${surge.multiplier}, total = ${total}
             WHERE id = ${e.orderId} AND status = ${CREATED}`);
        foreach PricedItem p in e.items {
            _ = check db->execute(`
                UPDATE order_items SET name = ${p.name}, unit_price = ${p.unitPrice}
                 WHERE order_id = ${e.orderId} AND menu_item_id = ${p.menuItemId}`);
        }
        PaymentRequestedEvent req = {
            ...newMeta("PaymentRequested"),
            orderId: e.orderId,
            customerId: o.customerId,
            amount: total,
            paymentMethod: o.paymentMethod
        };
        check publish(TOPIC_PAYMENTS_REQUESTED, e.orderId, req);
        check commit;
    }
    log:printInfo("order priced", orderId = e.orderId, subtotal = e.subtotal, deliveryFee = fee,
            surge = surge.multiplier, total = total);
}
