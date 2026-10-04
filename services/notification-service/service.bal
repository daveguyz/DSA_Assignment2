// =====================================================================
//  Notification Service - multi-channel alerts (simulated SMS / EMAIL / PUSH)
//  to customers, restaurants and drivers.
//  REST  : /notifications/**
//  Kafka : consumes orders.status.changed, payments.completed,
//          payments.failed, payments.refunded, delivery.assigned,
//          delivery.picked_up, delivery.completed, orders.rejected, dlq.events
// =====================================================================
import ballerina/http;
import ballerina/log;
import ballerina/sql;
import ballerinax/kafka;

const SERVICE_NAME = "notification-service";

configurable int httpPort = 9096;
configurable string dbName = "notification_db";
configurable string dbUser = "notification_svc";
configurable string dbPassword = "notification_pass";

const CUSTOMER = "CUSTOMER";
const RESTAURANT = "RESTAURANT";
const DRIVER = "DRIVER";
const ADMIN = "ADMIN";
const SMS = "SMS";
const EMAIL = "EMAIL";
const PUSH = "PUSH";

type Notification record {|
    int id;
    string recipientType;
    int recipientId;
    string channel;
    string eventType;
    string? orderId;
    string title;
    string message;
    string status;
    string createdAt;
|};

type ChannelStats record {|
    string channel;
    string recipientType;
    int count;
|};

// A message to deliver to one recipient over one or more channels.
type Alert record {|
    string recipientType;
    int recipientId;
    string[] channels;
    string title;
    string message;
|};

@http:ServiceConfig {
    cors: {allowOrigins: ["*"]}
}
isolated service /notifications on httpListener {

    isolated resource function get .(string? recipientType, int? recipientId, string? orderId, int 'limit = 50)
            returns Notification[]|error {
        sql:ParameterizedQuery q = `
            SELECT id, recipient_type AS recipientType, recipient_id AS recipientId, channel,
                   event_type AS eventType, order_id AS orderId, title, message, status,
                   DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ') AS createdAt
              FROM notifications WHERE 1 = 1`;
        if recipientType is string {
            q = sql:queryConcat(q, ` AND recipient_type = ${recipientType}`);
        }
        if recipientId is int {
            q = sql:queryConcat(q, ` AND recipient_id = ${recipientId}`);
        }
        if orderId is string {
            q = sql:queryConcat(q, ` AND order_id = ${orderId}`);
        }
        int lim = 'limit < 1 || 'limit > 500 ? 50 : 'limit;
        stream<Notification, sql:Error?> rows = db->query(sql:queryConcat(q, ` ORDER BY id DESC LIMIT ${lim}`));
        return from Notification n in rows select n;
    }

    isolated resource function get stats() returns ChannelStats[]|error {
        stream<ChannelStats, sql:Error?> rows = db->query(`
            SELECT channel, recipient_type AS recipientType, COUNT(*) AS count
              FROM notifications GROUP BY channel, recipient_type ORDER BY channel, recipient_type`);
        return from ChannelStats s in rows select s;
    }
}

// ---------- Channel adapters (simulated providers) ---------------------------
isolated function dispatch(Alert alert, string eventType, string? orderId) returns error? {
    foreach string channel in alert.channels {
        string status = sendViaChannel(channel, alert);
        _ = check db->execute(`
            INSERT INTO notifications (recipient_type, recipient_id, channel, event_type, order_id, title, message, status)
            VALUES (${alert.recipientType}, ${alert.recipientId}, ${channel}, ${eventType}, ${orderId},
                    ${alert.title}, ${alert.message}, ${status})`);
    }
}

isolated function sendViaChannel(string channel, Alert alert) returns string {
    // In production these would call an SMS gateway, SMTP server and FCM/APNs.
    log:printInfo(string `[${channel}] -> ${alert.recipientType}#${alert.recipientId}: ${alert.title} - ${alert.message}`);
    return "SENT";
}

// ---------- Kafka consumer ------------------------------------------------------
listener kafka:Listener eventListener = new (kafkaBootstrap, {
    groupId: SERVICE_NAME,
    topics: [
        TOPIC_ORDER_STATUS,
        TOPIC_ORDERS_REJECTED,
        TOPIC_PAYMENTS_COMPLETED,
        TOPIC_PAYMENTS_FAILED,
        TOPIC_PAYMENTS_REFUNDED,
        TOPIC_DELIVERY_ASSIGNED,
        TOPIC_DELIVERY_PICKED_UP,
        TOPIC_DELIVERY_COMPLETED,
        TOPIC_DLQ
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

isolated function shortId(string orderId) returns string => "#" + orderId.substring(0, 8).toUpperAscii();

isolated function handleEvent(string topic, json payload) returns error? {
    match topic {
        TOPIC_ORDER_STATUS => {
            OrderStatusChangedEvent e = check payload.cloneWithType();
            foreach Alert a in alertsForStatus(e) {
                check dispatch(a, e.eventType + ":" + e.toStatus, e.orderId);
            }
        }
        TOPIC_ORDERS_REJECTED => {
            OrderRejectedEvent e = check payload.cloneWithType();
            check dispatch({
                recipientType: RESTAURANT, recipientId: e.restaurantId, channels: [PUSH],
                title: "Order auto-rejected",
                message: string `Order ${shortId(e.orderId)} was rejected: ${e.reason}`
            }, e.eventType, e.orderId);
        }
        TOPIC_PAYMENTS_COMPLETED|TOPIC_PAYMENTS_FAILED|TOPIC_PAYMENTS_REFUNDED => {
            PaymentResultEvent e = check payload.cloneWithType();
            string title = e.status == "COMPLETED" ? "Payment receipt"
                : e.status == "REFUNDED" ? "Refund processed" : "Payment failed";
            string message = e.status == "COMPLETED"
                ? string `We received N$${e.amount} (${e.paymentMethod}) for order ${shortId(e.orderId)}. Ref: ${e.transactionRef ?: "-"}`
                : e.status == "REFUNDED"
                    ? string `N$${e.amount} for order ${shortId(e.orderId)} has been refunded.`
                    : string `Payment for order ${shortId(e.orderId)} failed: ${e.reason ?: "unknown reason"}`;
            check dispatch({recipientType: CUSTOMER, recipientId: e.customerId, channels: [EMAIL, PUSH], title, message},
                    e.eventType, e.orderId);
        }
        TOPIC_DELIVERY_ASSIGNED => {
            DeliveryEvent e = check payload.cloneWithType();
            int? driverId = e.driverId;
            if driverId is int {
                check dispatch({
                    recipientType: DRIVER, recipientId: driverId, channels: [PUSH, SMS],
                    title: "New delivery job",
                    message: string `Collect order ${shortId(e.orderId)} - ${e.distanceKm ?: 0.0} km trip, ETA ${e.etaMinutes ?: 0} min`
                }, e.eventType, e.orderId);
            }
            check dispatch({
                recipientType: CUSTOMER, recipientId: e.customerId, channels: [PUSH],
                title: "Driver assigned",
                message: string `${e.driverName ?: "A driver"} will deliver order ${shortId(e.orderId)} (ETA ~${e.etaMinutes ?: 0} min)`
            }, e.eventType, e.orderId);
        }
        TOPIC_DELIVERY_PICKED_UP => {
            DeliveryEvent e = check payload.cloneWithType();
            check dispatch({
                recipientType: RESTAURANT, recipientId: e.restaurantId, channels: [PUSH],
                title: "Order collected",
                message: string `${e.driverName ?: "Driver"} collected order ${shortId(e.orderId)}`
            }, e.eventType, e.orderId);
        }
        TOPIC_DELIVERY_COMPLETED => {
            DeliveryEvent e = check payload.cloneWithType();
            int? driverId = e.driverId;
            if driverId is int {
                check dispatch({
                    recipientType: DRIVER, recipientId: driverId, channels: [PUSH],
                    title: "Delivery completed",
                    message: string `Great job! Order ${shortId(e.orderId)} delivered.`
                }, e.eventType, e.orderId);
            }
        }
        TOPIC_DLQ => {
            DeadLetterEvent e = check payload.cloneWithType();
            check dispatch({
                recipientType: ADMIN, recipientId: 0, channels: [EMAIL],
                title: "Dead-lettered event",
                message: string `${e.failedBy} could not process an event from ${e.originalTopic}: ${e.errorMessage}`
            }, e.eventType, ());
        }
    }
}

isolated function alertsForStatus(OrderStatusChangedEvent e) returns Alert[] {
    string id = shortId(e.orderId);
    string restaurant = e.restaurantName ?: "the restaurant";
    match e.toStatus {
        CREATED => {
            return [{recipientType: CUSTOMER, recipientId: e.customerId, channels: [PUSH],
                title: "Order received", message: string `Order ${id} received - waiting for restaurant & payment confirmation`}];
        }
        CONFIRMED => {
            return [
                {recipientType: CUSTOMER, recipientId: e.customerId, channels: [PUSH, SMS],
                    title: "Order confirmed", message: string `${restaurant} confirmed order ${id} (N$${e.total})`},
                {recipientType: RESTAURANT, recipientId: e.restaurantId, channels: [PUSH],
                    title: "New order", message: string `New paid order ${id} - please start preparing`}
            ];
        }
        PREPARING => {
            return [{recipientType: CUSTOMER, recipientId: e.customerId, channels: [PUSH],
                title: "Being prepared", message: string `${restaurant} is preparing order ${id}`}];
        }
        READY => {
            Alert[] alerts = [{recipientType: CUSTOMER, recipientId: e.customerId, channels: [PUSH],
                title: "Food is ready", message: string `Order ${id} is ready and waiting for the driver`}];
            int? driverId = e.driverId;
            if driverId is int {
                alerts.push({recipientType: DRIVER, recipientId: driverId, channels: [PUSH, SMS],
                    title: "Pickup ready", message: string `Order ${id} is ready for collection at ${restaurant}`});
            }
            return alerts;
        }
        OUT_FOR_DELIVERY => {
            return [{recipientType: CUSTOMER, recipientId: e.customerId, channels: [PUSH, SMS],
                title: "On the way", message: string `Order ${id} is out for delivery`}];
        }
        DELIVERED => {
            return [
                {recipientType: CUSTOMER, recipientId: e.customerId, channels: [PUSH, EMAIL],
                    title: "Delivered", message: string `Order ${id} was delivered. Enjoy your meal!`},
                {recipientType: RESTAURANT, recipientId: e.restaurantId, channels: [PUSH],
                    title: "Order delivered", message: string `Order ${id} was delivered to the customer`}
            ];
        }
        CANCELLED => {
            Alert[] alerts = [{recipientType: CUSTOMER, recipientId: e.customerId, channels: [PUSH, SMS],
                title: "Order cancelled", message: string `Order ${id} was cancelled: ${e.reason ?: "no reason given"}`}];
            if e.fromStatus != CREATED {
                alerts.push({recipientType: RESTAURANT, recipientId: e.restaurantId, channels: [PUSH],
                    title: "Order cancelled", message: string `Order ${id} was cancelled - do not prepare`});
            }
            return alerts;
        }
    }
    return [];
}
