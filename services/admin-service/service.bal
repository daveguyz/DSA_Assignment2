// =====================================================================
//  Admin Service - reporting on restaurant statistics and delivery
//  performance. Implements CQRS: it never queries other services'
//  databases; it builds its own analytical read model (order_facts,
//  delivery_facts) purely from Kafka events.
//  REST  : /admin/**
//  Kafka : consumes orders.created, orders.status.changed,
//          delivery.assigned, delivery.picked_up, delivery.completed
// =====================================================================
import ballerina/http;
import ballerina/log;
import ballerina/sql;
import ballerinax/kafka;

const SERVICE_NAME = "admin-service";

configurable int httpPort = 9097;
configurable string dbName = "admin_db";
configurable string dbUser = "admin_svc";
configurable string dbPassword = "admin_pass";
configurable map<string> monitoredServices = {
    "customer-service": "http://localhost:9091",
    "restaurant-service": "http://localhost:9092",
    "order-service": "http://localhost:9093",
    "payment-service": "http://localhost:9094",
    "delivery-service": "http://localhost:9095",
    "notification-service": "http://localhost:9096"
};

type Summary record {|
    int totalOrders;
    int delivered;
    int cancelled;
    int inProgress;
    decimal revenue;
    decimal avgOrderValue;
    decimal avgFulfilmentMinutes;
    decimal cancellationRate;
|};

type StatusCount record {|
    string status;
    int count;
|};

type RestaurantReport record {|
    int restaurantId;
    string restaurantName;
    int totalOrders;
    int delivered;
    int cancelled;
    decimal revenue;
    decimal avgOrderValue;
    decimal avgPrepMinutes;
    decimal cancellationRate;
|};

type DriverReport record {|
    int driverId;
    string driverName;
    int assigned;
    int completed;
    decimal totalDistanceKm;
    decimal avgDistanceKm;
    decimal avgDeliveryMinutes;
    decimal avgTotalMinutes;
    decimal onTimeRate;
|};

type HourlyPoint record {|
    string period;
    int orders;
    int delivered;
    decimal revenue;
|};

type ServiceHealth record {|
    string name;
    string status;
    json? detail;
|};

// SQL expression converting an ISO-8601 event timestamp to DATETIME(3)
isolated function ts(string iso) returns sql:ParameterizedQuery =>
    `CAST(REPLACE(REPLACE(${iso}, 'T', ' '), 'Z', '') AS DATETIME(3))`;

@http:ServiceConfig {
    cors: {allowOrigins: ["*"]}
}
isolated service /admin on httpListener {

    isolated resource function get reports/summary() returns record {|Summary summary; StatusCount[] byStatus;|}|error {
        Summary s = check db->queryRow(`
            SELECT COUNT(*) AS totalOrders,
                   CAST(COALESCE(SUM(status = 'DELIVERED'), 0) AS SIGNED) AS delivered,
                   CAST(COALESCE(SUM(status = 'CANCELLED'), 0) AS SIGNED) AS cancelled,
                   CAST(COALESCE(SUM(status NOT IN ('DELIVERED', 'CANCELLED')), 0) AS SIGNED) AS inProgress,
                   CAST(COALESCE(SUM(CASE WHEN status = 'DELIVERED' THEN total END), 0) AS DECIMAL(12,2)) AS revenue,
                   CAST(COALESCE(AVG(CASE WHEN status = 'DELIVERED' THEN total END), 0) AS DECIMAL(12,2)) AS avgOrderValue,
                   CAST(COALESCE(AVG(TIMESTAMPDIFF(SECOND, created_at, delivered_at)) / 60, 0) AS DECIMAL(10,1)) AS avgFulfilmentMinutes,
                   CAST(COALESCE(100 * SUM(status = 'CANCELLED') / NULLIF(COUNT(*), 0), 0) AS DECIMAL(5,1)) AS cancellationRate
              FROM order_facts`);
        stream<StatusCount, sql:Error?> rows = db->query(`
            SELECT status, COUNT(*) AS count FROM order_facts GROUP BY status ORDER BY count DESC`);
        StatusCount[] byStatus = check from StatusCount c in rows select c;
        return {summary: s, byStatus};
    }

    // Restaurant statistics: volume, revenue, preparation speed, cancellations
    isolated resource function get reports/restaurants() returns RestaurantReport[]|error {
        stream<RestaurantReport, sql:Error?> rows = db->query(`
            SELECT restaurant_id AS restaurantId,
                   COALESCE(MAX(restaurant_name), CONCAT('Restaurant ', restaurant_id)) AS restaurantName,
                   COUNT(*) AS totalOrders,
                   CAST(SUM(status = 'DELIVERED') AS SIGNED) AS delivered,
                   CAST(SUM(status = 'CANCELLED') AS SIGNED) AS cancelled,
                   CAST(COALESCE(SUM(CASE WHEN status = 'DELIVERED' THEN total END), 0) AS DECIMAL(12,2)) AS revenue,
                   CAST(COALESCE(AVG(CASE WHEN status = 'DELIVERED' THEN total END), 0) AS DECIMAL(12,2)) AS avgOrderValue,
                   CAST(COALESCE(AVG(TIMESTAMPDIFF(SECOND, preparing_at, ready_at)) / 60, 0) AS DECIMAL(10,1)) AS avgPrepMinutes,
                   CAST(100 * SUM(status = 'CANCELLED') / COUNT(*) AS DECIMAL(5,1)) AS cancellationRate
              FROM order_facts GROUP BY restaurant_id ORDER BY revenue DESC, totalOrders DESC`);
        return from RestaurantReport r in rows select r;
    }

    // Delivery performance per driver: throughput, speed, distance, on-time %
    isolated resource function get reports/deliveries() returns DriverReport[]|error {
        stream<DriverReport, sql:Error?> rows = db->query(`
            SELECT driver_id AS driverId,
                   COALESCE(MAX(driver_name), CONCAT('Driver ', driver_id)) AS driverName,
                   COUNT(*) AS assigned,
                   CAST(SUM(delivered_at IS NOT NULL) AS SIGNED) AS completed,
                   CAST(COALESCE(SUM(CASE WHEN delivered_at IS NOT NULL THEN distance_km END), 0) AS DECIMAL(10,2)) AS totalDistanceKm,
                   CAST(COALESCE(AVG(distance_km), 0) AS DECIMAL(10,2)) AS avgDistanceKm,
                   CAST(COALESCE(AVG(TIMESTAMPDIFF(SECOND, picked_up_at, delivered_at)) / 60, 0) AS DECIMAL(10,1)) AS avgDeliveryMinutes,
                   CAST(COALESCE(AVG(TIMESTAMPDIFF(SECOND, assigned_at, delivered_at)) / 60, 0) AS DECIMAL(10,1)) AS avgTotalMinutes,
                   CAST(COALESCE(100 * SUM(TIMESTAMPDIFF(SECOND, assigned_at, delivered_at) <= eta_minutes * 60)
                        / NULLIF(SUM(delivered_at IS NOT NULL), 0), 0) AS DECIMAL(5,1)) AS onTimeRate
              FROM delivery_facts WHERE driver_id IS NOT NULL
             GROUP BY driver_id ORDER BY completed DESC`);
        return from DriverReport r in rows select r;
    }

    // Order volume per hour (last N hours, UTC)
    isolated resource function get reports/hourly(int hours = 24) returns HourlyPoint[]|error {
        stream<HourlyPoint, sql:Error?> rows = db->query(`
            SELECT DATE_FORMAT(created_at, '%Y-%m-%d %H:00') AS period, COUNT(*) AS orders,
                   CAST(SUM(status = 'DELIVERED') AS SIGNED) AS delivered,
                   CAST(COALESCE(SUM(CASE WHEN status = 'DELIVERED' THEN total END), 0) AS DECIMAL(12,2)) AS revenue
              FROM order_facts
             WHERE created_at >= UTC_TIMESTAMP() - INTERVAL ${hours} HOUR
             GROUP BY period ORDER BY period`);
        return from HourlyPoint p in rows select p;
    }

    // Aggregated platform health (service isolation / environment stability)
    isolated resource function get system/health() returns ServiceHealth[] {
        ServiceHealth[] result = [{name: SERVICE_NAME, status: "UP", detail: ()}];
        foreach [string, string] [name, url] in monitoredServices.entries() {
            http:Client|error c = new (url, {timeout: 2});
            if c is error {
                result.push({name, status: "DOWN", detail: c.message()});
                continue;
            }
            json|error h = c->get("/health");
            if h is error {
                result.push({name, status: "DOWN", detail: h.message()});
            } else {
                json|error st = h.status;
                result.push({name, status: st is string ? st : "UNKNOWN", detail: h});
            }
        }
        return result;
    }
}

// ---------- Kafka consumer: build the analytical read model ----------------------
listener kafka:Listener eventListener = new (kafkaBootstrap, {
    groupId: SERVICE_NAME,
    topics: [TOPIC_ORDERS_CREATED, TOPIC_ORDER_STATUS, TOPIC_DELIVERY_ASSIGNED, TOPIC_DELIVERY_PICKED_UP, TOPIC_DELIVERY_COMPLETED],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    autoCommit: false,
    pollingInterval: 1
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
        TOPIC_ORDERS_CREATED => {
            OrderCreatedEvent e = check payload.cloneWithType();
            _ = check db->execute(sql:queryConcat(`
                INSERT IGNORE INTO order_facts (order_id, customer_id, restaurant_id, status, total, created_at)
                VALUES (${e.orderId}, ${e.customerId}, ${e.restaurantId}, ${CREATED}, 0, `, ts(e.occurredAt), `)`));
        }
        TOPIC_ORDER_STATUS => {
            OrderStatusChangedEvent e = check payload.cloneWithType();
            sql:ParameterizedQuery at = ts(e.occurredAt);
            _ = check db->execute(sql:queryConcat(`
                INSERT INTO order_facts (order_id, customer_id, restaurant_id, restaurant_name, status, total, created_at)
                VALUES (${e.orderId}, ${e.customerId}, ${e.restaurantId}, ${e.restaurantName}, ${e.toStatus}, ${e.total}, `, at, `)
                ON DUPLICATE KEY UPDATE
                    status = ${e.toStatus},
                    restaurant_name = COALESCE(${e.restaurantName}, restaurant_name),
                    total = IF(${e.total} > 0, ${e.total}, total),
                    cancel_reason = IF(${e.toStatus} = 'CANCELLED', ${e.reason}, cancel_reason),
                    confirmed_at = IF(${e.toStatus} = 'CONFIRMED', `, at, `, confirmed_at),
                    preparing_at = IF(${e.toStatus} = 'PREPARING', `, at, `, preparing_at),
                    ready_at = IF(${e.toStatus} = 'READY', `, at, `, ready_at),
                    out_for_delivery_at = IF(${e.toStatus} = 'OUT_FOR_DELIVERY', `, at, `, out_for_delivery_at),
                    delivered_at = IF(${e.toStatus} = 'DELIVERED', `, at, `, delivered_at),
                    cancelled_at = IF(${e.toStatus} = 'CANCELLED', `, at, `, cancelled_at)`));
        }
        TOPIC_DELIVERY_ASSIGNED => {
            DeliveryEvent e = check payload.cloneWithType();
            _ = check db->execute(sql:queryConcat(`
                INSERT INTO delivery_facts (order_id, driver_id, driver_name, distance_km, eta_minutes, assigned_at)
                VALUES (${e.orderId}, ${e.driverId}, ${e.driverName}, ${e.distanceKm}, ${e.etaMinutes}, `, ts(e.occurredAt), `)
                ON DUPLICATE KEY UPDATE driver_id = VALUES(driver_id), driver_name = VALUES(driver_name),
                    distance_km = VALUES(distance_km), eta_minutes = VALUES(eta_minutes), assigned_at = VALUES(assigned_at)`));
        }
        TOPIC_DELIVERY_PICKED_UP => {
            DeliveryEvent e = check payload.cloneWithType();
            _ = check db->execute(sql:queryConcat(`
                INSERT INTO delivery_facts (order_id, driver_id, driver_name, picked_up_at)
                VALUES (${e.orderId}, ${e.driverId}, ${e.driverName}, `, ts(e.occurredAt), `)
                ON DUPLICATE KEY UPDATE picked_up_at = VALUES(picked_up_at)`));
        }
        TOPIC_DELIVERY_COMPLETED => {
            DeliveryEvent e = check payload.cloneWithType();
            _ = check db->execute(sql:queryConcat(`
                INSERT INTO delivery_facts (order_id, driver_id, driver_name, delivered_at)
                VALUES (${e.orderId}, ${e.driverId}, ${e.driverName}, `, ts(e.occurredAt), `)
                ON DUPLICATE KEY UPDATE delivered_at = VALUES(delivered_at)`));
        }
    }
}
