// =====================================================================
//  Delivery Service - driver fleet, nearest-driver dispatch, delivery
//  tracking, route optimisation and live location simulation.
//
//  REST  : /drivers/**, /deliveries/**, /routes/**
//  Kafka : consumes orders.confirmed (create + dispatch),
//                   orders.status.changed (READY -> pickup allowed, CANCELLED)
//          produces delivery.assigned, delivery.picked_up, delivery.completed,
//                   delivery.location.updated
// =====================================================================
import ballerina/http;
import ballerina/lang.runtime;
import ballerina/log;
import ballerina/sql;
import ballerina/task;
import ballerina/uuid;
import ballerinax/kafka;

const SERVICE_NAME = "delivery-service";

configurable int httpPort = 9095;
configurable string dbName = "delivery_db";
configurable string dbUser = "delivery_svc";
configurable string dbPassword = "delivery_pass";
configurable boolean simulateMovement = true;
configurable decimal simulationStepSeconds = 1.5;
configurable float simulationStepKm = 0.25;
configurable decimal pendingRetrySeconds = 15;

const AVAILABLE = "AVAILABLE";
const BUSY = "BUSY";
const OFFLINE = "OFFLINE";
const PENDING_ASSIGNMENT = "PENDING_ASSIGNMENT";
const AWAITING_DETAILS = "AWAITING_DETAILS";
const ASSIGNED = "ASSIGNED";
const PICKED_UP = "PICKED_UP";

// ---------- Types ---------------------------------------------------------
type Driver record {|
    int id;
    string name;
    string phone;
    string vehicle;
    string status;
    float latitude;
    float longitude;
    string updatedAt;
|};

type DriverInput record {|
    string name;
    string phone;
    string vehicle = "Motorbike";
    float latitude = -22.5609;
    float longitude = 17.0836;
|};

type DriverStatusInput record {|
    string status;
|};

type LocationInput record {|
    float latitude;
    float longitude;
|};

type DriverActionInput record {|
    int driverId;
|};

type DriverStats record {|
    int available;
    int busy;
    int offline;
    int pendingDeliveries;
    int activeDeliveries;
|};

type DeliveryRow record {|
    string id;
    string orderId;
    int customerId;
    int restaurantId;
    string? restaurantName;
    string? deliveryAddress;
    int? driverId;
    string? driverName;
    string? driverPhone;
    string status;
    boolean orderReady;
    float restaurantLat;
    float restaurantLng;
    float customerLat;
    float customerLng;
    float? driverLat;
    float? driverLng;
    string? route;
    float? distanceKm;
    int? etaMinutes;
    string? assignedAt;
    string? pickedUpAt;
    string? deliveredAt;
    string createdAt;
|};

type Delivery record {|
    string id;
    string orderId;
    int customerId;
    int restaurantId;
    string? restaurantName;
    string? deliveryAddress;
    int? driverId;
    string? driverName;
    string? driverPhone;
    string status;
    boolean orderReady;
    float restaurantLat;
    float restaurantLng;
    float customerLat;
    float customerLng;
    float? driverLat;
    float? driverLng;
    RouteResult? route;
    float? distanceKm;
    int? etaMinutes;
    string? assignedAt;
    string? pickedUpAt;
    string? deliveredAt;
    string createdAt;
|};

// ---------- SQL fragments --------------------------------------------------
isolated function driverSelect() returns sql:ParameterizedQuery =>
    `SELECT id, name, phone, vehicle, status, latitude, longitude,
            DATE_FORMAT(updated_at, '%Y-%m-%dT%H:%i:%sZ') AS updatedAt
       FROM drivers`;

isolated function deliverySelect() returns sql:ParameterizedQuery =>
    `SELECT d.id, d.order_id AS orderId, d.customer_id AS customerId, d.restaurant_id AS restaurantId,
            d.restaurant_name AS restaurantName, d.delivery_address AS deliveryAddress,
            d.driver_id AS driverId, dr.name AS driverName, dr.phone AS driverPhone,
            d.status, d.order_ready AS orderReady,
            d.restaurant_lat AS restaurantLat, d.restaurant_lng AS restaurantLng,
            d.customer_lat AS customerLat, d.customer_lng AS customerLng,
            dr.latitude AS driverLat, dr.longitude AS driverLng,
            d.route_json AS route, d.distance_km AS distanceKm, d.eta_minutes AS etaMinutes,
            DATE_FORMAT(d.assigned_at, '%Y-%m-%dT%H:%i:%sZ') AS assignedAt,
            DATE_FORMAT(d.picked_up_at, '%Y-%m-%dT%H:%i:%sZ') AS pickedUpAt,
            DATE_FORMAT(d.delivered_at, '%Y-%m-%dT%H:%i:%sZ') AS deliveredAt,
            DATE_FORMAT(d.created_at, '%Y-%m-%dT%H:%i:%sZ') AS createdAt
       FROM deliveries d LEFT JOIN drivers dr ON dr.id = d.driver_id`;

isolated function toDelivery(DeliveryRow r) returns Delivery {
    RouteResult? route = ();
    string? raw = r.route;
    if raw is string {
        RouteResult|error parsed = raw.fromJsonStringWithType();
        if parsed is RouteResult {
            route = parsed;
        }
    }
    return {
        id: r.id,
        orderId: r.orderId,
        customerId: r.customerId,
        restaurantId: r.restaurantId,
        restaurantName: r.restaurantName,
        deliveryAddress: r.deliveryAddress,
        driverId: r.driverId,
        driverName: r.driverName,
        driverPhone: r.driverPhone,
        status: r.status,
        orderReady: r.orderReady,
        restaurantLat: r.restaurantLat,
        restaurantLng: r.restaurantLng,
        customerLat: r.customerLat,
        customerLng: r.customerLng,
        driverLat: r.driverLat,
        driverLng: r.driverLng,
        route,
        distanceKm: r.distanceKm,
        etaMinutes: r.etaMinutes,
        assignedAt: r.assignedAt,
        pickedUpAt: r.pickedUpAt,
        deliveredAt: r.deliveredAt,
        createdAt: r.createdAt
    };
}

isolated function findDelivery(string orderId) returns DeliveryRow?|error {
    DeliveryRow|sql:Error row = db->queryRow(sql:queryConcat(deliverySelect(), ` WHERE d.order_id = ${orderId}`));
    if row is sql:NoRowsError {
        return ();
    }
    return row;
}

isolated function findDriver(int id) returns Driver?|error {
    Driver|sql:Error row = db->queryRow(sql:queryConcat(driverSelect(), ` WHERE id = ${id}`));
    if row is sql:NoRowsError {
        return ();
    }
    return row;
}

isolated function listDeliveries(sql:ParameterizedQuery whereClause) returns Delivery[]|error {
    stream<DeliveryRow, sql:Error?> rows = db->query(sql:queryConcat(deliverySelect(), whereClause,
            ` ORDER BY d.created_at DESC LIMIT 200`));
    DeliveryRow[] list = check from DeliveryRow r in rows select r;
    return from DeliveryRow r in list select toDelivery(r);
}

// ---------- REST: drivers ---------------------------------------------------
@http:ServiceConfig {
    cors: {allowOrigins: ["*"], allowMethods: ["GET", "POST", "PUT", "OPTIONS"]}
}
isolated service /drivers on httpListener {

    isolated resource function get .(string? status) returns Driver[]|error {
        sql:ParameterizedQuery q = driverSelect();
        if status is string {
            q = sql:queryConcat(q, ` WHERE status = ${status}`);
        }
        stream<Driver, sql:Error?> rows = db->query(sql:queryConcat(q, ` ORDER BY id`));
        return from Driver d in rows select d;
    }

    isolated resource function post .(DriverInput input) returns http:Created|http:BadRequest|error {
        if input.name.trim().length() < 2 || input.phone.trim().length() < 7 {
            return errBadRequest("name and phone are required");
        }
        sql:ExecutionResult res = check db->execute(`
            INSERT INTO drivers (name, phone, vehicle, status, latitude, longitude)
            VALUES (${input.name}, ${input.phone}, ${input.vehicle}, ${OFFLINE}, ${input.latitude}, ${input.longitude})`);
        int id = check res.lastInsertId.ensureType();
        Driver d = check db->queryRow(sql:queryConcat(driverSelect(), ` WHERE id = ${id}`));
        return <http:Created>{body: d};
    }

    // Fleet supply snapshot - used by the Order Service for surge pricing.
    isolated resource function get stats() returns DriverStats|error {
        return getStats();
    }

    isolated resource function get [int id]() returns Driver|http:NotFound|error {
        Driver? d = check findDriver(id);
        return d is Driver ? d : errNotFound(string `Driver ${id} not found`);
    }

    // Go online / offline
    isolated resource function put [int id]/status(DriverStatusInput input) returns Driver|http:NotFound|http:BadRequest|http:Conflict|error {
        if input.status != AVAILABLE && input.status != OFFLINE {
            return errBadRequest("status must be AVAILABLE or OFFLINE");
        }
        Driver? d = check findDriver(id);
        if d is () {
            return errNotFound(string `Driver ${id} not found`);
        }
        if d.status == BUSY {
            return errConflict("Driver is on an active delivery; finish it first");
        }
        _ = check db->execute(`UPDATE drivers SET status = ${input.status} WHERE id = ${id} AND status <> ${BUSY}`);
        log:printInfo("driver status changed", driverId = id, status = input.status);
        if input.status == AVAILABLE {
            check assignPending();
        }
        return check findDriver(id) ?: errNotFound("Driver vanished");
    }

    // GPS ping from a real device (or the simulator)
    isolated resource function put [int id]/location(LocationInput input) returns Driver|http:NotFound|error {
        Driver? d = check findDriver(id);
        if d is () {
            return errNotFound(string `Driver ${id} not found`);
        }
        string? orderId = check activeOrderOf(id);
        check moveDriver(id, orderId, input.latitude, input.longitude, "MANUAL");
        return check findDriver(id) ?: errNotFound("Driver vanished");
    }

    isolated resource function get [int id]/deliveries(boolean active = true) returns Delivery[]|error {
        if active {
            return listDeliveries(` WHERE d.driver_id = ${id} AND d.status IN ('ASSIGNED', 'PICKED_UP')`);
        }
        return listDeliveries(` WHERE d.driver_id = ${id}`);
    }
}

// ---------- REST: deliveries ---------------------------------------------------
@http:ServiceConfig {
    cors: {allowOrigins: ["*"], allowMethods: ["GET", "POST", "OPTIONS"]}
}
isolated service /deliveries on httpListener {

    isolated resource function get .(string? status) returns Delivery[]|error {
        if status is string {
            return listDeliveries(` WHERE d.status = ${status}`);
        }
        return listDeliveries(` WHERE 1 = 1`);
    }

    // Real-time tracking: status, driver position, route & ETA
    isolated resource function get [string orderId]() returns Delivery|http:NotFound|error {
        DeliveryRow? d = check findDelivery(orderId);
        return d is DeliveryRow ? toDelivery(d) : errNotFound(string `No delivery for order ${orderId}`);
    }

    isolated resource function post [string orderId]/pickup(DriverActionInput input) returns Delivery|http:NotFound|http:Conflict|error {
        DeliveryRow? found = check findDelivery(orderId);
        if found is () {
            return errNotFound(string `No delivery for order ${orderId}`);
        }
        DeliveryRow d = found;
        if d.driverId != input.driverId {
            return errConflict("This delivery is not assigned to you");
        }
        if d.status != ASSIGNED {
            return errConflict(string `Delivery is ${d.status}; only ASSIGNED deliveries can be picked up`);
        }
        if !d.orderReady {
            return errConflict("The restaurant has not marked this order READY yet");
        }
        RouteResult leg = shortestRoute(d.restaurantLat, d.restaurantLng, d.customerLat, d.customerLng);
        transaction {
            sql:ExecutionResult res = check db->execute(`
                UPDATE deliveries SET status = ${PICKED_UP}, picked_up_at = UTC_TIMESTAMP(),
                       route_json = ${leg.toJsonString()}, eta_minutes = ${leg.etaMinutes}
                 WHERE order_id = ${orderId} AND status = ${ASSIGNED}`);
            if res.affectedRowCount == 0 {
                fail error("Delivery changed concurrently");
            }
            _ = check db->execute(`UPDATE drivers SET latitude = ${d.restaurantLat}, longitude = ${d.restaurantLng} WHERE id = ${input.driverId}`);
            check publish(TOPIC_DELIVERY_PICKED_UP, orderId, deliveryEvent("DeliveryPickedUp", d, PICKED_UP, leg.distanceKm, leg.etaMinutes));
            check commit;
        }
        if simulateMovement {
            _ = start simulateLeg(input.driverId, orderId, leg.path.cloneReadOnly(), "TO_CUSTOMER", PICKED_UP);
        }
        DeliveryRow updated = check findDelivery(orderId) ?: d;
        return toDelivery(updated);
    }

    isolated resource function post [string orderId]/complete(DriverActionInput input) returns Delivery|http:NotFound|http:Conflict|error {
        DeliveryRow? found = check findDelivery(orderId);
        if found is () {
            return errNotFound(string `No delivery for order ${orderId}`);
        }
        DeliveryRow d = found;
        if d.driverId != input.driverId {
            return errConflict("This delivery is not assigned to you");
        }
        if d.status != PICKED_UP {
            return errConflict(string `Delivery is ${d.status}; pick it up first`);
        }
        transaction {
            sql:ExecutionResult res = check db->execute(`
                UPDATE deliveries SET status = ${DELIVERED}, delivered_at = UTC_TIMESTAMP(), eta_minutes = 0
                 WHERE order_id = ${orderId} AND status = ${PICKED_UP}`);
            if res.affectedRowCount == 0 {
                fail error("Delivery changed concurrently");
            }
            _ = check db->execute(`
                UPDATE drivers SET status = ${AVAILABLE}, latitude = ${d.customerLat}, longitude = ${d.customerLng}
                 WHERE id = ${input.driverId}`);
            check publish(TOPIC_DELIVERY_COMPLETED, orderId, deliveryEvent("DeliveryCompleted", d, DELIVERED, d.distanceKm, 0));
            check commit;
        }
        log:printInfo("delivery completed", orderId = orderId, driverId = input.driverId);
        check assignPending(); // driver is free again
        DeliveryRow updated = check findDelivery(orderId) ?: d;
        return toDelivery(updated);
    }
}

// ---------- REST: routing (bonus) -------------------------------------------------
@http:ServiceConfig {
    cors: {allowOrigins: ["*"]}
}
isolated service /routes on httpListener {
    isolated resource function get .(float fromLat, float fromLng, float toLat, float toLng) returns RouteResult {
        return shortestRoute(fromLat, fromLng, toLat, toLng);
    }

    isolated resource function get graph() returns json {
        json[] edges = from RoadEdge e in EDGES
            let int a = nodeIndex(e.a), int b = nodeIndex(e.b)
            select {'from: e.a, to: e.b, speedKmh: e.speedKmh,
                path: [[NODES[a].lat, NODES[a].lng], [NODES[b].lat, NODES[b].lng]]};
        return {nodes: NODES.toJson(), edges};
    }
}

// ---------- Dispatch logic ------------------------------------------------------
isolated function getStats() returns DriverStats|error {
    record {|int available; int busy; int offline;|} s = check db->queryRow(`
        SELECT CAST(COALESCE(SUM(status = 'AVAILABLE'), 0) AS SIGNED) AS available,
               CAST(COALESCE(SUM(status = 'BUSY'), 0) AS SIGNED) AS busy,
               CAST(COALESCE(SUM(status = 'OFFLINE'), 0) AS SIGNED) AS offline
          FROM drivers`);
    int pending = check db->queryRow(`SELECT COUNT(*) FROM deliveries WHERE status = ${PENDING_ASSIGNMENT}`);
    int active = check db->queryRow(`SELECT COUNT(*) FROM deliveries WHERE status IN ('ASSIGNED', 'PICKED_UP')`);
    return {available: s.available, busy: s.busy, offline: s.offline, pendingDeliveries: pending, activeDeliveries: active};
}

isolated function activeOrderOf(int driverId) returns string?|error {
    string|sql:Error o = db->queryRow(`
        SELECT order_id FROM deliveries WHERE driver_id = ${driverId} AND status IN ('ASSIGNED', 'PICKED_UP') LIMIT 1`);
    if o is sql:NoRowsError {
        return ();
    }
    return o;
}

isolated function deliveryEvent(string eventType, DeliveryRow d, string status, float? distanceKm, int? eta) returns DeliveryEvent => {
    ...newMeta(eventType),
    orderId: d.orderId,
    deliveryId: d.id,
    customerId: d.customerId,
    restaurantId: d.restaurantId,
    driverId: d.driverId,
    driverName: d.driverName,
    deliveryStatus: status,
    distanceKm,
    etaMinutes: eta
};

// Nearest-available-driver dispatch. Each candidate is claimed with an atomic
// conditional UPDATE so two concurrent dispatchers can never grab the same driver.
isolated function tryAssign(string orderId) returns boolean|error {
    DeliveryRow? found = check findDelivery(orderId);
    if found is () || found.status != PENDING_ASSIGNMENT {
        return false;
    }
    DeliveryRow d = found;
    stream<Driver, sql:Error?> rows = db->query(sql:queryConcat(driverSelect(), ` WHERE status = ${AVAILABLE}`));
    Driver[] candidates = check from Driver dr in rows
        order by haversineKm(dr.latitude, dr.longitude, d.restaurantLat, d.restaurantLng) ascending
        select dr;
    foreach Driver driver in candidates {
        sql:ExecutionResult claim = check db->execute(`
            UPDATE drivers SET status = ${BUSY} WHERE id = ${driver.id} AND status = ${AVAILABLE}`);
        if claim.affectedRowCount != 1 {
            continue; // someone else took this driver
        }
        RouteResult toRestaurant = shortestRoute(driver.latitude, driver.longitude, d.restaurantLat, d.restaurantLng);
        RouteResult toCustomer = shortestRoute(d.restaurantLat, d.restaurantLng, d.customerLat, d.customerLng);
        RouteResult full = combineRoutes(toRestaurant, toCustomer);
        sql:ExecutionResult res = check db->execute(`
            UPDATE deliveries SET driver_id = ${driver.id}, status = ${ASSIGNED}, assigned_at = UTC_TIMESTAMP(),
                   route_json = ${full.toJsonString()}, distance_km = ${full.distanceKm}, eta_minutes = ${full.etaMinutes}
             WHERE order_id = ${orderId} AND status = ${PENDING_ASSIGNMENT}`);
        if res.affectedRowCount == 0 {
            _ = check db->execute(`UPDATE drivers SET status = ${AVAILABLE} WHERE id = ${driver.id} AND status = ${BUSY}`);
            return false;
        }
        DeliveryRow assigned = check findDelivery(orderId) ?: d;
        check publish(TOPIC_DELIVERY_ASSIGNED, orderId,
                deliveryEvent("DeliveryAssigned", assigned, ASSIGNED, full.distanceKm, full.etaMinutes));
        log:printInfo("driver assigned", orderId = orderId, driverId = driver.id, distanceKm = full.distanceKm,
                etaMinutes = full.etaMinutes, via = string:'join(" > ", ...full.via));
        if simulateMovement {
            _ = start simulateLeg(driver.id, orderId, toRestaurant.path.cloneReadOnly(), "TO_RESTAURANT", ASSIGNED);
        }
        return true;
    }
    log:printWarn("no driver available - delivery queued", orderId = orderId);
    return false;
}

// Assign queued deliveries (oldest first) while drivers are available.
isolated function assignPending() returns error? {
    stream<record {|string orderId;|}, sql:Error?> rows = db->query(`
        SELECT order_id AS orderId FROM deliveries WHERE status = ${PENDING_ASSIGNMENT} ORDER BY created_at`);
    string[] pending = check from var r in rows select r.orderId;
    foreach string orderId in pending {
        boolean assigned = check tryAssign(orderId);
        if !assigned {
            int available = check db->queryRow(`SELECT COUNT(*) FROM drivers WHERE status = ${AVAILABLE}`);
            if available == 0 {
                return;
            }
        }
    }
}

// ---------- Location updates & simulation (bonus) ------------------------------
isolated function moveDriver(int driverId, string? orderId, float lat, float lng, string phase) returns error? {
    _ = check db->execute(`UPDATE drivers SET latitude = ${lat}, longitude = ${lng} WHERE id = ${driverId}`);
    _ = check db->execute(`
        INSERT INTO location_history (driver_id, order_id, latitude, longitude, phase)
        VALUES (${driverId}, ${orderId}, ${lat}, ${lng}, ${phase})`);
    DriverLocationEvent ev = {
        ...newMeta("DriverLocationUpdated"),
        driverId,
        orderId,
        latitude: lat,
        longitude: lng,
        phase
    };
    check publish(TOPIC_DRIVER_LOCATION, driverId.toString(), ev);
}

// Moves the driver along the route in ~simulationStepKm steps, emitting a
// delivery.location.updated event each step. Stops if the delivery state changes.
isolated function simulateLeg(int driverId, string orderId, float[][] & readonly path, string phase, string requiredStatus) {
    float[][] points = densify(path);
    foreach float[] p in points {
        string|error status = db->queryRow(`SELECT status FROM deliveries WHERE order_id = ${orderId}`);
        if status !is string || status != requiredStatus {
            return;
        }
        error? moved = moveDriver(driverId, orderId, p[0], p[1], phase);
        if moved is error {
            log:printError("location simulation stopped", 'error = moved);
            return;
        }
        runtime:sleep(simulationStepSeconds);
    }
    log:printInfo("simulated leg finished", driverId = driverId, orderId = orderId, phase = phase);
}

isolated function densify(float[][] path) returns float[][] {
    float[][] out = [];
    if path.length() == 0 {
        return out;
    }
    foreach int i in 1 ..< path.length() {
        float[] a = path[i - 1];
        float[] b = path[i];
        float km = haversineKm(a[0], a[1], b[0], b[1]);
        int steps = <int>float:ceiling(km / simulationStepKm);
        if steps < 1 {
            steps = 1;
        }
        foreach int s in 1 ... steps {
            float t = <float>s / <float>steps;
            out.push([a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t]);
        }
    }
    return out;
}

// ---------- Periodic re-dispatch (fault tolerance) -------------------------------
isolated class PendingDispatchJob {
    *task:Job;
    public isolated function execute() {
        error? e = assignPending();
        if e is error {
            log:printError("pending dispatch job failed", 'error = e);
        }
    }
}

function init() returns error? {
    _ = check task:scheduleJobRecurByFrequency(new PendingDispatchJob(), pendingRetrySeconds);
}

// ---------- Kafka consumer --------------------------------------------------------
listener kafka:Listener eventListener = new (kafkaBootstrap, {
    groupId: SERVICE_NAME,
    topics: [TOPIC_ORDERS_CONFIRMED, TOPIC_ORDER_STATUS],
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
        TOPIC_ORDERS_CONFIRMED => {
            check onOrderConfirmed(check payload.cloneWithType());
        }
        TOPIC_ORDER_STATUS => {
            OrderStatusChangedEvent e = check payload.cloneWithType();
            if e.toStatus == READY {
                // Upsert: the READY signal can overtake orders.confirmed (different topics)
                _ = check db->execute(`
                    INSERT INTO deliveries (id, order_id, customer_id, restaurant_id, status, order_ready)
                    VALUES (${uuid:createType4AsString()}, ${e.orderId}, ${e.customerId}, ${e.restaurantId},
                            ${AWAITING_DETAILS}, TRUE)
                    ON DUPLICATE KEY UPDATE order_ready = TRUE`);
                log:printInfo("order ready for pickup", orderId = e.orderId);
            } else if e.toStatus == CANCELLED {
                check onOrderCancelled(e);
            }
        }
    }
}

isolated function onOrderConfirmed(OrderConfirmedEvent e) returns error? {
    string|sql:Error existing = db->queryRow(`SELECT status FROM deliveries WHERE order_id = ${e.orderId}`);
    if existing is sql:NoRowsError {
        _ = check db->execute(`
            INSERT INTO deliveries (id, order_id, customer_id, restaurant_id, restaurant_name, delivery_address,
                                    status, restaurant_lat, restaurant_lng, customer_lat, customer_lng)
            VALUES (${uuid:createType4AsString()}, ${e.orderId}, ${e.customerId}, ${e.restaurantId}, ${e.restaurantName},
                    ${e.deliveryAddress}, ${PENDING_ASSIGNMENT}, ${e.restaurantLat}, ${e.restaurantLng},
                    ${e.deliveryLat}, ${e.deliveryLng})`);
    } else if existing is sql:Error {
        return existing;
    } else if existing == AWAITING_DETAILS {
        _ = check db->execute(`
            UPDATE deliveries SET restaurant_name = ${e.restaurantName}, delivery_address = ${e.deliveryAddress},
                   restaurant_lat = ${e.restaurantLat}, restaurant_lng = ${e.restaurantLng},
                   customer_lat = ${e.deliveryLat}, customer_lng = ${e.deliveryLng}, status = ${PENDING_ASSIGNMENT}
             WHERE order_id = ${e.orderId} AND status = ${AWAITING_DETAILS}`);
    } else {
        return; // duplicate or cancelled tombstone
    }
    _ = check tryAssign(e.orderId);
}

isolated function onOrderCancelled(OrderStatusChangedEvent e) returns error? {
    DeliveryRow? found = check findDelivery(e.orderId);
    if found is () {
        // tombstone so a late orders.confirmed does not dispatch a driver
        _ = check db->execute(`
            INSERT IGNORE INTO deliveries (id, order_id, customer_id, restaurant_id, status)
            VALUES (${uuid:createType4AsString()}, ${e.orderId}, ${e.customerId}, ${e.restaurantId}, ${CANCELLED})`);
        return;
    }
    DeliveryRow d = found;
    if d.status == DELIVERED || d.status == CANCELLED {
        return;
    }
    _ = check db->execute(`UPDATE deliveries SET status = ${CANCELLED} WHERE order_id = ${e.orderId}`);
    int? driverId = d.driverId;
    if driverId is int {
        _ = check db->execute(`UPDATE drivers SET status = ${AVAILABLE} WHERE id = ${driverId} AND status = ${BUSY}`);
        log:printInfo("driver released after cancellation", driverId = driverId, orderId = e.orderId);
        check assignPending();
    }
}
