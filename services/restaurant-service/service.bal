// =====================================================================
//  Restaurant Service - digital menus, real-time inventory, opening hours,
//  kitchen queue.
//  REST  : /restaurants/**
//  Kafka : consumes orders.created (validate + reserve stock),
//          orders.confirmed (kitchen queue), orders.cancelled (restock)
//          produces orders.validated | orders.rejected, kitchen.preparing,
//          kitchen.ready
// =====================================================================
import ballerina/http;
import ballerina/log;
import ballerina/sql;
import ballerinax/kafka;

const SERVICE_NAME = "restaurant-service";

configurable int httpPort = 9092;
configurable string dbName = "restaurant_db";
configurable string dbUser = "restaurant_svc";
configurable string dbPassword = "restaurant_pass";
// Namibia (Africa/Windhoek) is UTC+2 all year.
configurable int utcOffsetHours = 2;
configurable boolean enforceOpeningHours = true;

// ---------- Types ---------------------------------------------------------
type Restaurant record {|
    int id;
    string name;
    string cuisine;
    string phone;
    string street;
    string suburb;
    float latitude;
    float longitude;
    boolean acceptingOrders;
    decimal rating;
    boolean openNow;
|};

type RestaurantRow record {|
    int id;
    string name;
    string cuisine;
    string phone;
    string street;
    string suburb;
    float latitude;
    float longitude;
    boolean acceptingOrders;
    decimal rating;
    int openNow;
|};

type OpeningHours record {|
    int dayOfWeek; // 0 = Sunday ... 6 = Saturday
    string openTime; // HH:mm
    string closeTime; // HH:mm
|};

type MenuItem record {|
    int id;
    int restaurantId;
    string name;
    string description;
    string category;
    decimal price;
    int stock;
    boolean available;
|};

type RestaurantDetails record {|
    *Restaurant;
    OpeningHours[] openingHours;
    MenuItem[] menu;
|};

type RestaurantInput record {|
    string name;
    string cuisine;
    string phone;
    string street;
    string suburb;
    float latitude;
    float longitude;
    OpeningHours[] openingHours = [];
|};

type AvailabilityInput record {|
    boolean acceptingOrders;
|};

type MenuItemInput record {|
    string name;
    string description = "";
    string category = "Mains";
    decimal price;
    int stock = 0;
    boolean available = true;
|};

type MenuItemUpdate record {|
    string name?;
    string description?;
    string category?;
    decimal price?;
    boolean available?;
|};

type StockInput record {|
    int stock?; // absolute value
    int add?; // relative restock
|};

type KitchenOrder record {|
    string orderId;
    int restaurantId;
    int customerId;
    string status;
    PricedItem[] items;
    decimal subtotal;
    string createdAt;
    string updatedAt;
|};

type KitchenRow record {|
    string orderId;
    int restaurantId;
    int customerId;
    string status;
    string items;
    decimal subtotal;
    string createdAt;
    string updatedAt;
|};

type StockRow record {|
    string name;
    decimal price;
    int stock;
    boolean available;
|};

// ---------- SQL fragments --------------------------------------------------
isolated function restaurantSelect() returns sql:ParameterizedQuery =>
    `SELECT r.id, r.name, r.cuisine, r.phone, r.street, r.suburb, r.latitude, r.longitude,
            r.accepting_orders AS acceptingOrders, r.rating,
            EXISTS (SELECT 1 FROM opening_hours h
                     WHERE h.restaurant_id = r.id
                       AND h.day_of_week = DAYOFWEEK(DATE_ADD(UTC_TIMESTAMP(), INTERVAL ${utcOffsetHours} HOUR)) - 1
                       AND TIME(DATE_ADD(UTC_TIMESTAMP(), INTERVAL ${utcOffsetHours} HOUR))
                           BETWEEN h.open_time AND h.close_time) AS openNow
       FROM restaurants r`;

isolated function menuSelect() returns sql:ParameterizedQuery =>
    `SELECT id, restaurant_id AS restaurantId, name, description, category, price, stock, available FROM menu_items`;

isolated function kitchenSelect() returns sql:ParameterizedQuery =>
    `SELECT order_id AS orderId, restaurant_id AS restaurantId, customer_id AS customerId, status, items, subtotal,
            DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ') AS createdAt,
            DATE_FORMAT(updated_at, '%Y-%m-%dT%H:%i:%sZ') AS updatedAt
       FROM kitchen_orders`;

isolated function toRestaurant(RestaurantRow r) returns Restaurant => {
    id: r.id,
    name: r.name,
    cuisine: r.cuisine,
    phone: r.phone,
    street: r.street,
    suburb: r.suburb,
    latitude: r.latitude,
    longitude: r.longitude,
    acceptingOrders: r.acceptingOrders,
    rating: r.rating,
    openNow: r.openNow == 1
};

isolated function toKitchenOrder(KitchenRow k) returns KitchenOrder|error => {
    orderId: k.orderId,
    restaurantId: k.restaurantId,
    customerId: k.customerId,
    status: k.status,
    items: check k.items.fromJsonStringWithType(),
    subtotal: k.subtotal,
    createdAt: k.createdAt,
    updatedAt: k.updatedAt
};

// ---------- REST API --------------------------------------------------------
@http:ServiceConfig {
    cors: {allowOrigins: ["*"], allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"]}
}
isolated service /restaurants on httpListener {

    isolated resource function get .(string? cuisine) returns Restaurant[]|error {
        sql:ParameterizedQuery q = restaurantSelect();
        if cuisine is string {
            q = sql:queryConcat(q, ` WHERE r.cuisine = ${cuisine}`);
        }
        stream<RestaurantRow, sql:Error?> rows = db->query(sql:queryConcat(q, ` ORDER BY r.name`));
        return from RestaurantRow r in rows select toRestaurant(r);
    }

    isolated resource function post .(RestaurantInput input) returns http:Created|http:BadRequest|error {
        if input.name.trim().length() < 2 {
            return errBadRequest("name is required");
        }
        int id = 0;
        transaction {
            sql:ExecutionResult res = check db->execute(`
                INSERT INTO restaurants (name, cuisine, phone, street, suburb, latitude, longitude)
                VALUES (${input.name}, ${input.cuisine}, ${input.phone}, ${input.street}, ${input.suburb},
                        ${input.latitude}, ${input.longitude})`);
            id = check res.lastInsertId.ensureType();
            OpeningHours[] hours = input.openingHours.length() > 0 ? input.openingHours : defaultHours();
            check replaceHours(id, hours);
            check commit;
        }
        return <http:Created>{body: check getDetails(id)};
    }

    isolated resource function get [int id]() returns RestaurantDetails|http:NotFound|error {
        RestaurantDetails|error d = getDetails(id);
        if d is sql:NoRowsError {
            return errNotFound(string `Restaurant ${id} not found`);
        }
        return d;
    }

    // Manually open/close the kitchen (e.g. load-shedding, too busy)
    isolated resource function put [int id]/availability(AvailabilityInput input) returns Restaurant|http:NotFound|error {
        sql:ExecutionResult res = check db->execute(`UPDATE restaurants SET accepting_orders = ${input.acceptingOrders} WHERE id = ${id}`);
        if res.affectedRowCount == 0 && !(check restaurantExists(id)) {
            return errNotFound(string `Restaurant ${id} not found`);
        }
        log:printInfo("restaurant availability changed", restaurantId = id, acceptingOrders = input.acceptingOrders);
        RestaurantRow row = check db->queryRow(sql:queryConcat(restaurantSelect(), ` WHERE r.id = ${id}`));
        return toRestaurant(row);
    }

    isolated resource function get [int id]/hours() returns OpeningHours[]|error {
        return getHours(id);
    }

    isolated resource function put [int id]/hours(OpeningHours[] hours) returns OpeningHours[]|http:BadRequest|http:NotFound|error {
        foreach OpeningHours h in hours {
            if h.dayOfWeek < 0 || h.dayOfWeek > 6 {
                return errBadRequest("dayOfWeek must be 0 (Sunday) .. 6 (Saturday)");
            }
            if !re `^([01][0-9]|2[0-3]):[0-5][0-9]$`.isFullMatch(h.openTime)
                || !re `^([01][0-9]|2[0-3]):[0-5][0-9]$`.isFullMatch(h.closeTime) {
                return errBadRequest("times must be HH:mm");
            }
        }
        if !(check restaurantExists(id)) {
            return errNotFound(string `Restaurant ${id} not found`);
        }
        transaction {
            check replaceHours(id, hours);
            check commit;
        }
        return getHours(id);
    }

    isolated resource function get [int id]/menu(boolean availableOnly = false) returns MenuItem[]|error {
        sql:ParameterizedQuery q = sql:queryConcat(menuSelect(), ` WHERE restaurant_id = ${id}`);
        if availableOnly {
            q = sql:queryConcat(q, ` AND available = TRUE AND stock > 0`);
        }
        stream<MenuItem, sql:Error?> rows = db->query(sql:queryConcat(q, ` ORDER BY category, name`));
        return from MenuItem m in rows select m;
    }

    isolated resource function post [int id]/menu(MenuItemInput input) returns http:Created|http:BadRequest|http:NotFound|error {
        if input.price <= 0d || input.stock < 0 {
            return errBadRequest("price must be > 0 and stock >= 0");
        }
        if !(check restaurantExists(id)) {
            return errNotFound(string `Restaurant ${id} not found`);
        }
        sql:ExecutionResult res = check db->execute(`
            INSERT INTO menu_items (restaurant_id, name, description, category, price, stock, available)
            VALUES (${id}, ${input.name}, ${input.description}, ${input.category}, ${input.price}, ${input.stock}, ${input.available})`);
        int itemId = check res.lastInsertId.ensureType();
        MenuItem created = check db->queryRow(sql:queryConcat(menuSelect(), ` WHERE id = ${itemId}`));
        return <http:Created>{body: created};
    }

    isolated resource function put [int id]/menu/[int itemId](MenuItemUpdate input) returns MenuItem|http:NotFound|http:BadRequest|error {
        decimal? price = input.price;
        if price is decimal && price <= 0d {
            return errBadRequest("price must be > 0");
        }
        _ = check db->execute(`
            UPDATE menu_items
               SET name = COALESCE(${input.name}, name),
                   description = COALESCE(${input.description}, description),
                   category = COALESCE(${input.category}, category),
                   price = COALESCE(${price}, price),
                   available = COALESCE(${input.available}, available)
             WHERE id = ${itemId} AND restaurant_id = ${id}`);
        return getMenuItem(id, itemId);
    }

    // Real-time inventory: set absolute stock or add a delivery of stock
    isolated resource function put [int id]/menu/[int itemId]/stock(StockInput input) returns MenuItem|http:NotFound|http:BadRequest|error {
        int? absolute = input.stock;
        int? delta = input.add;
        if absolute is int {
            if absolute < 0 {
                return errBadRequest("stock cannot be negative");
            }
            _ = check db->execute(`UPDATE menu_items SET stock = ${absolute} WHERE id = ${itemId} AND restaurant_id = ${id}`);
        } else if delta is int {
            _ = check db->execute(`UPDATE menu_items SET stock = GREATEST(stock + ${delta}, 0) WHERE id = ${itemId} AND restaurant_id = ${id}`);
        } else {
            return errBadRequest("provide either 'stock' or 'add'");
        }
        return getMenuItem(id, itemId);
    }

    // Kitchen queue (optionally filtered by status)
    isolated resource function get [int id]/orders(string? status) returns KitchenOrder[]|error {
        sql:ParameterizedQuery q = sql:queryConcat(kitchenSelect(), ` WHERE restaurant_id = ${id}`);
        if status is string {
            q = sql:queryConcat(q, ` AND status = ${status}`);
        } else {
            q = sql:queryConcat(q, ` AND status IN ('CONFIRMED', 'PREPARING', 'READY')`);
        }
        stream<KitchenRow, sql:Error?> rows = db->query(sql:queryConcat(q, ` ORDER BY created_at`));
        KitchenRow[] list = check from KitchenRow k in rows select k;
        KitchenOrder[] result = [];
        foreach KitchenRow k in list {
            result.push(check toKitchenOrder(k));
        }
        return result;
    }

    isolated resource function post [int id]/orders/[string orderId]/preparing() returns KitchenOrder|http:NotFound|http:Conflict|error {
        return advanceKitchen(id, orderId, CONFIRMED, PREPARING, TOPIC_KITCHEN_PREPARING, "KitchenPreparing");
    }

    isolated resource function post [int id]/orders/[string orderId]/ready() returns KitchenOrder|http:NotFound|http:Conflict|error {
        return advanceKitchen(id, orderId, PREPARING, READY, TOPIC_KITCHEN_READY, "KitchenReady");
    }
}

// ---------- helpers --------------------------------------------------------
isolated function defaultHours() returns OpeningHours[] {
    OpeningHours[] hours = [];
    foreach int d in 0 ... 6 {
        hours.push({dayOfWeek: d, openTime: "08:00", closeTime: "22:00"});
    }
    return hours;
}

isolated function replaceHours(int restaurantId, OpeningHours[] hours) returns error? {
    _ = check db->execute(`DELETE FROM opening_hours WHERE restaurant_id = ${restaurantId}`);
    foreach OpeningHours h in hours {
        _ = check db->execute(`
            INSERT INTO opening_hours (restaurant_id, day_of_week, open_time, close_time)
            VALUES (${restaurantId}, ${h.dayOfWeek}, ${h.openTime}, ${h.closeTime})`);
    }
}

isolated function getHours(int restaurantId) returns OpeningHours[]|error {
    stream<OpeningHours, sql:Error?> rows = db->query(`
        SELECT day_of_week AS dayOfWeek, TIME_FORMAT(open_time, '%H:%i') AS openTime,
               TIME_FORMAT(close_time, '%H:%i') AS closeTime
          FROM opening_hours WHERE restaurant_id = ${restaurantId} ORDER BY day_of_week`);
    return from OpeningHours h in rows select h;
}

isolated function getDetails(int id) returns RestaurantDetails|error {
    RestaurantRow row = check db->queryRow(sql:queryConcat(restaurantSelect(), ` WHERE r.id = ${id}`));
    stream<MenuItem, sql:Error?> rows = db->query(sql:queryConcat(menuSelect(), ` WHERE restaurant_id = ${id} ORDER BY category, name`));
    MenuItem[] menu = check from MenuItem m in rows select m;
    return {...toRestaurant(row), openingHours: check getHours(id), menu};
}

isolated function getMenuItem(int restaurantId, int itemId) returns MenuItem|http:NotFound|error {
    MenuItem|sql:Error m = db->queryRow(sql:queryConcat(menuSelect(), ` WHERE id = ${itemId} AND restaurant_id = ${restaurantId}`));
    if m is sql:NoRowsError {
        return errNotFound(string `Menu item ${itemId} not found`);
    }
    return m;
}

isolated function restaurantExists(int id) returns boolean|error {
    int n = check db->queryRow(`SELECT COUNT(*) FROM restaurants WHERE id = ${id}`);
    return n > 0;
}

isolated function advanceKitchen(int restaurantId, string orderId, string expected, string next, string topic, string eventType)
        returns KitchenOrder|http:NotFound|http:Conflict|error {
    KitchenRow|sql:Error found = db->queryRow(sql:queryConcat(kitchenSelect(),
            ` WHERE order_id = ${orderId} AND restaurant_id = ${restaurantId}`));
    if found is sql:NoRowsError {
        return errNotFound(string `Order ${orderId} is not in this restaurant's kitchen`);
    }
    KitchenRow k = check found;
    if k.status != expected {
        return errConflict(string `Order is ${k.status}; expected ${expected} before moving to ${next}`);
    }
    transaction {
        sql:ExecutionResult res = check db->execute(`
            UPDATE kitchen_orders SET status = ${next} WHERE order_id = ${orderId} AND status = ${expected}`);
        if res.affectedRowCount == 0 {
            fail error("Concurrent kitchen update");
        }
        KitchenEvent ev = {...newMeta(eventType), orderId, restaurantId, kitchenStatus: next};
        check publish(topic, orderId, ev);
        check commit;
    }
    k.status = next;
    return toKitchenOrder(k);
}

// ---------- Kafka consumer --------------------------------------------------
listener kafka:Listener eventListener = new (kafkaBootstrap, {
    groupId: SERVICE_NAME,
    topics: [TOPIC_ORDERS_CREATED, TOPIC_ORDERS_CONFIRMED, TOPIC_ORDERS_CANCELLED],
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
        TOPIC_ORDERS_CREATED => {
            check onOrderCreated(check payload.cloneWithType());
        }
        TOPIC_ORDERS_CONFIRMED => {
            OrderConfirmedEvent e = check payload.cloneWithType();
            _ = check db->execute(`UPDATE kitchen_orders SET status = ${CONFIRMED} WHERE order_id = ${e.orderId} AND status = 'RESERVED'`);
            log:printInfo("order queued in kitchen", orderId = e.orderId, restaurantId = e.restaurantId);
        }
        TOPIC_ORDERS_CANCELLED => {
            check onOrderCancelled(check payload.cloneWithType());
        }
    }
}

// Validates the order, reserves stock atomically and answers with
// orders.validated (priced items) or orders.rejected (reason).
isolated function onOrderCreated(OrderCreatedEvent e) returns error? {
    int existing = check db->queryRow(`SELECT COUNT(*) FROM kitchen_orders WHERE order_id = ${e.orderId}`);
    if existing > 0 {
        // Already handled, or cancelled before we saw it (tombstone).
        return;
    }
    RestaurantRow|sql:Error found = db->queryRow(sql:queryConcat(restaurantSelect(), ` WHERE r.id = ${e.restaurantId}`));
    if found is sql:NoRowsError {
        return reject(e, string `Restaurant ${e.restaurantId} does not exist`);
    }
    RestaurantRow r = check found;
    if !r.acceptingOrders {
        return reject(e, string `${r.name} is not accepting orders right now`);
    }
    if enforceOpeningHours && r.openNow == 0 {
        return reject(e, string `${r.name} is closed (outside opening hours)`);
    }
    PricedItem[]|string reservation = check reserveStock(e);
    if reservation is string {
        return reject(e, reservation);
    }
    decimal subtotal = 0;
    foreach PricedItem p in reservation {
        subtotal += p.unitPrice * <decimal>p.quantity;
    }
    OrderValidatedEvent ev = {
        ...newMeta("OrderValidated"),
        orderId: e.orderId,
        restaurantId: r.id,
        restaurantName: r.name,
        restaurantLat: r.latitude,
        restaurantLng: r.longitude,
        items: reservation,
        subtotal
    };
    check publish(TOPIC_ORDERS_VALIDATED, e.orderId, ev);
}

// All-or-nothing stock reservation using row locks (SELECT ... FOR UPDATE).
isolated function reserveStock(OrderCreatedEvent e) returns PricedItem[]|string|error {
    PricedItem[] priced = [];
    string? rejection = ();
    transaction {
        foreach OrderItemRequest it in e.items {
            StockRow|sql:Error row = db->queryRow(`
                SELECT name, price, stock, available FROM menu_items
                 WHERE id = ${it.menuItemId} AND restaurant_id = ${e.restaurantId} FOR UPDATE`);
            if row is sql:NoRowsError {
                rejection = string `Menu item ${it.menuItemId} does not exist at this restaurant`;
                break;
            }
            StockRow s = check row;
            if !s.available {
                rejection = string `${s.name} is currently unavailable`;
                break;
            }
            if s.stock < it.quantity {
                rejection = string `Insufficient stock for ${s.name} (requested ${it.quantity}, in stock ${s.stock})`;
                break;
            }
            _ = check db->execute(`UPDATE menu_items SET stock = stock - ${it.quantity} WHERE id = ${it.menuItemId}`);
            priced.push({menuItemId: it.menuItemId, name: s.name, quantity: it.quantity, unitPrice: s.price});
        }
        if rejection is () {
            decimal subtotal = 0;
            foreach PricedItem p in priced {
                subtotal += p.unitPrice * <decimal>p.quantity;
            }
            _ = check db->execute(`
                INSERT INTO kitchen_orders (order_id, restaurant_id, customer_id, status, items, subtotal)
                VALUES (${e.orderId}, ${e.restaurantId}, ${e.customerId}, 'RESERVED', ${priced.toJsonString()}, ${subtotal})`);
            check commit;
        } else {
            rollback;
        }
    }
    if rejection is string {
        return rejection;
    }
    log:printInfo("stock reserved", orderId = e.orderId, lines = priced.length());
    return priced;
}

isolated function reject(OrderCreatedEvent e, string reason) returns error? {
    log:printWarn("order rejected", orderId = e.orderId, reason = reason);
    OrderRejectedEvent ev = {...newMeta("OrderRejected"), orderId: e.orderId, restaurantId: e.restaurantId, reason};
    check publish(TOPIC_ORDERS_REJECTED, e.orderId, ev);
}

// Puts reserved stock back. If the cancel arrives before orders.created was
// processed, a CANCELLED tombstone prevents a later reservation.
isolated function onOrderCancelled(OrderCancelledEvent e) returns error? {
    KitchenRow|sql:Error found = db->queryRow(sql:queryConcat(kitchenSelect(), ` WHERE order_id = ${e.orderId}`));
    if found is sql:NoRowsError {
        _ = check db->execute(`
            INSERT IGNORE INTO kitchen_orders (order_id, restaurant_id, customer_id, status, items, subtotal)
            VALUES (${e.orderId}, ${e.restaurantId}, ${e.customerId}, 'CANCELLED', '[]', 0)`);
        return;
    }
    KitchenRow k = check found;
    if k.status != "RESERVED" && k.status != CONFIRMED && k.status != PREPARING {
        return;
    }
    PricedItem[] items = check k.items.fromJsonStringWithType();
    transaction {
        foreach PricedItem p in items {
            _ = check db->execute(`UPDATE menu_items SET stock = stock + ${p.quantity} WHERE id = ${p.menuItemId}`);
        }
        _ = check db->execute(`UPDATE kitchen_orders SET status = 'CANCELLED' WHERE order_id = ${e.orderId}`);
        check commit;
    }
    log:printInfo("stock restored after cancellation", orderId = e.orderId);
}
