// =====================================================================
//  Customer Service - accounts, delivery addresses, order history
//  REST  : /customers/**
//  Kafka : consumes orders.created, orders.status.changed (order history)
// =====================================================================
import ballerina/http;
import ballerina/log;
import ballerina/sql;
import ballerinax/kafka;

const SERVICE_NAME = "customer-service";

configurable int httpPort = 9091;
configurable string dbName = "customer_db";
configurable string dbUser = "customer_svc";
configurable string dbPassword = "customer_pass";

// ---------- API types ----------------------------------------------------
type Customer record {|
    int id;
    string fullName;
    string email;
    string phone;
    string createdAt;
|};

type Address record {|
    int id;
    int customerId;
    string label;
    string street;
    string suburb;
    string city;
    float latitude;
    float longitude;
    boolean isDefault;
|};

type CustomerProfile record {|
    *Customer;
    Address[] addresses;
|};

type AddressInput record {|
    string label;
    string street;
    string suburb;
    string city = "Windhoek";
    float latitude;
    float longitude;
    boolean isDefault = false;
|};

type CustomerInput record {|
    string fullName;
    string email;
    string phone;
    AddressInput[] addresses = [];
|};

type CustomerUpdate record {|
    string fullName?;
    string phone?;
|};

type OrderHistoryEntry record {|
    string orderId;
    int? restaurantId;
    string? restaurantName;
    decimal total;
    string status;
    string placedAt;
    string updatedAt;
|};

isolated function customerSelect() returns sql:ParameterizedQuery =>
    `SELECT id, full_name AS fullName, email, phone,
            DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ') AS createdAt
       FROM customers`;

isolated function addressSelect() returns sql:ParameterizedQuery =>
    `SELECT id, customer_id AS customerId, label, street, suburb, city, latitude, longitude,
            is_default AS isDefault
       FROM addresses`;

@http:ServiceConfig {
    cors: {allowOrigins: ["*"], allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"]}
}
isolated service /customers on httpListener {

    // List all customers
    isolated resource function get .() returns Customer[]|error {
        stream<Customer, sql:Error?> rows = db->query(sql:queryConcat(customerSelect(), ` ORDER BY id`));
        return from Customer c in rows select c;
    }

    // Register a customer (optionally with addresses) - atomic
    isolated resource function post .(CustomerInput input) returns http:Created|http:BadRequest|http:Conflict|error {
        string? problem = validateCustomer(input.fullName, input.email, input.phone);
        if problem is string {
            return errBadRequest(problem);
        }
        foreach AddressInput a in input.addresses {
            string? addrProblem = validateAddress(a);
            if addrProblem is string {
                return errBadRequest(addrProblem);
            }
        }
        int newId = 0;
        transaction {
            sql:ExecutionResult res = check db->execute(`
                INSERT INTO customers (full_name, email, phone)
                VALUES (${input.fullName.trim()}, ${input.email.trim().toLowerAscii()}, ${input.phone.trim()})`);
            newId = check res.lastInsertId.ensureType();
            foreach AddressInput a in input.addresses {
                _ = check insertAddress(newId, a);
            }
            check commit;
        } on fail error e {
            if isDuplicateKey(e) {
                return errConflict("A customer with this email already exists");
            }
            return e;
        }
        log:printInfo("customer registered", customerId = newId);
        return <http:Created>{body: check getProfile(newId)};
    }

    isolated resource function get [int id]() returns CustomerProfile|http:NotFound|error {
        CustomerProfile|sql:Error profile = getProfile(id);
        if profile is sql:NoRowsError {
            return errNotFound(string `Customer ${id} not found`);
        }
        return profile;
    }

    isolated resource function put [int id](CustomerUpdate input) returns CustomerProfile|http:NotFound|http:BadRequest|error {
        string? name = input.fullName;
        string? phone = input.phone;
        if name is string && name.trim().length() < 2 {
            return errBadRequest("fullName must have at least 2 characters");
        }
        sql:ExecutionResult res = check db->execute(`
            UPDATE customers SET full_name = COALESCE(${name}, full_name), phone = COALESCE(${phone}, phone)
             WHERE id = ${id}`);
        if res.affectedRowCount == 0 && !(check customerExists(id)) {
            return errNotFound(string `Customer ${id} not found`);
        }
        return check getProfile(id);
    }

    isolated resource function get [int id]/addresses() returns Address[]|http:NotFound|error {
        if !(check customerExists(id)) {
            return errNotFound(string `Customer ${id} not found`);
        }
        return getAddresses(id);
    }

    isolated resource function post [int id]/addresses(AddressInput input) returns http:Created|http:NotFound|http:BadRequest|error {
        string? problem = validateAddress(input);
        if problem is string {
            return errBadRequest(problem);
        }
        if !(check customerExists(id)) {
            return errNotFound(string `Customer ${id} not found`);
        }
        int addressId = check insertAddress(id, input);
        Address created = check db->queryRow(sql:queryConcat(addressSelect(), ` WHERE id = ${addressId}`));
        return <http:Created>{body: created};
    }

    // Used synchronously by the Order Service to resolve the delivery location.
    isolated resource function get [int id]/addresses/[int addressId]() returns Address|http:NotFound|error {
        Address|sql:Error a = db->queryRow(sql:queryConcat(addressSelect(),
                ` WHERE id = ${addressId} AND customer_id = ${id}`));
        if a is sql:NoRowsError {
            return errNotFound(string `Address ${addressId} not found for customer ${id}`);
        }
        return a;
    }

    isolated resource function delete [int id]/addresses/[int addressId]() returns http:NoContent|http:NotFound|error {
        sql:ExecutionResult res = check db->execute(`DELETE FROM addresses WHERE id = ${addressId} AND customer_id = ${id}`);
        if res.affectedRowCount == 0 {
            return errNotFound("Address not found");
        }
        return http:NO_CONTENT;
    }

    // Historical order data - a local read model built from Kafka events.
    isolated resource function get [int id]/orders() returns OrderHistoryEntry[]|error {
        stream<OrderHistoryEntry, sql:Error?> rows = db->query(`
            SELECT order_id AS orderId, restaurant_id AS restaurantId, restaurant_name AS restaurantName, total, status,
                   DATE_FORMAT(placed_at, '%Y-%m-%dT%H:%i:%sZ') AS placedAt,
                   DATE_FORMAT(updated_at, '%Y-%m-%dT%H:%i:%sZ') AS updatedAt
              FROM order_history WHERE customer_id = ${id} ORDER BY placed_at DESC`);
        return from OrderHistoryEntry o in rows select o;
    }
}

// ---------- helpers --------------------------------------------------------
isolated function getProfile(int id) returns CustomerProfile|sql:Error {
    Customer c = check db->queryRow(sql:queryConcat(customerSelect(), ` WHERE id = ${id}`));
    Address[] addresses = check getAddresses(id);
    return {...c, addresses};
}

isolated function getAddresses(int customerId) returns Address[]|sql:Error {
    stream<Address, sql:Error?> rows = db->query(sql:queryConcat(addressSelect(),
            ` WHERE customer_id = ${customerId} ORDER BY is_default DESC, id`));
    return from Address a in rows select a;
}

isolated function customerExists(int id) returns boolean|error {
    int n = check db->queryRow(`SELECT COUNT(*) FROM customers WHERE id = ${id}`);
    return n > 0;
}

isolated function insertAddress(int customerId, AddressInput a) returns int|error {
    if a.isDefault {
        _ = check db->execute(`UPDATE addresses SET is_default = FALSE WHERE customer_id = ${customerId}`);
    }
    sql:ExecutionResult res = check db->execute(`
        INSERT INTO addresses (customer_id, label, street, suburb, city, latitude, longitude, is_default)
        VALUES (${customerId}, ${a.label}, ${a.street}, ${a.suburb}, ${a.city}, ${a.latitude}, ${a.longitude}, ${a.isDefault})`);
    return res.lastInsertId.ensureType();
}

isolated function validateCustomer(string fullName, string email, string phone) returns string? {
    if fullName.trim().length() < 2 {
        return "fullName must have at least 2 characters";
    }
    if !re `^[^@\s]+@[^@\s]+\.[^@\s]+$`.isFullMatch(email.trim()) {
        return "email is not valid";
    }
    if !re `^\+?[0-9 ]{7,15}$`.isFullMatch(phone.trim()) {
        return "phone must contain 7-15 digits (e.g. +264 81 123 4567)";
    }
    return ();
}

isolated function validateAddress(AddressInput a) returns string? {
    if a.street.trim() == "" || a.suburb.trim() == "" {
        return "street and suburb are required";
    }
    if a.latitude < -90.0 || a.latitude > 90.0 || a.longitude < -180.0 || a.longitude > 180.0 {
        return "latitude/longitude out of range";
    }
    return ();
}

// ---------- Kafka consumer: maintain order history read model -----------------
listener kafka:Listener eventListener = new (kafkaBootstrap, {
    groupId: SERVICE_NAME,
    topics: [TOPIC_ORDERS_CREATED, TOPIC_ORDER_STATUS],
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
            OrderCreatedEvent e = check payload.cloneWithType();
            _ = check db->execute(`
                INSERT IGNORE INTO order_history (order_id, customer_id, restaurant_id, status)
                VALUES (${e.orderId}, ${e.customerId}, ${e.restaurantId}, ${CREATED})`);
        }
        TOPIC_ORDER_STATUS => {
            OrderStatusChangedEvent e = check payload.cloneWithType();
            _ = check db->execute(`
                INSERT INTO order_history (order_id, customer_id, restaurant_id, restaurant_name, total, status)
                VALUES (${e.orderId}, ${e.customerId}, ${e.restaurantId}, ${e.restaurantName}, ${e.total}, ${e.toStatus})
                ON DUPLICATE KEY UPDATE status = VALUES(status),
                    restaurant_name = COALESCE(VALUES(restaurant_name), restaurant_name),
                    total = VALUES(total)`);
        }
    }
}
