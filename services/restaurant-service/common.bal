// =====================================================================
//  NamDeliver - shared infrastructure helpers (copied into every service)
//  - MySQL client (database-per-service)
//  - Idempotent Kafka producer + reliable consumer pipeline
//    (idempotency table, bounded retries with back-off, dead-letter topic)
//  - /health endpoint, HTTP error helpers, geo helpers
//  Each service defines: SERVICE_NAME, httpPort, dbName, dbUser, dbPassword
// =====================================================================
import ballerina/http;
import ballerina/lang.runtime;
import ballerina/log;
import ballerina/sql;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;
import ballerinax/prometheus as _;

configurable string kafkaBootstrap = "localhost:29092";
configurable string dbHost = "localhost";
configurable int dbPort = 3307;

const int MAX_HANDLER_ATTEMPTS = 3;

// ---------- Persistence ---------------------------------------------------
final mysql:Client db = check new (host = dbHost, user = dbUser, password = dbPassword,
    database = dbName, port = dbPort,
    connectionPool = {maxOpenConnections: 15, minIdleConnections: 2}
);

// ---------- Kafka producer (acks=all + idempotence => no duplicates on retry)
final kafka:Producer eventProducer = check new (kafkaBootstrap, {
    clientId: SERVICE_NAME + "-producer",
    acks: kafka:ACKS_ALL,
    retryCount: 5,
    enableIdempotence: true
});

// ---------- HTTP listener + health probe ---------------------------------
listener http:Listener httpListener = new (httpPort);

service /health on httpListener {
    isolated resource function get .() returns json {
        boolean dbUp = true;
        int|sql:Error ping = db->queryRow(`SELECT 1`);
        if ping is sql:Error {
            dbUp = false;
        }
        return {status: dbUp ? "UP" : "DEGRADED", serviceName: SERVICE_NAME, database: dbUp ? "UP" : "DOWN", time: nowIso()};
    }
}

// ---------- Small utilities ---------------------------------------------------
isolated function nowIso() returns string => time:utcToString(time:utcNow());

isolated function newMeta(string eventType) returns EventMeta => {
    eventId: uuid:createType4AsString(),
    eventType: eventType,
    occurredAt: nowIso(),
    'source: SERVICE_NAME
};

type ErrorBody record {|
    string message;
|};

isolated function errBadRequest(string message) returns http:BadRequest => {body: <ErrorBody>{message}};

isolated function errNotFound(string message) returns http:NotFound => {body: <ErrorBody>{message}};

isolated function errConflict(string message) returns http:Conflict => {body: <ErrorBody>{message}};

isolated function isDuplicateKey(error e) returns boolean {
    if e is sql:DatabaseError {
        return e.detail().errorCode == 1062;
    }
    return false;
}

// Great-circle distance in kilometres.
isolated function haversineKm(float lat1, float lng1, float lat2, float lng2) returns float {
    float r = 6371.0;
    float dLat = toRad(lat2 - lat1);
    float dLng = toRad(lng2 - lng1);
    float a = float:pow(float:sin(dLat / 2.0), 2.0)
        + float:cos(toRad(lat1)) * float:cos(toRad(lat2)) * float:pow(float:sin(dLng / 2.0), 2.0);
    return r * 2.0 * float:atan2(float:sqrt(a), float:sqrt(1.0 - a));
}

isolated function toRad(float deg) returns float => deg * float:PI / 180.0;

// ---------- Event publishing -------------------------------------------------
// key = aggregate id (orderId) => all events of one order share a partition => ordering.
isolated function publish(string topic, string key, anydata event) returns error? {
    check eventProducer->send({topic: topic, key: key.toBytes(), value: event.toJsonString().toBytes()});
    log:printInfo("event published", topic = topic, key = key);
}

// ---------- Reliable consumption pipeline -------------------------------------
type EventHandler isolated function (string topic, json payload) returns error?;

// Processes a polled batch. For every record:
//  1. skip if eventId already processed (idempotent consumer, at-least-once safe)
//  2. invoke handler with up to MAX_HANDLER_ATTEMPTS attempts + linear back-off
//  3. on permanent failure publish to dlq.events instead of blocking the partition
//  4. record eventId in processed_events
// Offsets are committed manually by the caller only after the batch succeeds.
isolated function processRecords(kafka:BytesConsumerRecord[] records, EventHandler handler) returns error? {
    foreach kafka:BytesConsumerRecord rec in records {
        string topic = rec.offset.partition.topic;
        string|error raw = string:fromBytes(rec.value);
        if raw is error {
            log:printError("undecodable record skipped", topic = topic, 'error = raw);
            continue;
        }
        json|error payload = raw.fromJsonString();
        if payload is error {
            check sendToDlq(topic, raw, "Invalid JSON: " + payload.message());
            continue;
        }
        json|error idField = payload.eventId;
        string eventId = idField is string ? idField : uuid:createType4AsString();
        if check isProcessed(eventId) {
            log:printDebug("duplicate event ignored", eventId = eventId, topic = topic);
            continue;
        }
        error? result = ();
        foreach int attempt in 1 ... MAX_HANDLER_ATTEMPTS {
            result = handler(topic, payload);
            if result is () {
                break;
            }
            log:printWarn("event handler failed", topic = topic, eventId = eventId, attempt = attempt, 'error = result);
            runtime:sleep(<decimal>attempt * 0.5d);
        }
        if result is error {
            check sendToDlq(topic, raw, result.message());
        }
        check markProcessed(eventId, topic);
    }
}

isolated function isProcessed(string eventId) returns boolean|error {
    int count = check db->queryRow(`SELECT COUNT(*) FROM processed_events WHERE event_id = ${eventId}`);
    return count > 0;
}

isolated function markProcessed(string eventId, string topic) returns error? {
    _ = check db->execute(`INSERT IGNORE INTO processed_events (event_id, topic) VALUES (${eventId}, ${topic})`);
}

isolated function sendToDlq(string topic, string raw, string reason) returns error? {
    DeadLetterEvent dead = {
        ...newMeta("DeadLetter"),
        originalTopic: topic,
        failedBy: SERVICE_NAME,
        errorMessage: reason,
        rawPayload: raw
    };
    log:printError("event moved to DLQ", topic = topic, reason = reason);
    check publish(TOPIC_DLQ, topic, dead);
}
