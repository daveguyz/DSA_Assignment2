// =====================================================================
//  Payment Service - simulated payment gateway
//  REST  : /payments/**
//  Kafka : consumes payments.requested, orders.cancelled
//          produces payments.completed | payments.failed | payments.refunded
//
//  Simulation rules (configurable):
//   * amount <= 0                              -> FAILED (INVALID_AMOUNT)
//   * amount > maxTransactionAmount           -> FAILED (LIMIT_EXCEEDED)
//   * random() < failureRate (CARD / WALLET)   -> FAILED (CARD_DECLINED)
//   * CASH                                     -> COMPLETED (cash on delivery)
//   * otherwise                                -> COMPLETED with a txn reference
// =====================================================================
import ballerina/http;
import ballerina/lang.runtime;
import ballerina/log;
import ballerina/random;
import ballerina/sql;
import ballerina/uuid;
import ballerinax/kafka;

const SERVICE_NAME = "payment-service";

configurable int httpPort = 9094;
configurable string dbName = "payment_db";
configurable string dbUser = "payment_svc";
configurable string dbPassword = "payment_pass";
configurable decimal processingDelaySeconds = 1.0;
configurable float failureRate = 0.0;
configurable decimal maxTransactionAmount = 5000.00;

type Payment record {|
    string id;
    string orderId;
    int customerId;
    decimal amount;
    string method;
    string status;
    string? failureReason;
    string? transactionRef;
    string createdAt;
    string updatedAt;
|};

type PaymentSummary record {|
    string status;
    int count;
    decimal amount;
|};

isolated function paymentSelect() returns sql:ParameterizedQuery =>
    `SELECT id, order_id AS orderId, customer_id AS customerId, amount, method, status,
            failure_reason AS failureReason, transaction_ref AS transactionRef,
            DATE_FORMAT(created_at, '%Y-%m-%dT%H:%i:%sZ') AS createdAt,
            DATE_FORMAT(updated_at, '%Y-%m-%dT%H:%i:%sZ') AS updatedAt
       FROM payments`;

@http:ServiceConfig {
    cors: {allowOrigins: ["*"]}
}
isolated service /payments on httpListener {

    isolated resource function get .(string? status, int? customerId) returns Payment[]|error {
        sql:ParameterizedQuery q = sql:queryConcat(paymentSelect(), ` WHERE 1 = 1`);
        if status is string {
            q = sql:queryConcat(q, ` AND status = ${status}`);
        }
        if customerId is int {
            q = sql:queryConcat(q, ` AND customer_id = ${customerId}`);
        }
        stream<Payment, sql:Error?> rows = db->query(sql:queryConcat(q, ` ORDER BY created_at DESC LIMIT 200`));
        return from Payment p in rows select p;
    }

    isolated resource function get summary() returns PaymentSummary[]|error {
        stream<PaymentSummary, sql:Error?> rows = db->query(`
            SELECT status, COUNT(*) AS count, COALESCE(SUM(amount), 0) AS amount FROM payments GROUP BY status`);
        return from PaymentSummary s in rows select s;
    }

    isolated resource function get [string paymentId]() returns Payment|http:NotFound|error {
        Payment|sql:Error p = db->queryRow(sql:queryConcat(paymentSelect(), ` WHERE id = ${paymentId}`));
        if p is sql:NoRowsError {
            return errNotFound(string `No payment with id ${paymentId}`);
        }
        return p;
    }

    isolated resource function get orders/[string orderId]() returns Payment|http:NotFound|error {
        Payment|sql:Error p = db->queryRow(sql:queryConcat(paymentSelect(), ` WHERE order_id = ${orderId}`));
        if p is sql:NoRowsError {
            return errNotFound(string `No payment for order ${orderId}`);
        }
        return p;
    }
}

// ---------- Kafka consumer --------------------------------------------------
listener kafka:Listener eventListener = new (kafkaBootstrap, {
    groupId: SERVICE_NAME,
    topics: [TOPIC_PAYMENTS_REQUESTED, TOPIC_ORDERS_CANCELLED],
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
        TOPIC_PAYMENTS_REQUESTED => {
            check processPayment(check payload.cloneWithType());
        }
        TOPIC_ORDERS_CANCELLED => {
            check onOrderCancelled(check payload.cloneWithType());
        }
    }
}

isolated function processPayment(PaymentRequestedEvent req) returns error? {
    int existing = check db->queryRow(`SELECT COUNT(*) FROM payments WHERE order_id = ${req.orderId}`);
    if existing > 0 {
        log:printInfo("payment already exists for order - skipping", orderId = req.orderId);
        return;
    }
    string paymentId = uuid:createType4AsString();
    _ = check db->execute(`
        INSERT INTO payments (id, order_id, customer_id, amount, method, status)
        VALUES (${paymentId}, ${req.orderId}, ${req.customerId}, ${req.amount}, ${req.paymentMethod}, 'PENDING')`);

    // simulate gateway latency
    runtime:sleep(processingDelaySeconds);

    string status = "COMPLETED";
    string? reason = ();
    string? txnRef = ();
    if req.amount <= 0d {
        status = "FAILED";
        reason = "INVALID_AMOUNT: amount must be greater than zero";
    } else if req.amount > maxTransactionAmount {
        status = "FAILED";
        reason = string `LIMIT_EXCEEDED: amount N$${req.amount} exceeds N$${maxTransactionAmount}`;
    } else if req.paymentMethod != "CASH" && random:createDecimal() < failureRate {
        status = "FAILED";
        reason = "CARD_DECLINED: issuer declined the transaction";
    } else {
        txnRef = req.paymentMethod == "CASH" ? "COD-" + req.orderId.substring(0, 8).toUpperAscii()
            : "TXN-" + uuid:createType4AsString().substring(0, 12).toUpperAscii();
    }

    // Only finalise if still PENDING (the order may have been cancelled meanwhile)
    sql:ExecutionResult res = check db->execute(`
        UPDATE payments SET status = ${status}, failure_reason = ${reason}, transaction_ref = ${txnRef}
         WHERE id = ${paymentId} AND status = 'PENDING'`);
    if res.affectedRowCount == 0 {
        log:printInfo("payment superseded by cancellation", orderId = req.orderId);
        return;
    }
    PaymentResultEvent ev = {
        ...newMeta(status == "COMPLETED" ? "PaymentCompleted" : "PaymentFailed"),
        orderId: req.orderId,
        paymentId,
        customerId: req.customerId,
        amount: req.amount,
        paymentMethod: req.paymentMethod,
        status,
        reason,
        transactionRef: txnRef
    };
    check publish(status == "COMPLETED" ? TOPIC_PAYMENTS_COMPLETED : TOPIC_PAYMENTS_FAILED, req.orderId, ev);
    log:printInfo("payment processed", orderId = req.orderId, status = status, amount = req.amount);
}

// Compensating action (saga): refund a completed payment when the order is
// cancelled. If no payment exists yet, a CANCELLED tombstone stops a late charge.
isolated function onOrderCancelled(OrderCancelledEvent e) returns error? {
    Payment|sql:Error found = db->queryRow(sql:queryConcat(paymentSelect(), ` WHERE order_id = ${e.orderId}`));
    if found is sql:NoRowsError {
        _ = check db->execute(`
            INSERT IGNORE INTO payments (id, order_id, customer_id, amount, method, status, failure_reason)
            VALUES (${uuid:createType4AsString()}, ${e.orderId}, ${e.customerId}, 0, 'NONE', 'CANCELLED', ${e.reason})`);
        return;
    }
    Payment p = check found;
    if p.status == "PENDING" {
        _ = check db->execute(`UPDATE payments SET status = 'CANCELLED', failure_reason = ${e.reason} WHERE id = ${p.id}`);
        return;
    }
    if p.status != "COMPLETED" {
        return;
    }
    _ = check db->execute(`UPDATE payments SET status = 'REFUNDED', failure_reason = ${e.reason} WHERE id = ${p.id}`);
    PaymentResultEvent ev = {
        ...newMeta("PaymentRefunded"),
        orderId: p.orderId,
        paymentId: p.id,
        customerId: p.customerId,
        amount: p.amount,
        paymentMethod: p.method,
        status: "REFUNDED",
        reason: e.reason,
        transactionRef: p.transactionRef
    };
    check publish(TOPIC_PAYMENTS_REFUNDED, p.orderId, ev);
    log:printInfo("payment refunded", orderId = p.orderId, amount = p.amount);
}
