import ballerina/http;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/log;

configurable string mongoHost = "localhost";
configurable string kafkaUrl = "localhost:9092";

type Payment record {|
    int id;
    int orderId;
    float amount;
    string status;
|};

mongodb:Client mongoClient;
mongodb:Database paymentDb;
mongodb:Collection paymentCollection;

kafka:Producer kafkaProducer;

function init() returns error? {
    mongoClient = check new ({
        connection: {
            serverAddress: {
                host: mongoHost,
                port: 27017
            }
        }
    });

    paymentDb = check mongoClient->getDatabase("fooddelivery");
    paymentCollection = check paymentDb->getCollection("payments");

    kafkaProducer = check new (
        kafkaUrl,
        {
            clientId: "payment-service",
            acks: "all",
            retryCount: 3
        }
    );
}

kafka:ConsumerConfiguration consumerConfiguration = {
    groupId: "payment-service-group",
    offsetReset: "earliest",
    topics: ["orders.created"]
};

listener kafka:Listener kafkaListener = new (
    kafkaUrl,
    consumerConfiguration
);

listener http:Listener paymentListener = new (8084);

service on kafkaListener {

    remote function onConsumerRecord(
        kafka:Caller caller,
        kafka:BytesConsumerRecord[] records
    ) {
        foreach kafka:BytesConsumerRecord kafkaRecord in records {

            string message = checkpanic string:fromBytes(
                kafkaRecord.value
            );

            log:printInfo(
                string `Payment Service received: ${message}`
            );
        }
    }
}

service /payments on paymentListener {

    resource function get .() returns Payment[]|error {

        stream<Payment, error?> result = check paymentCollection->find(
            {},
            {},
            (),
            Payment
        );

        Payment[] payments = [];

        record {| Payment value; |}|error? item = result.next();

        while item is record {| Payment value; |} {
            payments.push(item.value);
            item = result.next();
        }

        check result.close();

        return payments;
    }

    resource function post .(Payment newPayment) returns Payment|error {

        check paymentCollection->insertOne(
            newPayment,
            {}
        );

        return newPayment;
    }

    resource function put [int id]/process() returns Payment|error {

        stream<Payment, error?> result = check paymentCollection->find(
            {"id": id},
            {},
            (),
            Payment
        );

        record {| Payment value; |}|error? item = result.next();

        check result.close();

        if item is record {| Payment value; |} {

            Payment updatedPayment = {
                id: item.value.id,
                orderId: item.value.orderId,
                amount: item.value.amount,
                status: "COMPLETED"
            };

            _ = check paymentCollection->updateOne(
                {"id": id},
                {set: {"status": "COMPLETED"}},
                {}
            );

            string eventMessage =
                string `Payment ${id} completed for order ${item.value.orderId}`;

            check kafkaProducer->send({
                topic: "payments.completed",
                key: id.toString().toBytes(),
                value: eventMessage.toBytes()
            });

            check kafkaProducer->'flush();

            return updatedPayment;
        }

        return error("Payment not found");
    }
}
