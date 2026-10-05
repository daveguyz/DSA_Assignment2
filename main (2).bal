import ballerina/http;
import ballerinax/mongodb;
import ballerinax/kafka;

type Order record {|
    int id;
    int customerId;
    int restaurantId;
    string item;
    float amount;
    string status;
|};

configurable string mongoHost = "localhost";
mongodb:Client mongoClient;
mongodb:Database orderDb;
mongodb:Collection orderCollection;

kafka:Producer kafkaProducer;

function init() returns error? {
    // Connect to MongoDB
    mongoClient = check new ({
        connection: {
            serverAddress: {
                host: mongoHost,
                port: 27017
            }
        }
    });

    orderDb = check mongoClient->getDatabase("fooddelivery");
    orderCollection = check orderDb->getCollection("orders");

    // Connect to Kafka
    kafkaProducer = check new (
        kafka:DEFAULT_URL,
        {
            clientId: "order-service",
            acks: "all",
            retryCount: 3
        }
    );
}

// Check whether an order can move from its current status to the new status
function isValidTransition(string currentStatus, string newStatus)
        returns boolean {

    if newStatus == "CANCELLED" {
        return currentStatus != "DELIVERED" && currentStatus != "CANCELLED";
    }

    match currentStatus {
        "CREATED" => {
            return newStatus == "CONFIRMED";
        }
        "CONFIRMED" => {
            return newStatus == "PREPARING";
        }
        "PREPARING" => {
            return newStatus == "READY";
        }
        "READY" => {
            return newStatus == "OUT_FOR_DELIVERY";
        }
        "OUT_FOR_DELIVERY" => {
            return newStatus == "DELIVERED";
        }
        "DELIVERED" => {
            return false;
        }
        "CANCELLED" => {
            return false;
        }
        _ => {
            return false;
        }
    }
}

listener http:Listener orderListener = new (8083);

service /orders on orderListener {

    // GET all orders
    resource function get .() returns Order[]|error {
        stream<Order, error?> result = check orderCollection->find(
            {},
            {},
            (),
            Order
        );

        Order[] orders = [];

        record {| Order value; |}|error? item = result.next();

        while item is record {| Order value; |} {
            orders.push(item.value);
            item = result.next();
        }

        check result.close();

        return orders;
    }

    // POST a new order
    resource function post .(Order newOrder) returns Order|error {

        // New orders must start as CREATED
        if newOrder.status != "CREATED" {
            return error("New orders must start with status CREATED");
        }

        // Save order to MongoDB
        check orderCollection->insertOne(newOrder, {});

        // Create Kafka event
        string eventMessage =
            string `Order ${newOrder.id} created for customer ${newOrder.customerId}`;

        // Publish event to Kafka
        check kafkaProducer->send({
            topic: "orders.created",
            key: newOrder.id.toString().toBytes(),
            value: eventMessage.toBytes()
        });

        return newOrder;
    }

    // PUT order status
    resource function put [int id]/status(string newStatus)
            returns Order|error {

        stream<Order, error?> result = check orderCollection->find(
            {"id": id},
            {},
            (),
            Order
        );

        record {| Order value; |}|error? item = result.next();

        check result.close();

        if item is record {| Order value; |} {

            string currentStatus = item.value.status;

            // Check whether the requested transition is allowed
            if !isValidTransition(currentStatus, newStatus) {
                return error(
                    string `Invalid order status transition: ${currentStatus} -> ${newStatus}`
                );
            }

            Order updatedOrder = {
                id: item.value.id,
                customerId: item.value.customerId,
                restaurantId: item.value.restaurantId,
                item: item.value.item,
                amount: item.value.amount,
                status: newStatus
            };

            _ = check orderCollection->updateOne(
                {"id": id},
                {set: {"status": newStatus}},
                {}
            );

            return updatedOrder;
        }

        return error("Order not found");
    }
}
