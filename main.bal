import ballerina/http;
import ballerina/io;

type Order record {|
    int id;
    int customerId;
    int restaurantId;
    string item;
    float amount;
    string status;
|};

public function main() returns error? {

    // Ballerina client connects to the Order Service
    http:Client orderClient = check new ("http://localhost:8083");

    // Client sends a GET request to the Order Service
    Order[] orders = check orderClient->/orders;

    // Display the response received from the server
    io:println("Orders received from Order Service:");
    io:println(orders);
}
