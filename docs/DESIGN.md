# NamDeliver — Design Document

## 1. Problem and goals

The Ministry of Industrialisation and Trade needs a platform that lets local restaurants and
independent drivers serve customers reliably during peak meal times. The design goals are:

- **Loose coupling** – each actor (customer, restaurant, payment, driver) is an independent
  microservice with its own data; services communicate through Kafka events, not shared tables.
- **Reliability** – no lost or double-processed orders, even when a service crashes or Kafka
  redelivers messages.
- **Scalability** – partitioned topics and consumer groups let each service scale horizontally;
  per-order ordering is preserved through message keys.
- **Observability** – every service exposes health and Prometheus metrics; every order has a full
  audit trail.

## 2. Service boundaries

| Service | Bounded context | Sync API (REST) | Publishes | Consumes |
|---|---|---|---|---|
| Customer | Identity, addresses | `/customers` CRUD, addresses, order history | – | `orders.created`, `orders.status.changed` |
| Restaurant | Menu, inventory, hours, kitchen | `/restaurants`, menu, stock, hours, kitchen queue actions | `orders.validated`, `orders.rejected`, `kitchen.preparing`, `kitchen.ready` | `orders.created`, `orders.confirmed`, `orders.cancelled` |
| Order | Order aggregate + state machine, pricing | `/orders`, `/orders/{id}/cancel`, `/pricing/surge` | `orders.created`, `payments.requested`, `orders.confirmed`, `orders.cancelled`, `orders.status.changed` | `orders.validated`, `orders.rejected`, `payments.completed`, `payments.failed`, `kitchen.*`, `delivery.assigned`, `delivery.picked_up`, `delivery.completed` |
| Payment | Payments, refunds | `/payments`, `/payments/orders/{id}`, `/payments/summary` | `payments.completed`, `payments.failed`, `payments.refunded` | `payments.requested`, `orders.cancelled` |
| Delivery | Drivers, dispatch, routing, tracking | `/drivers`, `/drivers/stats`, `/deliveries/{orderId}`, pickup/complete, `/routes` | `delivery.assigned`, `delivery.picked_up`, `delivery.completed`, `delivery.location.updated` | `orders.confirmed`, `orders.status.changed` |
| Notification | Alerts | `/notifications`, `/notifications/stats` | – | status, payment, delivery, rejection, DLQ topics |
| Admin | Reporting (CQRS read model) | `/admin/reports/*`, `/admin/system/health` | – | `orders.created`, `orders.status.changed`, `delivery.*` |

Only two synchronous service-to-service calls exist, both on the order-placement path and both
with timeouts and graceful handling: Order → Customer (resolve the delivery address, returns
400/503 to the caller on failure) and Order → Delivery (driver supply for surge pricing, falls
back to "no driver data" if unreachable). Everything else is asynchronous.

## 3. Kafka design

### 3.1 Topics

| Topic | Partitions | Key | Producer | Purpose |
|---|---|---|---|---|
| `orders.created` | 3 | orderId | Order | new order placed |
| `orders.validated` | 3 | orderId | Restaurant | stock reserved, items priced |
| `orders.rejected` | 3 | orderId | Restaurant | closed / out of stock / unknown item |
| `payments.requested` | 3 | orderId | Order | charge customer |
| `payments.completed` | 3 | orderId | Payment | payment succeeded |
| `payments.failed` | 3 | orderId | Payment | payment declined |
| `payments.refunded` | 3 | orderId | Payment | compensation after cancel |
| `orders.confirmed` | 3 | orderId | Order | paid; kitchen + dispatch start |
| `orders.cancelled` | 3 | orderId | Order | triggers restock, refund, driver release |
| `orders.status.changed` | 3 | orderId | Order | every state transition (fan-out) |
| `kitchen.preparing` | 3 | orderId | Restaurant | kitchen started |
| `kitchen.ready` | 3 | orderId | Restaurant | food ready |
| `delivery.assigned` | 3 | orderId | Delivery | driver dispatched |
| `delivery.picked_up` | 3 | orderId | Delivery | driver collected food |
| `delivery.completed` | 3 | orderId | Delivery | delivered |
| `delivery.location.updated` | 6 | driverId | Delivery | GPS stream (high volume, 1-day retention) |
| `dlq.events` | 1 | original topic | any | poison messages after retries (30-day retention) |

Topics are created explicitly by the `kafka-init` container (`infra/kafka/create-topics.sh`);
broker auto-creation is **disabled** so a typo cannot silently create a topic.

### 3.2 Partitioning and ordering

Every order event is keyed by `orderId`. Kafka hashes the key to a partition, so all events of
one order are on the same partition and are consumed **in order**, while different orders are
spread over three partitions and can be processed in parallel by up to three instances of each
consumer group. GPS updates are keyed by `driverId` and get six partitions because they are the
highest-volume stream.

### 3.3 Producers

`shared/common.bal` configures one producer per service with `acks=all`, `enableIdempotence=true`
and 5 retries, so a broker hiccup cannot lose or duplicate a published event. Events are
published inside the same Ballerina `transaction` block as the database write; if publishing
fails the database change is rolled back and the client gets an error instead of a half-done
order.

### 3.4 Consumers

Each service has its own consumer group (`groupId = service name`), so every service receives
every event it subscribes to, and instances of the same service share partitions. The consumer
pipeline (`processRecords` in `shared/common.bal`):

1. **Manual offset commit** (`autoCommit: false`) – offsets are committed only after the whole
   batch was processed.
2. **Idempotent consumer** – the `eventId` of each event is checked against the service's
   `processed_events` table; duplicates from at-least-once delivery are skipped.
3. **Bounded retries** – a failing handler is retried 3 times with linear back-off.
4. **Dead-letter topic** – after the last retry the raw event goes to `dlq.events` (the
   notification service alerts the admin) so one poison message never blocks a partition.

### 3.5 Out-of-order delivery across topics

Ordering is only guaranteed within a partition, and different event types live on different
topics. The services handle the races explicitly:

- **Cancel overtakes create** – if `orders.cancelled` reaches the restaurant before
  `orders.created`, a `CANCELLED` tombstone row is written so the late create never reserves stock.
  Payment and delivery use the same tombstone technique.
- **READY overtakes confirmed** – delivery upserts a row in `AWAITING_DETAILS` and completes it
  when `orders.confirmed` arrives.
- **Late payment after cancel** – order-service ignores `payments.completed` for a cancelled order;
  payment-service refunds it when it processes `orders.cancelled`.

## 4. Order state machine

```
CREATED -> CONFIRMED -> PREPARING -> READY -> OUT_FOR_DELIVERY -> DELIVERED
   |           |
   +-----------+-----> CANCELLED
```

The order service is the **only** writer of order status. Allowed transitions are declared in a
read-only map (`TRANSITIONS`). Every transition:

1. checks the transition is legal (illegal or duplicate transitions are logged and ignored),
2. updates with an **optimistic lock** – `UPDATE ... WHERE id = ? AND status = <expected>` – so
   concurrent events cannot both win,
3. inserts a row in `order_status_history` (audit trail shown in the UI),
4. publishes `orders.status.changed`, plus `orders.confirmed` or `orders.cancelled` when relevant.

Customers may cancel while the order is `CREATED` or `CONFIRMED`; after that the kitchen has
started cooking.

### Saga and compensation

Order placement is a **choreographed saga**: Restaurant reserves stock → Payment charges →
Delivery dispatches. If a later step fails, earlier steps are compensated by reacting to
`orders.cancelled`: the restaurant restores stock, the payment service refunds, and the delivery
service releases the driver and re-dispatches queued orders.

## 5. Data model

One MySQL server hosts seven schemas, each with its own user that can only access that schema
(`GRANT ... ON order_db.* TO 'order_svc'`). In production each schema would move to its own
server without any code change.

| Schema | Tables | Notes |
|---|---|---|
| customer_db | customers, addresses, order_history, processed_events | `email` unique; `order_history` is a read model built from events |
| restaurant_db | restaurants, opening_hours, menu_items, kitchen_orders, processed_events | `CHECK (stock >= 0)`; stock reserved with `SELECT ... FOR UPDATE` in a transaction; `kitchen_orders.items` keeps what was reserved so it can be restored |
| order_db | orders, order_items, order_status_history, processed_events | `CHECK` on status values; indexes for customer/restaurant/status queries |
| payment_db | payments, processed_events | `order_id` unique → at most one charge per order |
| delivery_db | drivers, deliveries, location_history, processed_events | `order_id` unique; route stored as JSON |
| notification_db | notifications, processed_events | one row per recipient per channel |
| admin_db | order_facts, delivery_facts, processed_events | denormalised facts with per-stage timestamps for reporting |

## 6. Bonus features

**Surge pricing** (order-service `computeSurge`). Demand is active orders in the last 30 minutes plus
queued deliveries; supply is available drivers (from `GET /drivers/stats`). Multiplier steps:
ratio > 1 → ×1.2, > 2 → ×1.5, > 3 → ×1.8, no drivers → ×2.0; +0.2 during Windhoek peak meal
times (11:30–14:00, 17:30–20:30); capped at ×2.5. Delivery fee = (N$15 + N$5/km) × multiplier.

**Route optimisation** (delivery-service `routing.bal`). A weighted graph of 17 Windhoek suburbs
and 27 roads (arterials 45–60 km/h, Western Bypass 70 km/h). Edge weight is travel time. Dijkstra
finds the fastest path between the nodes nearest to the start and end; first/last mile at local
street speed. The trip is driver → restaurant → customer. `GET /routes` and `GET /routes/graph`
expose it.

**Nearest-driver dispatch.** Available drivers are sorted by distance to the restaurant and claimed
with an atomic `UPDATE drivers SET status='BUSY' WHERE id=? AND status='AVAILABLE'`, so two
dispatches can never take the same driver. Orders without a driver wait in `PENDING_ASSIGNMENT`
and are retried when a driver frees up and by a 15-second scheduled job.

**Driver location simulation.** After assignment the driver moves along the route to the restaurant,
and after pickup to the customer, in 250 m steps every 1.5 s. Each step updates the database,
appends to `location_history` and publishes `delivery.location.updated`. The UI map shows it live.
A real device can push GPS via `PUT /drivers/{id}/location`.

**Web UI.** Static HTML/JS served by nginx, which also acts as the API gateway (`/api/<resource>`
→ owning service). Four role views plus a live notification feed and Leaflet maps.

**Observability.** Ballerina's built-in observability (`observabilityIncluded = true` +
`ballerinax/prometheus`) exposes metrics on port 9797 of every service; Prometheus scrapes them
and Grafana is pre-provisioned with a datasource and a dashboard. `/health` on every service
checks database connectivity; `/admin/system/health` aggregates the whole platform.

## 7. Containerisation

- **Multi-stage Dockerfiles**: `ballerina/ballerina:2201.10.3` compiles the package, the runtime
  image is `eclipse-temurin:17-jre` with only the JAR and its config (smaller, no build tools).
- **Configuration** via `Config.docker.toml` → `/app/Config.toml` (Docker host names); the code
  defaults target localhost for development.
- **Startup ordering**: services wait for MySQL to be healthy and for `kafka-init` to finish
  creating topics (`service_completed_successfully`).
- **Self-healing**: `restart: unless-stopped` plus container `HEALTHCHECK` on `/health`.
- **Isolation**: one container per service on a private bridge network; each service talks to
  its own schema with its own credentials; JVM heap capped at 256 MB per service.
- **Persistence**: named volumes for Kafka and MySQL data.

## 8. REST API reference (via gateway `http://localhost:8080/api`)

| Method | Path | Description |
|---|---|---|
| GET/POST | `/customers` | list / register (with addresses) |
| GET/PUT | `/customers/{id}` | profile / update |
| GET/POST | `/customers/{id}/addresses` | list / add |
| GET/DELETE | `/customers/{id}/addresses/{aid}` | get / remove |
| GET | `/customers/{id}/orders` | order history |
| GET/POST | `/restaurants` | list (with `openNow`) / create |
| GET | `/restaurants/{id}` | details, hours, menu |
| PUT | `/restaurants/{id}/availability` | `{acceptingOrders}` |
| GET/PUT | `/restaurants/{id}/hours` | opening hours |
| GET/POST | `/restaurants/{id}/menu` | menu / add item |
| PUT | `/restaurants/{id}/menu/{itemId}` | edit price/name/availability |
| PUT | `/restaurants/{id}/menu/{itemId}/stock` | `{stock}` absolute or `{add}` restock |
| GET | `/restaurants/{id}/orders` | kitchen queue |
| POST | `/restaurants/{id}/orders/{orderId}/preparing` · `/ready` | kitchen progress |
| POST | `/orders` | place order `{customerId, restaurantId, addressId, items[{menuItemId, quantity}], paymentMethod}` → 202 |
| GET | `/orders?customerId=&restaurantId=&driverId=&status=` | search |
| GET | `/orders/{id}` | details + items + status history |
| POST | `/orders/{id}/cancel` | `{reason}` |
| GET | `/pricing/surge` | current surge multiplier |
| GET | `/payments`, `/payments/summary`, `/payments/orders/{orderId}` | payments |
| GET/POST | `/drivers` | list / register |
| GET | `/drivers/stats` | fleet supply |
| PUT | `/drivers/{id}/status` | `{status: AVAILABLE or OFFLINE}` |
| PUT | `/drivers/{id}/location` | GPS ping |
| GET | `/drivers/{id}/deliveries?active=true` | driver jobs |
| GET | `/deliveries`, `/deliveries/{orderId}` | tracking (status, driver position, route, ETA) |
| POST | `/deliveries/{orderId}/pickup` · `/complete` | `{driverId}` |
| GET | `/routes?fromLat=&fromLng=&toLat=&toLng=` · `/routes/graph` | route optimisation |
| GET | `/notifications?recipientType=&recipientId=&orderId=` · `/notifications/stats` | alerts |
| GET | `/admin/reports/summary` · `/restaurants` · `/deliveries` · `/hourly` | reports |
| GET | `/admin/system/health` | platform health |

## 9. Limitations and future work

- Single Kafka broker and single MySQL server to fit on a laptop; production would use a 3-broker
  cluster (replication factor 3, `min.insync.replicas=2`) and separate database servers.
- Events are published inside the DB transaction but not atomically with the commit; a
  transactional **outbox** table would close the small window where a commit fails after publishing.
- No authentication; production would put OAuth2/JWT on the gateway.
- Overnight opening hours (closing after midnight) are not modelled.
