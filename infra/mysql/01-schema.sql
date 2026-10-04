-- =====================================================================
--  NamDeliver - persistence layer
--  Pattern: DATABASE-PER-SERVICE. One MySQL server (for laptop friendliness)
--  but every microservice owns a private schema and a private DB user that
--  is GRANTed rights on that schema only. No service can read another
--  service's tables - data is shared exclusively through Kafka events.
--
--  Every schema has a processed_events table used for idempotent consumption
--  (Kafka delivers at-least-once).
-- =====================================================================

SET NAMES utf8mb4;

-- ---------------------------------------------------------------------
-- 1. CUSTOMER SERVICE
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS customer_db;
USE customer_db;

CREATE TABLE customers (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    full_name   VARCHAR(120) NOT NULL,
    email       VARCHAR(160) NOT NULL UNIQUE,
    phone       VARCHAR(30)  NOT NULL,
    created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE addresses (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    customer_id  INT          NOT NULL,
    label        VARCHAR(40)  NOT NULL,
    street       VARCHAR(200) NOT NULL,
    suburb       VARCHAR(80)  NOT NULL,
    city         VARCHAR(80)  NOT NULL DEFAULT 'Windhoek',
    latitude     DOUBLE       NOT NULL,
    longitude    DOUBLE       NOT NULL,
    is_default   BOOLEAN      NOT NULL DEFAULT FALSE,
    CONSTRAINT fk_address_customer FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE CASCADE,
    INDEX idx_address_customer (customer_id)
);

-- read model fed by orders.created / orders.status.changed
CREATE TABLE order_history (
    order_id         VARCHAR(36)   PRIMARY KEY,
    customer_id      INT           NOT NULL,
    restaurant_id    INT           NULL,
    restaurant_name  VARCHAR(120)  NULL,
    total            DECIMAL(10,2) NOT NULL DEFAULT 0,
    status           VARCHAR(30)   NOT NULL,
    placed_at        TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_history_customer (customer_id, placed_at)
);

CREATE TABLE processed_events (
    event_id      VARCHAR(64) PRIMARY KEY,
    topic         VARCHAR(80) NOT NULL,
    processed_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO customers (full_name, email, phone) VALUES
    ('Ndapewa Shikongo', 'ndapewa@example.na', '+264811234567'),
    ('Johannes van Wyk', 'johannes@example.na', '+264812345678'),
    ('Maria Nghifikwa',  'maria@example.na',    '+264813456789');

INSERT INTO addresses (customer_id, label, street, suburb, latitude, longitude, is_default) VALUES
    (1, 'Home',   '12 Hosea Kutako Drive', 'Katutura',      -22.5235, 17.0585, TRUE),
    (1, 'Work',   'NUST, 13 Jackson Kaujeua St', 'Windhoek West', -22.5650, 17.0760, FALSE),
    (2, 'Home',   '7 Nelson Mandela Ave',  'Klein Windhoek', -22.5705, 17.1025, TRUE),
    (3, 'Home',   '45 Omuramba Road',      'Eros',           -22.5480, 17.0960, TRUE),
    (3, 'Campus', 'UNAM Main Campus',      'Pionierspark',   -22.6110, 17.0590, FALSE);

-- ---------------------------------------------------------------------
-- 2. RESTAURANT SERVICE
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS restaurant_db;
USE restaurant_db;

CREATE TABLE restaurants (
    id                INT AUTO_INCREMENT PRIMARY KEY,
    name              VARCHAR(120) NOT NULL,
    cuisine           VARCHAR(60)  NOT NULL,
    phone             VARCHAR(30)  NOT NULL,
    street            VARCHAR(200) NOT NULL,
    suburb            VARCHAR(80)  NOT NULL,
    latitude          DOUBLE       NOT NULL,
    longitude         DOUBLE       NOT NULL,
    accepting_orders  BOOLEAN      NOT NULL DEFAULT TRUE,
    rating            DECIMAL(2,1) NOT NULL DEFAULT 4.0,
    created_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE opening_hours (
    restaurant_id  INT     NOT NULL,
    day_of_week    TINYINT NOT NULL,          -- 0 = Sunday ... 6 = Saturday
    open_time      TIME    NOT NULL,
    close_time     TIME    NOT NULL,
    PRIMARY KEY (restaurant_id, day_of_week),
    CONSTRAINT fk_hours_restaurant FOREIGN KEY (restaurant_id) REFERENCES restaurants (id) ON DELETE CASCADE,
    CONSTRAINT chk_day CHECK (day_of_week BETWEEN 0 AND 6)
);

CREATE TABLE menu_items (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    restaurant_id  INT           NOT NULL,
    name           VARCHAR(120)  NOT NULL,
    description    VARCHAR(255)  NOT NULL DEFAULT '',
    category       VARCHAR(60)   NOT NULL DEFAULT 'Mains',
    price          DECIMAL(10,2) NOT NULL,
    stock          INT           NOT NULL DEFAULT 0,
    available      BOOLEAN       NOT NULL DEFAULT TRUE,
    updated_at     TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_menu_restaurant FOREIGN KEY (restaurant_id) REFERENCES restaurants (id) ON DELETE CASCADE,
    CONSTRAINT chk_stock CHECK (stock >= 0),
    CONSTRAINT chk_price CHECK (price > 0),
    INDEX idx_menu_restaurant (restaurant_id)
);

-- kitchen queue + record of reserved stock (needed for restocking on cancel)
CREATE TABLE kitchen_orders (
    order_id       VARCHAR(36)   PRIMARY KEY,
    restaurant_id  INT           NOT NULL,
    customer_id    INT           NOT NULL,
    status         VARCHAR(20)   NOT NULL,  -- RESERVED, CONFIRMED, PREPARING, READY, CANCELLED
    items          TEXT          NOT NULL,  -- JSON array of priced items
    subtotal       DECIMAL(10,2) NOT NULL DEFAULT 0,
    created_at     TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_kitchen_restaurant (restaurant_id, status)
);

CREATE TABLE processed_events (
    event_id      VARCHAR(64) PRIMARY KEY,
    topic         VARCHAR(80) NOT NULL,
    processed_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO restaurants (name, cuisine, phone, street, suburb, latitude, longitude, rating) VALUES
    ('Kapana Corner Grill',      'Namibian',   '+264612000001', 'Single Quarters Market',  'Katutura',       -22.5262, 17.0618, 4.7),
    ('Eros Wood-fired Pizza',    'Italian',    '+264612000002', '3 Omuramba Road',         'Eros',           -22.5470, 17.0940, 4.4),
    ('Klein Windhoek Sushi Bar', 'Japanese',   '+264612000003', '22 Sam Nujoma Drive',     'Klein Windhoek', -22.5715, 17.0990, 4.6),
    ('Mama Ndapewa''s Kitchen',  'Home-style', '+264612000004', '9 Otjomuise Road',        'Khomasdal',      -22.5530, 17.0510, 4.5),
    ('Olympia Burger Shack',     'Burgers',    '+264612000005', '15 Joseph Mukwayu Ithana St', 'Olympia',    -22.5890, 17.0870, 4.2);

-- Restaurants 1,2,4,5 open 24/7 for demos; the sushi bar keeps real hours
-- (11:00-22:00) to demonstrate opening-hours validation.
INSERT INTO opening_hours (restaurant_id, day_of_week, open_time, close_time)
SELECT r.id, d.dow, '00:00:00', '23:59:59'
  FROM restaurants r
  CROSS JOIN (SELECT 0 AS dow UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6) d
 WHERE r.id IN (1, 2, 4, 5);
INSERT INTO opening_hours (restaurant_id, day_of_week, open_time, close_time)
SELECT 3, d.dow, '11:00:00', '22:00:00'
  FROM (SELECT 0 AS dow UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6) d;

INSERT INTO menu_items (restaurant_id, name, description, category, price, stock) VALUES
    (1, 'Kapana Platter',        'Flame-grilled beef strips with salsa & fat cakes', 'Mains',    85.00, 40),
    (1, 'Kapana Single',         'Classic street kapana with chilli salt',           'Mains',    45.00, 60),
    (1, 'Fat Cakes (6)',         'Golden vetkoek, six pieces',                       'Sides',    25.00, 80),
    (1, 'Oshikundu',             'Traditional fermented millet drink',               'Drinks',   20.00, 30),
    (2, 'Margherita',            'Tomato, mozzarella, basil',                        'Pizza',    95.00, 30),
    (2, 'Game Pizza',            'Kudu, springbok salami, peppadew',                 'Pizza',   145.00, 20),
    (2, 'Garlic Bread',          'Wood-fired garlic focaccia',                       'Sides',    40.00, 50),
    (2, 'Lemonade',              'Home-made',                                        'Drinks',   30.00, 3),
    (3, 'Salmon Roses (4)',      'Fresh salmon roses',                               'Sushi',   120.00, 25),
    (3, 'California Roll (8)',   'Crab, avocado, cucumber',                          'Sushi',    95.00, 30),
    (3, 'Miso Soup',             'Tofu, wakame',                                     'Starters', 35.00, 40),
    (4, 'Mahangu Pap & Beef Stew','Traditional pap with slow-cooked beef',           'Mains',    70.00, 35),
    (4, 'Chicken & Rice',        'Braised chicken, yellow rice, veg',                'Mains',    65.00, 35),
    (4, 'Spinach (Ekaka)',       'Wild spinach side',                                'Sides',    20.00, 50),
    (5, 'Classic Cheeseburger',  'Beef patty, cheddar, pickles',                     'Burgers',  89.00, 40),
    (5, 'Oryx Burger',           'Grilled oryx patty, onion jam',                    'Burgers', 125.00, 15),
    (5, 'Chips',                 'Hand-cut chips',                                   'Sides',    30.00, 100),
    (5, 'Milkshake',             'Vanilla, chocolate or strawberry',                 'Drinks',   45.00, 0);

-- ---------------------------------------------------------------------
-- 3. ORDER SERVICE
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS order_db;
USE order_db;

CREATE TABLE orders (
    id                VARCHAR(36)   PRIMARY KEY,
    customer_id       INT           NOT NULL,
    restaurant_id     INT           NOT NULL,
    restaurant_name   VARCHAR(120)  NULL,
    status            VARCHAR(20)   NOT NULL,
    payment_method    VARCHAR(20)   NOT NULL,
    subtotal          DECIMAL(10,2) NOT NULL DEFAULT 0,
    delivery_fee      DECIMAL(10,2) NOT NULL DEFAULT 0,
    surge_multiplier  DECIMAL(4,2)  NOT NULL DEFAULT 1.00,
    total             DECIMAL(10,2) NOT NULL DEFAULT 0,
    delivery_address  VARCHAR(255)  NOT NULL,
    delivery_lat      DOUBLE        NOT NULL,
    delivery_lng      DOUBLE        NOT NULL,
    restaurant_lat    DOUBLE        NULL,
    restaurant_lng    DOUBLE        NULL,
    driver_id         INT           NULL,
    notes             VARCHAR(255)  NULL,
    cancel_reason     VARCHAR(255)  NULL,
    created_at        TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT chk_order_status CHECK (status IN
        ('CREATED','CONFIRMED','PREPARING','READY','OUT_FOR_DELIVERY','DELIVERED','CANCELLED')),
    INDEX idx_orders_customer (customer_id, created_at),
    INDEX idx_orders_restaurant (restaurant_id, status),
    INDEX idx_orders_status (status, created_at)
);

CREATE TABLE order_items (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    order_id      VARCHAR(36)   NOT NULL,
    menu_item_id  INT           NOT NULL,
    name          VARCHAR(120)  NULL,
    quantity      INT           NOT NULL,
    unit_price    DECIMAL(10,2) NOT NULL DEFAULT 0,
    CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE,
    CONSTRAINT chk_qty CHECK (quantity > 0)
);

-- full audit trail of the state machine
CREATE TABLE order_status_history (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id     VARCHAR(36)  NOT NULL,
    from_status  VARCHAR(20)  NULL,
    to_status    VARCHAR(20)  NOT NULL,
    reason       VARCHAR(255) NULL,
    changed_at   TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    CONSTRAINT fk_history_order FOREIGN KEY (order_id) REFERENCES orders (id) ON DELETE CASCADE,
    INDEX idx_history_order (order_id)
);

CREATE TABLE processed_events (
    event_id      VARCHAR(64) PRIMARY KEY,
    topic         VARCHAR(80) NOT NULL,
    processed_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------
-- 4. PAYMENT SERVICE
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS payment_db;
USE payment_db;

CREATE TABLE payments (
    id               VARCHAR(36)   PRIMARY KEY,
    order_id         VARCHAR(36)   NOT NULL UNIQUE,   -- one payment per order (idempotency)
    customer_id      INT           NOT NULL,
    amount           DECIMAL(10,2) NOT NULL,
    method           VARCHAR(20)   NOT NULL,
    status           VARCHAR(20)   NOT NULL,          -- PENDING, COMPLETED, FAILED, REFUNDED, CANCELLED
    failure_reason   VARCHAR(255)  NULL,
    transaction_ref  VARCHAR(40)   NULL,
    created_at       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_payments_status (status)
);

CREATE TABLE processed_events (
    event_id      VARCHAR(64) PRIMARY KEY,
    topic         VARCHAR(80) NOT NULL,
    processed_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------
-- 5. DELIVERY SERVICE
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS delivery_db;
USE delivery_db;

CREATE TABLE drivers (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(120) NOT NULL,
    phone       VARCHAR(30)  NOT NULL,
    vehicle     VARCHAR(40)  NOT NULL DEFAULT 'Motorbike',
    status      VARCHAR(20)  NOT NULL DEFAULT 'OFFLINE',   -- AVAILABLE, BUSY, OFFLINE
    latitude    DOUBLE       NOT NULL,
    longitude   DOUBLE       NOT NULL,
    updated_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_drivers_status (status)
);

CREATE TABLE deliveries (
    id                VARCHAR(36)  PRIMARY KEY,
    order_id          VARCHAR(36)  NOT NULL UNIQUE,
    customer_id       INT          NOT NULL,
    restaurant_id     INT          NOT NULL,
    restaurant_name   VARCHAR(120) NULL,
    delivery_address  VARCHAR(255) NULL,
    driver_id         INT          NULL,
    status            VARCHAR(30)  NOT NULL, -- AWAITING_DETAILS, PENDING_ASSIGNMENT, ASSIGNED, PICKED_UP, DELIVERED, CANCELLED
    order_ready       BOOLEAN      NOT NULL DEFAULT FALSE,
    restaurant_lat    DOUBLE       NOT NULL DEFAULT 0,
    restaurant_lng    DOUBLE       NOT NULL DEFAULT 0,
    customer_lat      DOUBLE       NOT NULL DEFAULT 0,
    customer_lng      DOUBLE       NOT NULL DEFAULT 0,
    route_json        TEXT         NULL,
    distance_km       DOUBLE       NULL,
    eta_minutes       INT          NULL,
    assigned_at       TIMESTAMP    NULL,
    picked_up_at      TIMESTAMP    NULL,
    delivered_at      TIMESTAMP    NULL,
    created_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_delivery_driver FOREIGN KEY (driver_id) REFERENCES drivers (id),
    INDEX idx_deliveries_status (status, created_at),
    INDEX idx_deliveries_driver (driver_id, status)
);

CREATE TABLE location_history (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    driver_id    INT          NOT NULL,
    order_id     VARCHAR(36)  NULL,
    latitude     DOUBLE       NOT NULL,
    longitude    DOUBLE       NOT NULL,
    phase        VARCHAR(20)  NOT NULL,
    recorded_at  TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_location_driver (driver_id, recorded_at)
);

CREATE TABLE processed_events (
    event_id      VARCHAR(64) PRIMARY KEY,
    topic         VARCHAR(80) NOT NULL,
    processed_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO drivers (name, phone, vehicle, status, latitude, longitude) VALUES
    ('Petrus Amutenya',  '+264814000001', 'Motorbike', 'AVAILABLE', -22.5609, 17.0836),
    ('Selma Hamutenya',  '+264814000002', 'Motorbike', 'AVAILABLE', -22.5250, 17.0600),
    ('Kevin Beukes',     '+264814000003', 'Car',       'AVAILABLE', -22.5895, 17.0860),
    ('Hilma Nangolo',    '+264814000004', 'Scooter',   'AVAILABLE', -22.5710, 17.1010),
    ('Tjitjo Kandjii',   '+264814000005', 'Bicycle',   'OFFLINE',   -22.5540, 17.0500);

-- ---------------------------------------------------------------------
-- 6. NOTIFICATION SERVICE
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS notification_db;
USE notification_db;

CREATE TABLE notifications (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    recipient_type  VARCHAR(20)  NOT NULL,   -- CUSTOMER, RESTAURANT, DRIVER, ADMIN
    recipient_id    INT          NOT NULL,
    channel         VARCHAR(10)  NOT NULL,   -- SMS, EMAIL, PUSH
    event_type      VARCHAR(80)  NOT NULL,
    order_id        VARCHAR(36)  NULL,
    title           VARCHAR(120) NOT NULL,
    message         VARCHAR(500) NOT NULL,
    status          VARCHAR(10)  NOT NULL,
    created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_notifications_recipient (recipient_type, recipient_id, id),
    INDEX idx_notifications_order (order_id)
);

CREATE TABLE processed_events (
    event_id      VARCHAR(64) PRIMARY KEY,
    topic         VARCHAR(80) NOT NULL,
    processed_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------
-- 7. ADMIN SERVICE (analytical read model - CQRS)
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS admin_db;
USE admin_db;

CREATE TABLE order_facts (
    order_id             VARCHAR(36)   PRIMARY KEY,
    customer_id          INT           NOT NULL,
    restaurant_id        INT           NOT NULL,
    restaurant_name      VARCHAR(120)  NULL,
    status               VARCHAR(20)   NOT NULL,
    total                DECIMAL(10,2) NOT NULL DEFAULT 0,
    cancel_reason        VARCHAR(255)  NULL,
    created_at           DATETIME(3)   NOT NULL,
    confirmed_at         DATETIME(3)   NULL,
    preparing_at         DATETIME(3)   NULL,
    ready_at             DATETIME(3)   NULL,
    out_for_delivery_at  DATETIME(3)   NULL,
    delivered_at         DATETIME(3)   NULL,
    cancelled_at         DATETIME(3)   NULL,
    INDEX idx_facts_restaurant (restaurant_id),
    INDEX idx_facts_created (created_at)
);

CREATE TABLE delivery_facts (
    order_id      VARCHAR(36)  PRIMARY KEY,
    driver_id     INT          NULL,
    driver_name   VARCHAR(120) NULL,
    distance_km   DOUBLE       NULL,
    eta_minutes   INT          NULL,
    assigned_at   DATETIME(3)  NULL,
    picked_up_at  DATETIME(3)  NULL,
    delivered_at  DATETIME(3)  NULL,
    INDEX idx_dfacts_driver (driver_id)
);

CREATE TABLE processed_events (
    event_id      VARCHAR(64) PRIMARY KEY,
    topic         VARCHAR(80) NOT NULL,
    processed_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------
-- Service accounts: least privilege, one schema each
-- ---------------------------------------------------------------------
CREATE USER IF NOT EXISTS 'customer_svc'@'%'     IDENTIFIED WITH mysql_native_password BY 'customer_pass';
CREATE USER IF NOT EXISTS 'restaurant_svc'@'%'   IDENTIFIED WITH mysql_native_password BY 'restaurant_pass';
CREATE USER IF NOT EXISTS 'order_svc'@'%'        IDENTIFIED WITH mysql_native_password BY 'order_pass';
CREATE USER IF NOT EXISTS 'payment_svc'@'%'      IDENTIFIED WITH mysql_native_password BY 'payment_pass';
CREATE USER IF NOT EXISTS 'delivery_svc'@'%'     IDENTIFIED WITH mysql_native_password BY 'delivery_pass';
CREATE USER IF NOT EXISTS 'notification_svc'@'%' IDENTIFIED WITH mysql_native_password BY 'notification_pass';
CREATE USER IF NOT EXISTS 'admin_svc'@'%'        IDENTIFIED WITH mysql_native_password BY 'admin_pass';

GRANT SELECT, INSERT, UPDATE, DELETE ON customer_db.*     TO 'customer_svc'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON restaurant_db.*   TO 'restaurant_svc'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON order_db.*        TO 'order_svc'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON payment_db.*      TO 'payment_svc'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON delivery_db.*     TO 'delivery_svc'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON notification_db.* TO 'notification_svc'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON admin_db.*        TO 'admin_svc'@'%';
FLUSH PRIVILEGES;
