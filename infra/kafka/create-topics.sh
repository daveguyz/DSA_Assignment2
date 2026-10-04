#!/bin/sh
# Creates every platform topic with an explicit partition count.
# Partitioning: messages are keyed by orderId (driverId for GPS pings), so all
# events of one order live on ONE partition => strict per-order ordering, while
# different orders are processed in parallel across partitions / consumers.
set -e
BS="${BOOTSTRAP:-kafka:9092}"
KT=/opt/kafka/bin/kafka-topics.sh

echo "Waiting for Kafka at $BS ..."
until $KT --bootstrap-server "$BS" --list >/dev/null 2>&1; do sleep 2; done

create() { # name partitions retention.ms
  $KT --bootstrap-server "$BS" --create --if-not-exists --topic "$1" \
      --partitions "$2" --replication-factor 1 --config retention.ms="$3"
}

WEEK=604800000
DAY=86400000
for t in orders.created orders.validated orders.rejected orders.confirmed orders.cancelled \
         orders.status.changed payments.requested payments.completed payments.failed \
         payments.refunded kitchen.preparing kitchen.ready delivery.assigned \
         delivery.picked_up delivery.completed; do
  create "$t" 3 "$WEEK"
done
create delivery.location.updated 6 "$DAY"   # high-volume GPS stream
create dlq.events 1 2592000000              # 30 days for investigation

echo "Topics:"
$KT --bootstrap-server "$BS" --describe | grep -E "^Topic:" || true
