#!/usr/bin/env bash
# =====================================================================
#  End-to-end demo of the full order life-cycle through the gateway.
#  Usage:  ./scripts/demo.sh            (stack must be running)
#  Requires: curl, jq
# =====================================================================
set -euo pipefail
GW="${GW:-http://localhost:8080/api}"
say()  { printf '\n\033[1;33m== %s\033[0m\n' "$*"; }
wait_status() { # orderId status
  for _ in $(seq 1 40); do
    s=$(curl -s "$GW/orders/$1" | jq -r .status)
    [ "$s" = "$2" ] && { echo "   order is $s"; return 0; }
    [ "$s" = "CANCELLED" ] && { echo "   order CANCELLED: $(curl -s "$GW/orders/$1" | jq -r .cancelReason)"; return 1; }
    sleep 1
  done
  echo "   timed out waiting for $2 (current: $s)"; return 1
}

say "Platform health"
curl -s "$GW/admin/system/health" | jq -r '.[] | "   \(.name): \(.status)"'

say "Surge pricing right now"
curl -s "$GW/pricing/surge" | jq .

say "1. Customer 1 orders 2x Kapana Platter + 1x Fat Cakes from Kapana Corner Grill (restaurant 1)"
ORDER=$(curl -s -X POST "$GW/orders" -H 'Content-Type: application/json' -d '{
  "customerId": 1, "restaurantId": 1, "addressId": 1, "paymentMethod": "CARD",
  "items": [ {"menuItemId": 1, "quantity": 2}, {"menuItemId": 3, "quantity": 1} ] }')
OID=$(echo "$ORDER" | jq -r .id)
echo "   orderId = $OID (status $(echo "$ORDER" | jq -r .status))"

say "2. Restaurant validates + reserves stock, payment is simulated -> CONFIRMED"
wait_status "$OID" CONFIRMED
curl -s "$GW/orders/$OID" | jq '{subtotal, deliveryFee, surgeMultiplier, total}'
curl -s "$GW/payments/orders/$OID" | jq '{status, amount, transactionRef}'

say "3. Delivery service dispatched the nearest driver"
sleep 2
curl -s "$GW/deliveries/$OID" | jq '{status, driverName, distanceKm, etaMinutes, via: .route.via}'
DRIVER=$(curl -s "$GW/deliveries/$OID" | jq -r .driverId)

say "4. Kitchen: start preparing, then mark ready"
curl -s -X POST "$GW/restaurants/1/orders/$OID/preparing" | jq -r '"   kitchen: \(.status)"'
wait_status "$OID" PREPARING
curl -s -X POST "$GW/restaurants/1/orders/$OID/ready" | jq -r '"   kitchen: \(.status)"'
wait_status "$OID" READY
sleep 2

say "5. Driver $DRIVER picks up -> OUT_FOR_DELIVERY (location simulation starts)"
curl -s -X POST "$GW/deliveries/$OID/pickup" -H 'Content-Type: application/json' -d "{\"driverId\": $DRIVER}" | jq -r '"   delivery: \(.status)"'
wait_status "$OID" OUT_FOR_DELIVERY
sleep 4
curl -s "$GW/deliveries/$OID" | jq '{driverLat, driverLng, etaMinutes}'

say "6. Driver completes -> DELIVERED"
curl -s -X POST "$GW/deliveries/$OID/complete" -H 'Content-Type: application/json' -d "{\"driverId\": $DRIVER}" | jq -r '"   delivery: \(.status)"'
wait_status "$OID" DELIVERED

say "Full state-machine audit trail"
curl -s "$GW/orders/$OID" | jq -r '.history[] | "   \(.changedAt)  \(.fromStatus // "-") -> \(.toStatus)  (\(.reason // ""))"'

say "Notifications sent for this order"
curl -s "$GW/notifications?orderId=$OID&limit=50" | jq -r '.[] | "   [\(.channel)] \(.recipientType)#\(.recipientId): \(.title)"'

say "7. Failure path: order above the N\$1500 payment limit is declined -> CANCELLED + stock restored"
BIG=$(curl -s -X POST "$GW/orders" -H 'Content-Type: application/json' -d '{
  "customerId": 2, "restaurantId": 2, "addressId": 3, "paymentMethod": "CARD",
  "items": [ {"menuItemId": 6, "quantity": 12} ] }' | jq -r .id)
wait_status "$BIG" CANCELLED || true

say "8. Rejection path: out-of-stock item (Milkshake has 0 stock)"
REJ=$(curl -s -X POST "$GW/orders" -H 'Content-Type: application/json' -d '{
  "customerId": 3, "restaurantId": 5, "addressId": 4, "items": [ {"menuItemId": 18, "quantity": 1} ] }' | jq -r .id)
wait_status "$REJ" CANCELLED || true

say "Admin reports"
sleep 2
curl -s "$GW/admin/reports/summary" | jq .summary
curl -s "$GW/admin/reports/restaurants" | jq -r '.[] | "   \(.restaurantName): \(.totalOrders) orders, revenue N$\(.revenue)"'
curl -s "$GW/admin/reports/deliveries" | jq -r '.[] | "   \(.driverName): \(.completed) delivered, on-time \(.onTimeRate)%"'
echo
