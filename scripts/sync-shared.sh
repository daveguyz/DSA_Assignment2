#!/bin/sh
# Copies the shared event contract + infrastructure helpers into every service.
# Run after editing anything in shared/.
set -e
cd "$(dirname "$0")/.."
for svc in services/*/; do
  cp shared/events.bal "$svc"events.bal
  cp shared/common.bal "$svc"common.bal
  echo "synced -> $svc"
done
