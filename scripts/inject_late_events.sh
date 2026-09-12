#!/usr/bin/env bash
# inject_late_events.sh — §2 Step 3.4 Late Event / 정정 / 중복 / 순서역전 테스트
set -e

echo "=== [Injecting Late Events, Duplicates, and Out-of-Order Ticks] ==="

if [ ! -f "./services/traffic-gen/traffic-gen" ]; then
    echo "Building traffic-gen binary..."
    (cd services/traffic-gen && go build -o traffic-gen .)
fi

./services/traffic-gen/traffic-gen \
  --topic market.tick.raw \
  --brokers localhost:19092 \
  --tps 5000 \
  --duration 120s \
  --symbols 100 \
  --inject-late-events 0.05 \
  --inject-corrections 0.01 \
  --inject-duplicates 0.02 \
  --inject-out-of-order 0.03 \
  --ground-truth validation/ground_truth/traffic-gen-ground-truth.json
