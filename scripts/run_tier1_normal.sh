#!/usr/bin/env bash
# run_tier1_normal.sh — §2 Step 3.1 정상 상태 (Tier 1, 10,000 TPS)
set -e

echo "=== [Running Tier 1: Normal Trading Hours (10,000 TPS)] ==="

# Build traffic-gen if binary does not exist
if [ ! -f "./services/traffic-gen/traffic-gen" ]; then
    echo "Building traffic-gen binary..."
    (cd services/traffic-gen && go build -o traffic-gen .)
fi

./services/traffic-gen/traffic-gen \
  --topic market.tick.raw \
  --brokers localhost:19092 \
  --tps 10000 \
  --duration 300s \
  --symbols 500 \
  --zipf-skew 1.0 \
  --ground-truth validation/ground_truth/traffic-gen-ground-truth.json
