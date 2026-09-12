#!/usr/bin/env bash
# run_tier2_open.sh — §2 Step 3.2 개장 초입 폭주 (Tier 2, 100,000 TPS)
set -e

echo "=== [Running Tier 2: Market Opening Surge (100,000 TPS)] ==="

if [ ! -f "./services/traffic-gen/traffic-gen" ]; then
    echo "Building traffic-gen binary..."
    (cd services/traffic-gen && go build -o traffic-gen .)
fi

./services/traffic-gen/traffic-gen \
  --topic market.tick.raw \
  --brokers localhost:19092 \
  --tps 100000 \
  --duration 60s \
  --symbols 2500 \
  --zipf-skew 1.5 \
  --burst-profile opening-bell \
  --ground-truth validation/ground_truth/traffic-gen-ground-truth.json
