#!/usr/bin/env bash
# pause_scylla_isolation_test.sh — §3.3 ScyllaDB 지연 시 NATS 실시간 경로 격리성 검증
set -e

PAUSE_SEC="${1:-15}"

echo "=== [ScyllaDB Isolation Test] Pausing ScyllaDB for ${PAUSE_SEC}s ==="
echo "Observe latency-probe.go: NATS fanout p99 latency should remain within 20% degradation."
docker pause scylladb

echo "ScyllaDB is paused. Sleeping for ${PAUSE_SEC} seconds..."
sleep "$PAUSE_SEC"

echo "=== Unpausing ScyllaDB ==="
docker unpause scylladb
echo "ScyllaDB unpaused successfully."
