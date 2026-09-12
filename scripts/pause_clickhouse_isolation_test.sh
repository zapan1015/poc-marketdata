#!/usr/bin/env bash
# pause_clickhouse_isolation_test.sh — §3.3 ClickHouse 지연 시 NATS 실시간 경로 격리성 검증
set -e

PAUSE_SEC="${1:-15}"

echo "=== [ClickHouse Isolation Test] Pausing ClickHouse for ${PAUSE_SEC}s ==="
docker pause clickhouse

echo "ClickHouse is paused. Sleeping for ${PAUSE_SEC} seconds..."
sleep "$PAUSE_SEC"

echo "=== Unpausing ClickHouse ==="
docker unpause clickhouse
echo "ClickHouse unpaused successfully."
