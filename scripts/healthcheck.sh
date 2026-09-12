#!/usr/bin/env bash
# healthcheck.sh — WSL2 내부(ext4 경로)에서 실행
set -e

echo "=== [Step 1: Container Readiness Check] ==="
docker compose up -d

echo "Waiting for containers to initialize..."
sleep 10

echo -n "[Redpanda] Admin API: "
curl -sf http://localhost:9644/v1/status/ready && echo " OK" || echo " FAIL"

echo -n "[ScyllaDB] CQL Readiness: "
docker exec scylladb cqlsh -e "SELECT release_version FROM system.local;" > /dev/null 2>&1 && echo " OK" || echo " FAIL"

echo -n "[ClickHouse] HTTP Ping: "
curl -sf "http://localhost:8123/?query=SELECT%201" > /dev/null 2>&1 && echo " OK" || echo " FAIL"

echo -n "[NATS Core] HTTP Monitor: "
curl -sf http://localhost:8222/varz > /dev/null && echo " OK" || echo " FAIL"

echo ""
echo "=== Current Docker Container Resource Usage ==="
docker stats --no-stream
