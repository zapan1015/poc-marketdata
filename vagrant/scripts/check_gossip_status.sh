#!/usr/bin/env bash
# vagrant/scripts/check_gossip_status.sh — A.5.2 nodetool status로 장애 감지 확인
set -e

echo "=== [DC-A ScyllaDB Nodetool Status] ==="
vagrant ssh dc-a -c "docker exec scylladb-a nodetool status" || true

echo ""
echo "=== [DC-B ScyllaDB Nodetool Status] ==="
vagrant ssh dc-b -c "docker exec scylladb-b nodetool status" || true
