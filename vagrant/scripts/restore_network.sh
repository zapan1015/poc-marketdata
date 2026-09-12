#!/usr/bin/env bash
# vagrant/scripts/restore_network.sh — A.5.3 네트워크 복구: 링크 복구 및 iptables 플러시
set -e

echo "=== [Multi-DC Restoration] Restoring network link between DC-A and DC-B ==="
vagrant ssh dc-a -c "sudo iptables -F"
echo "Cross-DC link restored on dc-a."
