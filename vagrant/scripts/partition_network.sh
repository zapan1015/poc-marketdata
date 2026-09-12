#!/usr/bin/env bash
# vagrant/scripts/partition_network.sh — A.5.2 네트워크 파티션: DC 간 링크 차단
set -e

echo "=== [Multi-DC Partition] Blocking traffic between DC-A and DC-B ==="
vagrant ssh dc-a -c "sudo iptables -A INPUT -s 192.168.56.12 -j DROP && sudo iptables -A OUTPUT -d 192.168.56.12 -j DROP"
echo "Cross-DC link (192.168.56.12) dropped on dc-a."
