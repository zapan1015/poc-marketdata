#!/usr/bin/env bash
# inject_network_delay.sh — §2 Step 3.3 네트워크 지연 모의 (tc netem)
# Usage: ./scripts/inject_network_delay.sh [add|del] [delay] [jitter]
set -e

ACTION="${1:-add}"
DELAY="${2:-20ms}"
JITTER="${3:-5ms}"

if [ "$ACTION" == "add" ]; then
    echo "=== Injecting network delay ($DELAY +/- $JITTER) to Redpanda container ==="
    docker run --rm --net=container:redpanda --cap-add=NET_ADMIN nicolaka/netshoot \
      tc qdisc add dev eth0 root netem delay "$DELAY" "$JITTER" distribution normal
    echo "Network delay injected successfully."
elif [ "$ACTION" == "del" ] || [ "$ACTION" == "reset" ]; then
    echo "=== Removing network delay from Redpanda container ==="
    docker run --rm --net=container:redpanda --cap-add=NET_ADMIN nicolaka/netshoot \
      tc qdisc del dev eth0 root || true
    echo "Network delay removed."
else
    echo "Usage: $0 [add|del] [delay] [jitter]"
    exit 1
fi
