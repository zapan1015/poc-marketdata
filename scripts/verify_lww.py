#!/usr/bin/env python3
"""
verify_lww.py — §3.2 ScyllaDB LWW (Last-Write-Wins) 정합성 검증 스크립트
과거 시각의 Out-of-order 이벤트가 최신 정상 시세 값을 덮어쓰지 않았는지 검증합니다.
"""

import sys
import os
import json
import subprocess

def main():
    ground_truth_path = sys.argv[1] if len(sys.argv) > 1 else "validation/ground_truth/traffic-gen-ground-truth.json"
    target_symbol = sys.argv[2] if len(sys.argv) > 2 else "593812"

    if not os.path.exists(ground_truth_path):
        print(f"[Error] Ground truth file not found: {ground_truth_path}")
        sys.exit(1)

    with open(ground_truth_path, "r", encoding="utf-8") as f:
        ground_truth = json.load(f)

    if target_symbol not in ground_truth:
        print(f"[Error] Target symbol {target_symbol} not found in ground truth file.")
        print(f"Available symbols: {list(ground_truth.keys())[:10]}...")
        sys.exit(1)

    expected = ground_truth[target_symbol]
    expected_price = float(expected["price"])
    expected_seq = int(expected["feed_seq"])

    print(f"=== [LWW Verification for Symbol {target_symbol}] ===")
    print(f"Expected Ground Truth -> Price: {expected_price}, feed_seq: {expected_seq}")

    # Query ScyllaDB via docker exec cqlsh
    cql_cmd = f"SELECT instrument_id, price, feed_seq, event_ts FROM marketdata.quote_latest WHERE instrument_id={target_symbol};"
    result = subprocess.run(
        ["docker", "exec", "scylladb", "cqlsh", "-e", cql_cmd],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"[Error] Failed to execute CQL in scylladb: {result.stderr}")
        sys.exit(1)

    output = result.stdout
    print("\n[ScyllaDB Current State Output]:")
    print(output)

    # Check price match (handling formatting)
    price_str = f"{expected_price:.2f}"
    price_int_str = f"{int(expected_price)}"

    matched = price_str in output or price_int_str in output or str(expected_price) in output
    if matched:
        print(">> [PASS] LWW 검증 성공: out-of-order 지연 이벤트가 최신 가격을 덮어쓰지 않고 최신 상태가 보존됨.")
    else:
        print(f">> [FAIL] LWW 검증 실패: 기대값 {expected_price}이(가) ScyllaDB 결과에 존재하지 않습니다.")
        sys.exit(2)

if __name__ == "__main__":
    main()
