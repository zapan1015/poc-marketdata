# PoC 벤치마크 결과 — YYYY-MM-DD

## 환경
- Windows 11 버전:
- WSL2 커널 버전:
- Docker Desktop 버전:
- 호스트 스펙(CPU/RAM/Disk):
- .wslconfig 설정: memory=___ processors=___

## 성능 결과

| 시나리오 | TPS(목표) | TPS(실측) | p50(ms) | p95(ms) | p99(ms) | CPU(%) | Mem(GB) | Disk I/O(MB/s) |
|---|---|---|---|---|---|---|---|---|
| 정상(Tier1) | 10,000 | | | | | | | |
| 개장초입(Tier2) | 100,000 | | | | | | | |
| 피크(Tier3) | 300,000 | | | | | | | |
| 네트워크 지연 주입(20ms) | | | | | | | | |

## 정합성 결과

| 항목 | 주입 비율 | 탐지 건수 | 탐지율 | 판정 |
|---|---|---|---|---|
| LWW 정확성 | out-of-order 3% | | | Pass/Fail |
| 중복 탐지 | 2% | | | Pass/Fail |
| 누락(Gap) 탐지 | - | | | Pass/Fail |
| 정정(Correction) 처리 | 1% | | | Pass/Fail |

## 복구 결과

| 항목 | 측정값 |
|---|---|
| Consumer 다운→재기동 후 Lag 0 도달 시간 | |
| ScyllaDB pause 중 NATS p99 변화율 | |
| ClickHouse pause 중 NATS p99 변화율 | |
