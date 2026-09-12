# PoC Tier 1 (10,000 TPS) 벤치마크 결과 기록

- 일시: 
- 담당자:
- 실행 스크립트: `scripts/run_tier1_normal.sh`

## 환경 요약
- 호스트: Windows 11 + WSL2 (Ubuntu)
- CPU / RAM: 
- 컨테이너 상태: Redpanda, ScyllaDB, ClickHouse, NATS

## 측정 지표

| 지표 | 목표 기준 | 실측치 | 달성 여부 |
|---|---|---|---|
| 달성 TPS | 10,000 | | |
| NATS p50 지연 | < 2.0 ms | | |
| NATS p95 지연 | < 5.0 ms | | |
| NATS p99 지연 | < 10.0 ms | | |
| Max 지연 | - | | |
| CPU 사용률 | < 50% | | |
| 메모리 사용량 | < 10 GB | | |

## 비고 및 특이사항
- 
