# PoC Tier 2 (100,000 TPS) 벤치마크 결과 기록

- 일시: 
- 담당자:
- 실행 스크립트: `scripts/run_tier2_open.sh`

## 환경 요약
- 호스트: Windows 11 + WSL2 (Ubuntu)
- CPU / RAM: 
- 트래픽 프로파일: opening-bell (첫 5초 선형 램프업)

## 측정 지표

| 지표 | 목표 기준 | 실측치 | 달성 여부 |
|---|---|---|---|
| 달성 TPS | 100,000 | | |
| NATS p50 지연 | < 3.0 ms | | |
| NATS p95 지연 | < 8.0 ms | | |
| NATS p99 지연 | < 10.0 ms | | |
| Max 지연 | - | | |
| Scylla Queue 사용률 | < 80% | | |

## 비고 및 특이사항
- 
