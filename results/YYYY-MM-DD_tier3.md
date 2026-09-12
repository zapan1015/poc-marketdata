# PoC Tier 3 (300,000 TPS 피크) 벤치마크 결과 기록

- 일시: 
- 담당자:
- 실행 스크립트: `scripts/run_tier3_peak.sh`

## 환경 요약
- 호스트: Windows 11 + WSL2 (Ubuntu)
- CPU / RAM: 
- 트래픽 프로파일: opening-bell

## 측정 지표

| 지표 | 목표 기준 | 실측치 | 달성 여부 |
|---|---|---|---|
| 달성 TPS | 300,000 | | |
| NATS p50 지연 | < 5.0 ms | | |
| NATS p95 지연 | < 12.0 ms | | |
| NATS p99 지연 | < 20.0 ms | | |
| Max 지연 | - | | |
| Kafka Consumer Lag | 수렴 여부 | | |
| 드롭된 이벤트 수 | 0 | | |

## 비고 및 특이사항
- WSL2 환경에서 생성기 프로세스 CPU 포화 여부 확인 필수
