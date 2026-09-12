# Multi-DC 장애 시나리오 결과 — YYYY-MM-DD

## 환경
- VirtualBox 버전 / Vagrant 버전:
- 하이퍼바이저 모드: WHP 호환 / Hyper-V 완전 비활성화

## 장애 감지
| 항목 | 측정값 | 판정 |
|---|---|---|
| 파티션 발생→nodetool DN 감지 시간 | | |
| DC-A 로컬 LOCAL_QUORUM 가용성 유지 여부 | | Pass/Fail |
| NATS 로컬 팬아웃 지속 여부 | | Pass/Fail |

## 복구
| 항목 | 측정값 | 판정 |
|---|---|---|
| 복구→Hinted Handoff 완료 시간 | | |
| Reconciliation 최종 일치 여부 | | Pass/Fail |
| 수동 repair 필요 여부 | | |

## 판정 및 본운영 반영 사항
(§13 Multi-DC HA 섹션 대비 실제 동작 차이, 필요 시 개정 항목)
