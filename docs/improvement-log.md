# 아키텍처 개선 피드백 이력 (Improvement Log)

본 문서는 PoC 수행 결과(`results/*.md`)를 바탕으로 본운영 아키텍처 명세서 및 파이프라인 설계를 개선한 이력을 관리합니다.

---

## 1. 아키텍처 변경 판단 기준 가이드 (§4.3)

1. **p99 지연이 예산(10ms)을 초과하고 특정 구간에 집중된 경우**:
   - NATS 팬아웃 구간 초과 시: Gateway 구독 Aggregation 및 커널 소켓 버퍼 튜닝 검토.
   - Kafka 컨슘 구간 지연 시: 파티션 수 확장 및 배치 Fetch 크기 조정.
2. **LWW 정합성 검증 실패 시**:
   - `USING TIMESTAMP`의 시간 단위(마이크로초) 정확성 검증.
   - PTP/NTP 시계 동기화 오차 점검 및 시퀀스 번호 결합 타임스탬프 도입 검토.
3. **격리성 검증 실패 시 (Scylla/ClickHouse 지연이 NATS로 전파)**:
   - Materializer 내부에서 DB write와 NATS publish가 동기 체인에 있는지 확인.
   - 비동기 워커 큐 및 백프레셔(Backpressure) 드롭 정책 점검.
4. **300K TPS 피크에서 Consumer Lag 지속 증가 시**:
   - Kafka 파티션 수 및 Consumer Group 스레드 병렬성 확대.
   - Hot Symbol 전용 토픽 및 처리 파이프라인 격리 검토.
5. **Replay 시간이 과도한 경우**:
   - Checkpoint 간격 축소 및 Replay 전용 Consumer Pool 분리 검토.

---

## 2. 개정 이력

### [v0.1.0] - PoC 초기 환경 구축
- **근거 결과**: 초기 환경 구성 단계
- **변경 사항**:
  - ScyllaDB LWW 테이블 스키마 및 마이크로초 단위 `USING TIMESTAMP` 쿼리 정의.
  - Materializer에 NATS 실시간 경로와 Scylla 비동기 워커 풀 격리 구조 기본 적용.
  - Redpanda, ClickHouse+Keeper, NATS Core 단일 Compose 환경 구성.

### [v0.2.0] - Tier 1 벤치마크 및 격리성/LWW 검증 완료 (2026-09-12)
- **근거 결과**: `results/2026-09-12_tier1.md`, `results/poc_final_evaluation_report.md`
- **개선 사항**:
  - **Kafka Reader 지연 튜닝**: `MinBytes: 10KB`로 인한 버퍼링 대기를 `MinBytes: 1`, `MaxWait: 10ms`로 튜닝하여 E2E p50 지연시간 55% 단축(30.8ms → 13.6ms).
  - **ScyllaDB gocql 타입 및 연결성 개선**: `quote_latest` 테이블의 가격 컬럼을 `double`로 정합하고 `ReconnectInterval: 1s`, `RetryPolicy`를 적용하여 노드 pause 복구 후 100% 자동 재연결 달성.
  - **장애 격리성 입증**: 10,000 TPS 부하 상태에서 ScyllaDB 12초 일시정지 중 NATS 실시간 팬아웃 p99 지연시간 영향 0.8% 이하로 완벽 격리 확인.
  - **스토리지 레벨 LWW 100% 정합성 검증**: 20%의 Late/Out-of-order 이벤트 주입 상태에서 `USING TIMESTAMP`가 과거 이벤트를 스토리지 레벨에서 무시하여 Ground Truth와 100% 일치함 확인.
  - **ClickHouse 내장 Keeper 전환**: 외장 Keeper SSL/호스트 설정 이슈를 제거하고 ClickHouse 24.3 내장 `keeper_server`로 일원화.
