# Risk & Bottleneck Analysis Checklist (WSL2 → Bare Metal 전환 시 재검증 필수 항목)

본 체크리스트는 WSL2 가상화 환경에서 수행된 PoC 결과를 프로덕션 Bare Metal 환경으로 이관할 때 반드시 재검증해야 하는 제약 및 병목 항목입니다.

- [ ] **CPU pinning/NUMA**: WSL2에서는 측정 불가. Bare Metal에서 Feed Handler·Materializer의 코어 고정(core pinning) 후 p99 지연시간 재측정 필요.
- [ ] **Disk I/O**: WSL2 ext4.vhdx는 NVMe 직접 접근 대비 오버헤드가 존재함. ClickHouse Merge 성능 및 ScyllaDB CommitLog I/O는 Bare Metal NVMe에서 별도 벤치마크 필요.
- [ ] **네트워크 가상화 오버헤드**: WSL2 NAT는 실제 프로덕션 네트워크(전용 스위치, 10G/25G NIC, 커널 바이패스/Solarflare)와 지연 특성이 다르므로 본 PoC의 절대 latency 수치를 그대로 최종 SLO 근거로 확정하지 않는다.
- [ ] **트래픽 생성기 자체 병목**: 생성기 프로세스가 300K TPS 부하에서 CPU 상한(100%)에 도달했는지 확인 (도달 시 실측 TPS는 시스템 한계가 아니라 생성기 프로세스 병목임).
- [ ] **단일 노드 구성의 한계**: 본 PoC는 단일 노드(또는 최소 Replica)로 진행했으므로, Multi-DC 및 RF=3 클러스터 환경에서의 네트워크 RTT 및 쿼럼 복제 오버헤드는 별도 검증 필요.
- [ ] **Docker Desktop 리소스 경쟁**: 호스트 OS에서 동시에 실행 중인 타 프로세스로 인한 CPU 스케줄링 간섭 배제 (측정 시 PoC 전용으로 호스트 격리 권장).
- [ ] **vhdx 파일 비대화로 인한 Disk I/O 저하**: 반복 테스트로 가상 디스크가 팽창한 경우 `scripts/compact_vhdx.ps1`을 통해 정기 압축 수행 후 재측정.
