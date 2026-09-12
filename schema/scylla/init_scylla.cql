-- init_scylla.cql: ScyllaDB 키스페이스 및 LWW/최근체결 테이블 생성
CREATE KEYSPACE IF NOT EXISTS marketdata
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE marketdata;

-- 최신 시세 캐시 테이블 (LWW: USING TIMESTAMP 로 최종 쓰기 보장)
CREATE TABLE IF NOT EXISTS quote_latest (
    instrument_id bigint,
    price         double,
    bid_price     double,
    ask_price     double,
    event_ts      timestamp,
    feed_seq      bigint,
    PRIMARY KEY ((instrument_id))
);

-- 최근 틱 스트림 버킷 테이블 (클러스터링 키 기반 정렬)
CREATE TABLE IF NOT EXISTS tick_recent (
    instrument_id bigint,
    bucket        text,
    event_ts      timestamp,
    feed_seq      bigint,
    price         decimal,
    quantity      bigint,
    PRIMARY KEY ((instrument_id, bucket), event_ts, feed_seq)
) WITH CLUSTERING ORDER BY (event_ts DESC, feed_seq DESC);
