-- init_clickhouse.sql: ClickHouse 데이터베이스 및 ReplicatedMergeTree 테이블 생성
CREATE DATABASE IF NOT EXISTS marketdata;

-- Keeper 연동 복제 테이블 (Keeper 1노드 + replica1)
CREATE TABLE IF NOT EXISTS marketdata.market_tick
(
    event_date    Date DEFAULT toDate(exchange_ts),
    exchange_ts   DateTime64(9),
    instrument_id UInt32,
    feed_seq      UInt64,
    price         Decimal64(4),
    quantity      UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/poc/market_tick', 'replica1')
PARTITION BY toYYYYMMDD(event_date)
ORDER BY (instrument_id, exchange_ts, feed_seq);
