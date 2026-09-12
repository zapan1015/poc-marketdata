-- check_gap_duplicate.sql — §3.2 ClickHouse에서 feed_seq 연속성 검사
-- gap > 1: 누락(Gap) 발생
-- gap <= 0: 중복(Duplicate) 또는 역전(Out-of-Order) 발생

SELECT
    instrument_id,
    feed_seq,
    feed_seq - lagInFrame(feed_seq) OVER (PARTITION BY instrument_id ORDER BY feed_seq) AS gap,
    exchange_ts,
    price
FROM marketdata.market_tick
WHERE instrument_id = 593812
HAVING gap != 1 AND gap IS NOT NULL
ORDER BY feed_seq;
