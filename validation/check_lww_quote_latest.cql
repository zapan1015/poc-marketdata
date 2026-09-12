-- check_lww_quote_latest.cql — §3.2 ScyllaDB LWW 정상 동작 확인 쿼리
-- out-of-order 주입 후에도 quote_latest가 exchange_ts 기준 가장 최신 값을 유지하는지 조회
SELECT instrument_id, price, bid_price, ask_price, feed_seq, event_ts
FROM marketdata.quote_latest
WHERE instrument_id = 593812;
