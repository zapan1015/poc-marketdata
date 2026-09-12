// materializer.go — Kafka(Redpanda) 소비 → Scylla LWW 쓰기 + NATS 발행
// §3.3 격리성 원칙: ScyllaDB 지연/장애가 NATS 팬아웃 경로를 블로킹하지 않도록
// Scylla 쓰기는 비동기 버퍼 큐(Worker Pool)로 격리하고 NATS는 즉시 발행합니다.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/nats-io/nats.go"
	"github.com/segmentio/kafka-go"
)

type Tick struct {
	InstrumentID int64   `json:"instrument_id"`
	FeedSeq      int64   `json:"feed_seq"`
	ExchangeTsNs int64   `json:"exchange_ts_ns"`
	Price        float64 `json:"price"`
	BidPrice     float64 `json:"bid_price"`
	AskPrice     float64 `json:"ask_price"`
	Volume       int64   `json:"volume,omitempty"`
}

var (
	kafkaBrokers = flag.String("kafka-brokers", "localhost:19092", "Kafka/Redpanda broker addresses (comma-separated)")
	topic        = flag.String("topic", "market.tick.raw", "Input Kafka topic")
	groupID      = flag.String("group-id", "materializer-poc", "Kafka consumer group ID")
	scyllaHosts  = flag.String("scylla-hosts", "127.0.0.1", "ScyllaDB cluster hosts (comma-separated)")
	scyllaPort   = flag.Int("scylla-port", 9042, "ScyllaDB CQL port")
	natsURL      = flag.String("nats-url", nats.DefaultURL, "NATS server URL")
	asyncScylla  = flag.Bool("async-scylla", true, "Isolate Scylla writes using async worker pool (§3.3 isolation test)")
	workers      = flag.Int("workers", 16, "Number of concurrent workers for Scylla writes")
	queueSize    = flag.Int("queue-size", 50000, "Queue size for Scylla async write buffer")
)

func main() {
	flag.Parse()
	log.Printf("[Materializer] Starting... Kafka: %s, Topic: %s, Scylla: %s:%d, NATS: %s (Async Scylla: %v)",
		*kafkaBrokers, *topic, *scyllaHosts, *scyllaPort, *natsURL, *asyncScylla)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("[Materializer] Shutdown signal received, terminating...")
		cancel()
	}()

	// 1. Kafka / Redpanda Reader
	brokers := strings.Split(*kafkaBrokers, ",")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          *topic,
		GroupID:        *groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        10 * time.Millisecond,
		CommitInterval: 100 * time.Millisecond,
	})
	defer reader.Close()

	// 2. ScyllaDB Session
	cluster := gocql.NewCluster(strings.Split(*scyllaHosts, ",")...)
	cluster.Port = *scyllaPort
	cluster.Keyspace = "marketdata"
	cluster.Consistency = gocql.One
	cluster.Timeout = 2 * time.Second
	cluster.ConnectTimeout = 5 * time.Second
	cluster.NumConns = 8
	cluster.ReconnectInterval = 1 * time.Second
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}

	var session *gocql.Session
	var err error
	for i := 0; i < 5; i++ {
		session, err = cluster.CreateSession()
		if err == nil {
			break
		}
		log.Printf("[Materializer] Waiting for ScyllaDB... (%v)", err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("[Materializer] Failed to connect to ScyllaDB: %v", err)
	}
	defer session.Close()
	log.Println("[Materializer] ScyllaDB connected successfully.")

	// 3. NATS Core Connection
	nc, err := nats.Connect(*natsURL, nats.Name("materializer-poc"))
	if err != nil {
		log.Fatalf("[Materializer] Failed to connect to NATS: %v", err)
	}
	defer nc.Close()
	log.Println("[Materializer] NATS connected successfully.")

	// 4. Scylla Async Worker Pool (§3.3 격리성: ScyllaDB write 지연이 NATS publish를 블로킹하지 않음)
	scyllaQueue := make(chan Tick, *queueSize)
	var wg sync.WaitGroup

	if *asyncScylla {
		for i := 0; i < *workers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				stmt := `UPDATE quote_latest USING TIMESTAMP ? SET price=?, bid_price=?, ask_price=?, feed_seq=?, event_ts=? WHERE instrument_id=?`
				for {
					select {
					case <-ctx.Done():
						return
					case t, ok := <-scyllaQueue:
						if !ok {
							return
						}
						// exchange_ts_ns를 마이크로초(us) USING TIMESTAMP 로 지정해 스토리지 레벨 LWW 달성
						usTimestamp := t.ExchangeTsNs / 1000
						err := session.Query(stmt, usTimestamp, t.Price, t.BidPrice, t.AskPrice, t.FeedSeq, time.Now(), t.InstrumentID).Exec()
						if err != nil && workerID == 0 {
							log.Printf("[Materializer] Scylla write error sample: %v", err)
						}
					}
				}
			}(i)
		}
	}

	// 5. Main Consumer Loop
	var processedCount int64
	lastReport := time.Now()

	for {
		if ctx.Err() != nil {
			break
		}
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Println("[Materializer] Kafka read error:", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		var t Tick
		if err := json.Unmarshal(m.Value, &t); err != nil {
			continue
		}

		// NATS 실시간 팬아웃 발행 (초저지연 경로: 즉시 수행)
		// Subject: market.<instrument_id>
		subject := fmt.Sprintf("market.%d", t.InstrumentID)
		if err := nc.Publish(subject, m.Value); err != nil {
			log.Printf("[Materializer] NATS publish error on %s: %v", subject, err)
		}

		// ScyllaDB LWW 쓰기 경로
		if *asyncScylla {
			select {
			case scyllaQueue <- t:
			default:
				// 큐가 가득 찬 경우 (예: Scylla가 pause 상태) NATS 경로 지연 방지를 위해 드롭/경고
			}
		} else {
			// 동기식 쓰기 (격리성 비교 대조군)
			usTimestamp := t.ExchangeTsNs / 1000
			err = session.Query(
				`UPDATE quote_latest USING TIMESTAMP ? SET price=?, bid_price=?, ask_price=?, feed_seq=?, event_ts=? WHERE instrument_id=?`,
				usTimestamp, t.Price, t.BidPrice, t.AskPrice, t.FeedSeq, time.Now(), t.InstrumentID,
			).Exec()
			if err != nil {
				// log.Printf("[Materializer] Scylla sync write error: %v", err)
			}
		}

		processedCount++
		if time.Since(lastReport) >= 5*time.Second {
			log.Printf("[Materializer] Processed %d messages (Queue size: %d/%d)",
				processedCount, len(scyllaQueue), *queueSize)
			lastReport = time.Now()
		}
	}

	close(scyllaQueue)
	wg.Wait()
	log.Printf("[Materializer] Terminated. Total processed messages: %d", processedCount)
	_ = strconv.Itoa(0) // suppress unused
}
