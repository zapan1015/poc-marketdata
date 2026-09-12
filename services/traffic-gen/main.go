// main.go — 고성능 트래픽 생성기 (Go)
// Tier 0(1K) ~ Tier 3(300K TPS), Zipf 종목 분포, 결함 주입, Ground Truth 기록
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type Tick struct {
	InstrumentID int64 `json:"instrument_id"`
	FeedSeq      int64 `json:"feed_seq"`
	ExchangeTsNs int64 `json:"exchange_ts_ns"`
	Price        int64 `json:"price"`     // fixed-point integer (scale: 10^4, e.g. 81210.3700 -> 812103700)
	BidPrice     int64 `json:"bid_price"` // fixed-point integer (scale: 10^4)
	AskPrice     int64 `json:"ask_price"` // fixed-point integer (scale: 10^4)
	Volume       int64 `json:"volume,omitempty"`
}

type SymbolState struct {
	mu          sync.Mutex
	LastSeq     int64
	LastPrice   int64 // scale 10^4
	LastTsNs    int64
	LatestValid Tick // Ground Truth용 최신 정상 틱
	RecentTick  *Tick
}

var (
	topic           = flag.String("topic", "market.tick.raw", "Destination Kafka topic")
	correctionTopic = flag.String("correction-topic", "market.tick.correction", "Topic for correction messages")
	brokers         = flag.String("brokers", "localhost:19092", "Kafka/Redpanda broker addresses (comma-separated)")
	targetTPS       = flag.Int("tps", 10000, "Target transactions per second")
	duration        = flag.Duration("duration", 60*time.Second, "Execution duration (e.g. 60s, 300s)")
	symbolsCount    = flag.Int("symbols", 500, "Number of distinct instrument IDs")
	zipfSkew        = flag.Float64("zipf-skew", 1.2, "Zipf skew parameter (>1.0 concentrates traffic on top symbols)")
	burstProfile    = flag.String("burst-profile", "constant", "Traffic profile: 'constant' or 'opening-bell' (5s ramp-up to peak)")
	groundTruthPath = flag.String("ground-truth", "validation/ground_truth/traffic-gen-ground-truth.json", "Output path for ground truth verification JSON")

	// Fault injection flags
	injectLate        = flag.Float64("inject-late-events", 0.0, "Fraction of late events (0.0~1.0)")
	injectCorrection  = flag.Float64("inject-corrections", 0.0, "Fraction of price corrections (0.0~1.0)")
	injectDuplicates  = flag.Float64("inject-duplicates", 0.0, "Fraction of duplicate feed_seq (0.0~1.0)")
	injectOutOfOrder  = flag.Float64("inject-out-of-order", 0.0, "Fraction of out-of-order feed_seq (0.0~1.0)")
	batchSize         = flag.Int("batch-size", 1000, "Kafka producer batch size")
	producerWorkers   = flag.Int("workers", 8, "Number of concurrent batch worker goroutines")
)

func main() {
	flag.Parse()

	log.Printf("=== [Traffic-Gen] Starting ===")
	log.Printf("Brokers: %s | Topic: %s | Target TPS: %d | Duration: %v | Symbols: %d | Zipf Skew: %.2f",
		*brokers, *topic, *targetTPS, *duration, *symbolsCount, *zipfSkew)
	log.Printf("Faults: Late=%.3f, Corr=%.3f, Dup=%.3f, OutOfOrder=%.3f | Profile: %s",
		*injectLate, *injectCorrection, *injectDuplicates, *injectOutOfOrder, *burstProfile)

	// 1. Symbol ID 목록 생성 (593812를 1위 Hot Symbol로 설정)
	symbolIDs := make([]int64, *symbolsCount)
	symbolIDs[0] = 593812 // Samsung Electronics ticker code from §3.2
	for i := 1; i < *symbolsCount; i++ {
		symbolIDs[i] = int64(100000 + i)
	}

	symbolStates := make(map[int64]*SymbolState, *symbolsCount)
	for _, id := range symbolIDs {
		basePrice := (50000 + (id%500)*100) * 10000
		symbolStates[id] = &SymbolState{
			LastSeq:   0,
			LastPrice: basePrice,
		}
	}

	zipf := NewZipfGenerator(symbolIDs, *zipfSkew)
	faultInjector := NewFaultInjector(FaultConfig{
		LateEventRate:  *injectLate,
		CorrectionRate: *injectCorrection,
		DuplicateRate:  *injectDuplicates,
		OutOfOrderRate: *injectOutOfOrder,
	})

	// 2. Kafka Writer 설정
	brokerList := strings.Split(*brokers, ",")
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerList...),
		Topic:        *topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    *batchSize,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		Transport: &kafka.Transport{
			Dial: (&net.Dialer{
				Timeout: 3 * time.Second,
			}).DialContext,
		},
	}
	defer writer.Close()

	corrWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokerList...),
		Topic:        *correctionTopic,
		Balancer:     &kafka.Hash{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
	}
	defer corrWriter.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("[Traffic-Gen] Interrupted by user, stopping...")
		cancel()
	}()

	// 3. TPS Rate Limiting / Generation
	var totalProduced int64
	var totalBytes int64
	startTime := time.Now()

	// Channel for batches
	msgQueue := make(chan kafka.Message, 50000)
	corrQueue := make(chan kafka.Message, 5000)

	var wg sync.WaitGroup
	for w := 0; w < *producerWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]kafka.Message, 0, *batchSize)
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()

			flush := func() {
				if len(batch) > 0 {
					_ = writer.WriteMessages(context.Background(), batch...)
					batch = batch[:0]
				}
			}

			for {
				select {
				case msg, ok := <-msgQueue:
					if !ok {
						flush()
						return
					}
					batch = append(batch, msg)
					if len(batch) >= *batchSize {
						flush()
					}
				case <-ticker.C:
					flush()
				}
			}
		}()
	}

	// Correction worker
	go func() {
		batch := make([]kafka.Message, 0, 100)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		flushCorr := func() {
			if len(batch) > 0 {
				_ = corrWriter.WriteMessages(context.Background(), batch...)
				batch = batch[:0]
			}
		}
		for {
			select {
			case msg, ok := <-corrQueue:
				if !ok {
					flushCorr()
					return
				}
				batch = append(batch, msg)
				if len(batch) >= 100 {
					flushCorr()
				}
			case <-ticker.C:
				flushCorr()
			}
		}
	}()

	// Progress reporter
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		var lastCount int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				curr := atomic.LoadInt64(&totalProduced)
				currentTPS := curr - lastCount
				lastCount = curr
				log.Printf("[Traffic-Gen Status] Actual TPS: %d | Total: %d | Late: %d | OOO: %d | Dup: %d | Corr: %d",
					currentTPS, curr,
					faultInjector.LateCount, faultInjector.OutOfOrderCount,
					faultInjector.DuplicateCount, faultInjector.CorrectionCount)
			}
		}
	}()

	// 4. Generation Loop with Adaptive Sleep / Token Bucket
	tickerInterval := 10 * time.Millisecond
	genTicker := time.NewTicker(tickerInterval)
	defer genTicker.Stop()

	for ctx.Err() == nil {
		<-genTicker.C
		elapsed := time.Since(startTime)

		// Calculate current target TPS based on profile
		activeTPS := *targetTPS
		if *burstProfile == "opening-bell" && elapsed < 5*time.Second {
			// Linear ramp-up from 10% to 100% in 5 seconds
			fraction := 0.1 + 0.9*(float64(elapsed)/float64(5*time.Second))
			activeTPS = int(float64(*targetTPS) * fraction)
		}

		eventsInTick := int(float64(activeTPS) * tickerInterval.Seconds())
		if eventsInTick < 1 {
			eventsInTick = 1
		}

		for i := 0; i < eventsInTick; i++ {
			symID := zipf.Next()
			state := symbolStates[symID]

			state.mu.Lock()
			state.LastSeq++
			seq := state.LastSeq
			priceDelta := int64((rand.Float64() - 0.49) * 50.0 * 10000)
			newPrice := state.LastPrice + priceDelta
			if newPrice < 10000000 {
				newPrice = 10000000
			}
			state.LastPrice = newPrice
			nowNs := time.Now().UnixNano()
			if nowNs <= state.LastTsNs {
				nowNs = state.LastTsNs + 1000
			}
			state.LastTsNs = nowNs

			tick := Tick{
				InstrumentID: symID,
				FeedSeq:      seq,
				ExchangeTsNs: nowNs,
				Price:        newPrice,
				BidPrice:     newPrice - 100000, // -10.0000
				AskPrice:     newPrice + 100000, // +10.0000
				Volume:       int64(10 + rand.Intn(100)),
			}

			// Apply fault injection
			finalTick, isCorr, isDup, isOOO := faultInjector.Apply(tick, state.RecentTick)

			// Record Ground Truth only for strictly valid, monotonic, forward-moving events
			if !isCorr && !isDup && !isOOO && finalTick.ExchangeTsNs > state.LatestValid.ExchangeTsNs {
				state.LatestValid = finalTick
				tCopy := finalTick
				state.RecentTick = &tCopy
			}
			state.mu.Unlock()

			payload, _ := json.Marshal(finalTick)
			key := []byte(strconv.FormatInt(symID, 10))

			msg := kafka.Message{
				Key:   key,
				Value: payload,
			}

			if isCorr {
				corrQueue <- msg
			} else {
				msgQueue <- msg
				atomic.AddInt64(&totalProduced, 1)
				atomic.AddInt64(&totalBytes, int64(len(payload)))
			}
		}
	}

	close(msgQueue)
	close(corrQueue)
	wg.Wait()
	_ = writer.Close()
	_ = corrWriter.Close()
	time.Sleep(500 * time.Millisecond)

	totalTime := time.Since(startTime).Seconds()
	avgTPS := float64(totalProduced) / totalTime
	log.Printf("=== [Traffic-Gen] Completed ===")
	log.Printf("Total Events: %d, Elapsed: %.2fs, Avg TPS: %.1f", totalProduced, totalTime, avgTPS)

	// 5. Ground Truth JSON 저장
	saveGroundTruth(*groundTruthPath, symbolStates)
}

func saveGroundTruth(path string, states map[int64]*SymbolState) {
	groundTruth := make(map[string]interface{})
	for id, state := range states {
		state.mu.Lock()
		if state.LatestValid.InstrumentID != 0 {
			groundTruth[strconv.FormatInt(id, 10)] = map[string]interface{}{
				"instrument_id":  state.LatestValid.InstrumentID,
				"price":          state.LatestValid.Price,
				"bid_price":      state.LatestValid.BidPrice,
				"ask_price":      state.LatestValid.AskPrice,
				"feed_seq":       state.LatestValid.FeedSeq,
				"exchange_ts_ns": state.LatestValid.ExchangeTsNs,
			}
		}
		state.mu.Unlock()
	}

	_ = os.MkdirAll(filepath.Dir(path), 0755)
	file, err := os.Create(path)
	if err != nil {
		log.Printf("[Ground Truth] Error creating file %s: %v", path, err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(groundTruth); err != nil {
		log.Printf("[Ground Truth] Error encoding JSON: %v", err)
	} else {
		log.Printf("[Ground Truth] Saved expected latest states to %s (%d symbols)", path, len(groundTruth))
	}
}
