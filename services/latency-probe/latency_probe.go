// latency_probe.go — NATS 구독 후 지연 히스토그램(p50, p95, p99, p99.9, max) 계산
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/nats-io/nats.go"
)

type ProbeTick struct {
	InstrumentID int64 `json:"instrument_id"`
	FeedSeq      int64 `json:"feed_seq"`
	ExchangeTsNs int64 `json:"exchange_ts_ns"`
}

var (
	natsURL      = flag.String("nats-url", nats.DefaultURL, "NATS server URL")
	subject      = flag.String("subject", "market.>", "NATS subject to subscribe to")
	duration     = flag.Duration("duration", 60*time.Second, "Monitoring duration")
	windowPeriod = flag.Duration("window", 5*time.Second, "Reporting window interval")
)

func main() {
	flag.Parse()

	log.Printf("[Latency-Probe] Connecting to NATS at %s, Subject: %s, Duration: %v", *natsURL, *subject, *duration)
	nc, err := nats.Connect(*natsURL, nats.Name("latency-probe"))
	if err != nil {
		log.Fatalf("[Latency-Probe] Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	// 1us ~ 10,000,000us (10s), 3 significant figures
	totalHist := hdrhistogram.New(1, 10_000_000, 3)
	windowHist := hdrhistogram.New(1, 10_000_000, 3)
	var histMu sync.Mutex

	var messageCount int64
	var droppedOrFutureCount int64

	sub, err := nc.Subscribe(*subject, func(msg *nats.Msg) {
		nowNs := time.Now().UnixNano()
		var t ProbeTick
		if err := json.Unmarshal(msg.Data, &t); err != nil {
			return
		}
		if t.ExchangeTsNs <= 0 {
			return
		}

		diffNs := nowNs - t.ExchangeTsNs
		if diffNs < 0 {
			atomic.AddInt64(&droppedOrFutureCount, 1)
			return
		}
		latencyUs := diffNs / 1000
		if latencyUs < 1 {
			latencyUs = 1
		} else if latencyUs > 10_000_000 {
			latencyUs = 10_000_000
		}

		atomic.AddInt64(&messageCount, 1)

		histMu.Lock()
		_ = totalHist.RecordValue(latencyUs)
		_ = windowHist.RecordValue(latencyUs)
		histMu.Unlock()
	})
	if err != nil {
		log.Fatalf("[Latency-Probe] Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	log.Println("[Latency-Probe] Subscribed successfully. Monitoring latency...")

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	windowTicker := time.NewTicker(*windowPeriod)
	defer windowTicker.Stop()

	timeoutChan := time.After(*duration)

	for {
		select {
		case <-stopChan:
			log.Println("[Latency-Probe] Interrupted by user.")
			printSummary("FINAL REPORT (Interrupted)", totalHist, atomic.LoadInt64(&messageCount))
			return
		case <-timeoutChan:
			log.Println("[Latency-Probe] Duration elapsed.")
			printSummary("FINAL BENCHMARK REPORT", totalHist, atomic.LoadInt64(&messageCount))
			return
		case <-windowTicker.C:
			histMu.Lock()
			count := windowHist.TotalCount()
			if count > 0 {
				p50 := float64(windowHist.ValueAtQuantile(50)) / 1000.0
				p90 := float64(windowHist.ValueAtQuantile(90)) / 1000.0
				p95 := float64(windowHist.ValueAtQuantile(95)) / 1000.0
				p99 := float64(windowHist.ValueAtQuantile(99)) / 1000.0
				p999 := float64(windowHist.ValueAtQuantile(99.9)) / 1000.0
				max := float64(windowHist.Max()) / 1000.0
				fmt.Printf("[Latency Window] Count: %d | p50: %.2fms | p90: %.2fms | p95: %.2fms | p99: %.2fms | p99.9: %.2fms | max: %.2fms\n",
					count, p50, p90, p95, p99, p999, max)
				windowHist.Reset()
			}
			histMu.Unlock()
		}
	}
}

func printSummary(title string, hist *hdrhistogram.Histogram, totalCount int64) {
	fmt.Println("\n=======================================================")
	fmt.Printf("           %s\n", title)
	fmt.Println("=======================================================")
	fmt.Printf("Total Messages Received : %d\n", totalCount)
	if hist.TotalCount() == 0 {
		fmt.Println("No latency samples recorded.")
		fmt.Println("=======================================================")
		return
	}

	p50 := float64(hist.ValueAtQuantile(50)) / 1000.0
	p90 := float64(hist.ValueAtQuantile(90)) / 1000.0
	p95 := float64(hist.ValueAtQuantile(95)) / 1000.0
	p99 := float64(hist.ValueAtQuantile(99)) / 1000.0
	p999 := float64(hist.ValueAtQuantile(99.9)) / 1000.0
	max := float64(hist.Max()) / 1000.0

	fmt.Printf("p50 Latency  : %8.3f ms\n", p50)
	fmt.Printf("p90 Latency  : %8.3f ms\n", p90)
	fmt.Printf("p95 Latency  : %8.3f ms\n", p95)
	fmt.Printf("p99 Latency  : %8.3f ms (Budget < 10ms: %s)\n", p99, checkBudget(p99, 10.0))
	fmt.Printf("p99.9 Latency: %8.3f ms\n", p999)
	fmt.Printf("Max Latency  : %8.3f ms\n", max)
	fmt.Println("=======================================================")
}

func checkBudget(val, budget float64) string {
	if val <= budget {
		return "PASS"
	}
	return "FAIL"
}
