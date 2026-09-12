// fault_injection.go — late-event / duplicate / out-of-order / correction 주입 로직
package main

import (
	"math/rand"
	"sync"
	"time"
)

type FaultConfig struct {
	LateEventRate   float64 // 예: 0.05 (5%)
	CorrectionRate  float64 // 예: 0.01 (1%)
	DuplicateRate   float64 // 예: 0.02 (2%)
	OutOfOrderRate  float64 // 예: 0.03 (3%)
}

type FaultInjector struct {
	cfg FaultConfig
	rnd *rand.Rand
	mu  sync.Mutex

	// Stats
	LateCount       int64
	CorrectionCount int64
	DuplicateCount  int64
	OutOfOrderCount int64
}

func NewFaultInjector(cfg FaultConfig) *FaultInjector {
	return &FaultInjector{
		cfg: cfg,
		rnd: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// CheckFaults evaluates whether a tick should undergo fault injection
// Returns: modifiedTick, isCorrection (bool), isDuplicate (bool), isOutOfOrder (bool)
func (f *FaultInjector) Apply(t Tick, lastTick *Tick) (Tick, bool, bool, bool) {
	f.mu.Lock()
	rLate := f.rnd.Float64()
	rCorr := f.rnd.Float64()
	rDup := f.rnd.Float64()
	rOoo := f.rnd.Float64()
	f.mu.Unlock()

	var isCorrection, isDuplicate, isOutOfOrder bool

	// 1. Late Event: exchange_ts 기준 200~2000ms 과거 시각 타임스탬프로 위장
	if f.cfg.LateEventRate > 0 && rLate < f.cfg.LateEventRate {
		delayMs := int64(200 + rand.Intn(1800))
		t.ExchangeTsNs -= delayMs * 1_000_000
		f.mu.Lock()
		f.LateCount++
		f.mu.Unlock()
	}

	// 2. Out of order: 이전 tick이 존재할 경우 feed_seq를 이전 값(또는 이전-1)으로 낮추고 시각도 과거로 설정
	if f.cfg.OutOfOrderRate > 0 && rOoo < f.cfg.OutOfOrderRate && lastTick != nil && lastTick.FeedSeq > 1 {
		isOutOfOrder = true
		t.FeedSeq = lastTick.FeedSeq - 1
		t.ExchangeTsNs = lastTick.ExchangeTsNs - int64(50*time.Millisecond)
		t.Price = lastTick.Price - 10000 // 구버전 가격 (1.0000 감산)
		t.BidPrice = lastTick.BidPrice - 10000
		t.AskPrice = lastTick.AskPrice - 10000
		f.mu.Lock()
		f.OutOfOrderCount++
		f.mu.Unlock()
		return t, false, false, true
	}

	// 3. Duplicate: 이전 tick과 동일한 feed_seq 재발행
	if f.cfg.DuplicateRate > 0 && rDup < f.cfg.DuplicateRate && lastTick != nil {
		isDuplicate = true
		t.FeedSeq = lastTick.FeedSeq
		t.ExchangeTsNs = lastTick.ExchangeTsNs
		f.mu.Lock()
		f.DuplicateCount++
		f.mu.Unlock()
		return t, false, true, false
	}

	// 4. Correction: 정정 호가 (별도 토픽으로 발행되거나 플래그)
	if f.cfg.CorrectionRate > 0 && rCorr < f.cfg.CorrectionRate {
		isCorrection = true
		f.mu.Lock()
		f.CorrectionCount++
		f.mu.Unlock()
	}

	return t, isCorrection, isDuplicate, isOutOfOrder
}
