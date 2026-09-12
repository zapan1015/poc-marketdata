// zipf.go — Hot Symbol 시나리오 재현을 위한 Zipf 분포 생성기
// 상위 20개 종목에 트래픽 60% 이상 집중
package main

import (
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type ZipfGenerator struct {
	n       int
	cdf     []float64
	rnd     *rand.Rand
	mu      sync.Mutex
	symbols []int64
}

// NewZipfGenerator initializes a Zipfian distribution generator
// n: total symbols, skew: Zipf parameter s (e.g. 1.1~1.3 concentrates ~60% in top 20)
func NewZipfGenerator(symbolIDs []int64, skew float64) *ZipfGenerator {
	n := len(symbolIDs)
	cdf := make([]float64, n)

	var sum float64
	for i := 1; i <= n; i++ {
		sum += 1.0 / math.Pow(float64(i), skew)
	}

	var cumulative float64
	for i := 1; i <= n; i++ {
		cumulative += (1.0 / math.Pow(float64(i), skew)) / sum
		cdf[i-1] = cumulative
	}
	cdf[n-1] = 1.0 // clamp max

	return &ZipfGenerator{
		n:       n,
		cdf:     cdf,
		rnd:     rand.New(rand.NewSource(time.Now().UnixNano())),
		symbols: symbolIDs,
	}
}

// Next returns a symbol ID chosen according to the Zipf distribution
func (z *ZipfGenerator) Next() int64 {
	z.mu.Lock()
	r := z.rnd.Float64()
	z.mu.Unlock()

	idx := sort.Search(z.n, func(i int) bool {
		return z.cdf[i] >= r
	})
	if idx >= z.n {
		idx = z.n - 1
	}
	return z.symbols[idx]
}
