package backoff

import (
	"math"
	"math/rand"
	"time"
)

// Backoff 是指数退避参数（设计 §3.1）。
type Backoff struct {
	Base   time.Duration // 1s
	Factor float64       // 2.0
	Cap    time.Duration // 60s
}

// Next 返回第 attempt 次重试的延迟，full jitter：
// random(0, min(Cap, Base·Factor^attempt))。
func (b Backoff) Next(attempt int, rng *rand.Rand) time.Duration {
	max := float64(b.Base) * math.Pow(b.Factor, float64(attempt))
	if max > float64(b.Cap) {
		max = float64(b.Cap)
	}
	return time.Duration(rng.Float64() * max)
}
