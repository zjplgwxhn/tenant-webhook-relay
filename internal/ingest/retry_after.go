package ingest

import (
	"math"
	"time"
)

// RetryAfterCalc 计算 429 响应的 Retry-After。
type RetryAfterCalc interface {
	Compute(queueDepth int64, drainRate float64) time.Duration
}

// ClampingRetryAfter 把 ceil(depth/drainRate) 钳制到 [Min, Max]。
// drainRate 下限 1 event/s，避免除零与过度乐观的 Retry-After（设计 §2.2）。
type ClampingRetryAfter struct {
	Min time.Duration
	Max time.Duration
}

func (c ClampingRetryAfter) Compute(queueDepth int64, drainRate float64) time.Duration {
	if drainRate < 1.0 {
		drainRate = 1.0
	}
	secs := int64(math.Ceil(float64(queueDepth) / drainRate))
	d := time.Duration(secs) * time.Second
	if d < c.Min {
		return c.Min
	}
	if d > c.Max {
		return c.Max
	}
	return d
}
