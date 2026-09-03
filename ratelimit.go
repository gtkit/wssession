package wssession

import "time"

// rateLimiter 是单连接入站消息的令牌桶限速器(标准库实现,零外部依赖)。
//
// 并发约定:仅在 processLoop 单 goroutine 内调用,故无需加锁。
// nil *rateLimiter 表示不限速,allow 恒为 true(见 newRateLimiter)。
type rateLimiter struct {
	ratePerSec float64
	burst      float64
	tokens     float64
	last       time.Time
}

// newRateLimiter 构造限速器;ratePerSec <= 0 返回 nil(不限速)。
func newRateLimiter(ratePerSec float64, burst int) *rateLimiter {
	if ratePerSec <= 0 {
		return nil
	}
	if burst < 1 {
		burst = 1
	}
	return &rateLimiter{
		ratePerSec: ratePerSec,
		burst:      float64(burst),
		tokens:     float64(burst),
		last:       time.Now(),
	}
}

// allow 按当前时间补充令牌并尝试取一个;放行返回 true,超速返回 false。
// nil receiver(未配置限速)恒放行。
func (r *rateLimiter) allow() bool {
	if r == nil {
		return true
	}
	now := time.Now()
	r.tokens = min(r.tokens+now.Sub(r.last).Seconds()*r.ratePerSec, r.burst)
	r.last = now
	if r.tokens >= 1 {
		r.tokens--
		return true
	}
	return false
}
