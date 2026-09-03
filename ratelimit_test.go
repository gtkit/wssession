package wssession

import (
	"testing"
	"time"
)

func TestRateLimiterUnlimited(t *testing.T) {
	t.Parallel()
	r := newRateLimiter(0, 5)
	if r != nil {
		t.Fatal("rate <= 0 应返回 nil(不限速)")
	}
	if !r.allow() {
		t.Fatal("nil limiter 应恒放行")
	}
}

func TestRateLimiterBurstThenDeny(t *testing.T) {
	t.Parallel()
	r := newRateLimiter(1, 2) // 1/s,burst 2
	if !r.allow() {
		t.Fatal("第 1 条(burst)应放行")
	}
	if !r.allow() {
		t.Fatal("第 2 条(burst)应放行")
	}
	if r.allow() {
		t.Fatal("第 3 条立即应被拒")
	}
}

func TestRateLimiterRefill(t *testing.T) {
	t.Parallel()
	r := newRateLimiter(100, 1) // 100/s,burst 1 → ~10ms 补 1 个
	if !r.allow() {
		t.Fatal("首条应放行")
	}
	if r.allow() {
		t.Fatal("第 2 条立即应被拒")
	}
	time.Sleep(50 * time.Millisecond) // 补充后(上限 burst=1)
	if !r.allow() {
		t.Fatal("补充后应放行")
	}
}

func TestRateLimiterBurstDefault(t *testing.T) {
	t.Parallel()
	// burst < 1 时回退为 1
	r := newRateLimiter(5, 0)
	if r == nil || r.burst != 1 {
		t.Fatalf("burst<1 应回退为 1,得 %v", r)
	}
}
