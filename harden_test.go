package wssession

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	gtkitjson "github.com/gtkit/json/v2"

	"github.com/gorilla/websocket"
)

// ============================================================================
// keyedCounter 归零删除(内存安全)
// ============================================================================

func TestKeyedCounterDeletesOnZero(t *testing.T) {
	t.Parallel()
	kc := newKeyedCounter()

	if _, ok := kc.acquire("k", 5); !ok {
		t.Fatal("acquire should succeed")
	}
	kc.release("k")

	if n := kc.count("k"); n != 0 {
		t.Fatalf("count = %d, want 0", n)
	}
	// 归零后条目必须从 map 中删除,而非残留 0 值导致无界增长。
	s := &kc.shards[shardIndex("k")]
	s.mu.Lock()
	_, exists := s.counts["k"]
	s.mu.Unlock()
	if exists {
		t.Fatal("key not deleted at zero (memory leak)")
	}
}

func TestKeyedCounterRejectDoesNotCreateEntry(t *testing.T) {
	t.Parallel()
	kc := newKeyedCounter()

	// max=0:任何 acquire 都应被拒,且不产生条目。
	if _, ok := kc.acquire("k", 0); ok {
		t.Fatal("acquire with max=0 should fail")
	}
	s := &kc.shards[shardIndex("k")]
	s.mu.Lock()
	_, exists := s.counts["k"]
	s.mu.Unlock()
	if exists {
		t.Fatal("rejected acquire must not create an entry")
	}
}

// ============================================================================
// clientIP 可信来源策略
// ============================================================================

func TestClientIP(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		remoteAddr  string
		xff         string
		trustedHops int
		want        string
	}{
		{"default ignores xff", "10.0.0.1:1234", "1.2.3.4", 0, "10.0.0.1"},
		{"default no xff", "10.0.0.1:1234", "", 0, "10.0.0.1"},
		{"forged xff ignored by default", "10.0.0.1:1234", "evil, attacker", 0, "10.0.0.1"},
		{"one trusted hop single entry", "10.0.0.1:1234", "1.2.3.4", 1, "1.2.3.4"},
		{"one trusted hop picks rightmost", "10.0.0.1:1234", "client, proxyA", 1, "proxyA"},
		{"two trusted hops picks client", "10.0.0.1:1234", "client, proxyA", 2, "client"},
		{"hops exceed list falls left", "10.0.0.1:1234", "client, proxyA", 9, "client"},
		{"trusted but empty xff falls remote", "10.0.0.1:1234", "", 1, "10.0.0.1"},
		{"trims spaces", "10.0.0.1:1234", "  9.9.9.9  ", 1, "9.9.9.9"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := &http.Request{
				RemoteAddr: tt.remoteAddr,
				Header:     http.Header{},
			}
			if tt.xff != "" {
				r.Header.Set("X-Forwarded-For", tt.xff)
			}
			if got := clientIP(r, tt.trustedHops); got != tt.want {
				t.Fatalf("clientIP = %q, want %q", got, tt.want)
			}
		})
	}
}

// ============================================================================
// panic 上抛:loop recover 后返回非 nil error
// ============================================================================

func TestProcessLoopPanicReturnsError(t *testing.T) {
	t.Parallel()
	s := &Session{
		inbox:   make(chan inboundFrame, 1),
		outbox:  make(chan outboundMessage, 4),
		options: normalizeOptions(Options{}),
		handlers: Handlers{
			ParseRequest: func(context.Context, []byte) (string, any, error) {
				panic("boom in parse")
			},
			Run: func(context.Context, any, PushSink) error { return nil },
		},
	}
	s.inbox <- inboundFrame{raw: []byte("{}")}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err := s.processLoop(ctx, cancel)

	if err == nil {
		t.Fatal("processLoop returned nil, want panic error")
	}
	if !strings.Contains(err.Error(), "panic in processLoop") {
		t.Fatalf("err = %v, want panic-in-processLoop", err)
	}
}

// ============================================================================
// 错误帧下发幂等:并发/多次 closeWithError 只下发一帧
// ============================================================================

func TestCloseWithErrorIdempotent(t *testing.T) {
	t.Parallel()
	s := &Session{
		outbox:  make(chan outboundMessage, 4),
		options: Options{QueueOfferTimeout: time.Second},
	}

	frames := make(chan errorFrame, 4)
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) }) // 测试结束让消费 goroutine 退出(goleak)
	go func() {
		for {
			select {
			case msg := <-s.outbox:
				// error 帧之后会跟一帧 close 控制帧:只统计文本 error 帧,控制帧仅兑现 done
				if msg.messageType == websocket.TextMessage {
					var f errorFrame
					_ = gtkitjson.Unmarshal(msg.data, &f)
					frames <- f
				}
				if msg.done != nil {
					close(msg.done)
				}
			case <-stop:
				return
			}
		}
	}()

	s.closeWithError(t.Context(), CodeTooManyConn, "first")
	s.closeWithError(t.Context(), CodeInternal, "second")

	got := 0
	var firstReason string
loop:
	for {
		select {
		case f := <-frames:
			if got == 0 {
				firstReason = f.Reason
			}
			got++
		case <-time.After(150 * time.Millisecond):
			break loop
		}
	}

	if got != 1 {
		t.Fatalf("queued %d error frames, want 1 (idempotent)", got)
	}
	if firstReason != "first" {
		t.Fatalf("first frame reason = %q, want %q", firstReason, "first")
	}
}
