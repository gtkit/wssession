package wssession

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// ============================================================================
// OnEvent 事件上报
// ============================================================================

func TestEventCapRejectedIP(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 8)
	opts := Options{
		ConnCapEnabled: true,
		ConnCapIPMax:   1,
		ConnCapKeyMax:  10,
		OnEvent:        func(_ context.Context, ev Event) { events <- ev },
	}
	handlers := passthroughHandlers(func(ctx context.Context, _ PushSink) error {
		<-ctx.Done()
		return nil
	})
	srv := newTestSession(t, path, opts, handlers)

	conn1, _ := dial(t, wsURL(srv.URL, path))
	_ = conn1.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"x"}`))
	_ = readJSONFrame(t, conn1, 2*time.Second)

	// 第 2 条触发 IP cap 拒绝 → EventCapRejected
	_ = dialExpectingFailure(t, wsURL(srv.URL, path))

	select {
	case ev := <-events:
		if ev.Type != EventCapRejected {
			t.Fatalf("event type = %v, want EventCapRejected", ev.Type)
		}
		if !strings.HasPrefix(ev.Key, "ip:") {
			t.Fatalf("event key = %q, want ip: prefix", ev.Key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no EventCapRejected within 2s")
	}
}

func TestEventSlowConsumer(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 8)
	opts := Options{OnEvent: func(_ context.Context, ev Event) { events <- ev }}
	// Run 直接返回 ErrSlowConsumer,触发 EventSlowConsumer 分支
	handlers := passthroughHandlers(func(context.Context, PushSink) error {
		return ErrSlowConsumer
	})
	srv := newTestSession(t, path, opts, handlers)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"x"}`))

	for {
		select {
		case ev := <-events:
			if ev.Type == EventSlowConsumer {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("no EventSlowConsumer within 2s")
		}
	}
}

func TestEventPanic(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 8)
	opts := Options{OnEvent: func(_ context.Context, ev Event) { events <- ev }}
	handlers := passthroughHandlers(func(context.Context, PushSink) error {
		panic("boom in run")
	})
	srv := newTestSession(t, path, opts, handlers)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"x"}`))

	for {
		select {
		case ev := <-events:
			if ev.Type == EventPanic {
				if ev.Err == nil {
					t.Fatal("EventPanic.Err is nil")
				}
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("no EventPanic within 2s")
		}
	}
}

func TestEmitNilSafe(t *testing.T) {
	t.Parallel()
	// OnEvent 为 nil 时 emit 不应 panic
	var o Options
	o.emit(t.Context(), Event{Type: EventPanic, Reason: "x"})
}

func TestEmitCallbackPanicContained(t *testing.T) {
	t.Parallel()
	// 用户回调 panic 必须被 emit 内部 recover,不传播
	o := Options{OnEvent: func(context.Context, Event) { panic("user callback panic") }}
	o.emit(t.Context(), Event{Type: EventCapRejected})
	// 跑到这里即说明 panic 已被吞,未传播
}

func TestEventTypeString(t *testing.T) {
	t.Parallel()
	cases := map[EventType]string{
		EventPanic:         "panic",
		EventSlowConsumer:  "slow_consumer",
		EventCapRejected:   "cap_rejected",
		EventAbnormalClose: "abnormal_close",
		EventType(0):       "unknown",
	}
	for et, want := range cases {
		if got := et.String(); got != want {
			t.Errorf("EventType(%d).String() = %q, want %q", et, got, want)
		}
	}
}

// ============================================================================
// ConnCapSnapshot
// ============================================================================

func TestConnCapSnapshot(t *testing.T) {
	t.Parallel()
	key := "test:" + strings.ReplaceAll(t.Name(), "/", "_")

	_, _ = tryAcquire(key, 5)
	_, _ = tryAcquire(key, 5)

	snap := ConnCapSnapshot()
	if snap[key] != 2 {
		t.Fatalf("snapshot[%s] = %d, want 2", key, snap[key])
	}

	// 独立副本:改返回的 map 不影响内部
	snap[key] = 999
	if again := ConnCapSnapshot(); again[key] != 2 {
		t.Fatalf("after mutating returned map, internal = %d, want 2", again[key])
	}

	release(key)
	release(key)

	// 归零后不应出现在快照中
	if _, ok := ConnCapSnapshot()[key]; ok {
		t.Fatalf("zeroed key %s still present in snapshot", key)
	}
}

// ============================================================================
// 1006 异常断开归类
// ============================================================================

func TestIsExpectedCloseExcludes1006(t *testing.T) {
	t.Parallel()
	err := &websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "abnormal"}
	if IsExpectedClose(err) {
		t.Fatal("IsExpectedClose(1006) = true, want false (1006 should not be silently expected)")
	}
	if !isAbnormalClose(err) {
		t.Fatal("isAbnormalClose(1006) = false, want true")
	}
}

func TestIsExpectedCloseStillCoversNormal(t *testing.T) {
	t.Parallel()
	normal := &websocket.CloseError{Code: websocket.CloseNormalClosure}
	if !IsExpectedClose(normal) {
		t.Fatal("IsExpectedClose(normal closure) = false, want true")
	}
	going := &websocket.CloseError{Code: websocket.CloseGoingAway}
	if !IsExpectedClose(going) {
		t.Fatal("IsExpectedClose(going away) = false, want true")
	}
}

// ============================================================================
// sameOrigin 默认端口归一化
// ============================================================================

func TestSameOriginDefaultPort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		origin string
		host   string
		want   bool
	}{
		{"https origin no port vs host 443", "https://example.com", "example.com:443", true},
		{"https origin 443 vs host no port", "https://example.com:443", "example.com", true},
		{"http origin no port vs host 80", "http://example.com", "example.com:80", true},
		{"both no port", "https://example.com", "example.com", true},
		{"non-default port mismatch", "https://example.com:8443", "example.com:443", false},
		{"different host", "https://a.com", "b.com:443", false},
		{"origin with path", "https://example.com/ws", "example.com:443", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := sameOrigin(tt.origin, tt.host); got != tt.want {
				t.Fatalf("sameOrigin(%q,%q) = %v, want %v", tt.origin, tt.host, got, tt.want)
			}
		})
	}
}

// ============================================================================
// Benchmark:keyedCounter 热路径
// ============================================================================

func BenchmarkKeyedCounterAcquireRelease(b *testing.B) {
	kc := newKeyedCounter()
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := kc.acquire("ip:1.2.3.4:/ws", 1024); ok {
			kc.release("ip:1.2.3.4:/ws")
		}
	}
}

func BenchmarkKeyedCounterParallel(b *testing.B) {
	kc := newKeyedCounter()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, ok := kc.acquire("ip:1.2.3.4:/ws", 1<<30); ok {
				kc.release("ip:1.2.3.4:/ws")
			}
		}
	})
}
