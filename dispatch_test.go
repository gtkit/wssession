package wssession

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// recordPeak 把 in-flight 峰值记进 peak(CAS 循环,避免读改写竞态)。
func recordPeak(peak *atomic.Int32, n int32) {
	for {
		cur := peak.Load()
		if n <= cur || peak.CompareAndSwap(cur, n) {
			return
		}
	}
}

// drainNoEvent 断言事件通道里没有出现指定类型的事件。
func drainNoEvent(t *testing.T, events <-chan Event, unwanted EventType) {
	t.Helper()
	for {
		select {
		case ev := <-events:
			if ev.Type == unwanted {
				t.Fatalf("不应上报事件 %v", unwanted)
			}
		default:
			return
		}
	}
}

// TestDispatchSequentialDoesNotInterrupt:顺序模式下新消息不打断正在运行的回调。
func TestDispatchSequentialDoesNotInterrupt(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 16)
	started := make(chan struct{}, 1)
	proceed := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(proceed) }) }
	t.Cleanup(release)

	h := duplexHandlers(func(ctx context.Context, raw []byte, sink PushSink) error {
		if string(raw) == "first" {
			started <- struct{}{}
			<-proceed
			return sink.Push(ctx, map[string]any{"id": "first", "canceled": ctx.Err() != nil})
		}
		return sink.Push(ctx, map[string]any{"id": string(raw), "canceled": ctx.Err() != nil})
	})
	opts := Options{
		Dispatch: DispatchSequential,
		OnEvent:  func(_ context.Context, ev Event) { events <- ev },
	}
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	subscribe(t, conn)
	_ = conn.WriteMessage(websocket.TextMessage, []byte("first"))
	<-started

	// 第二条消息在第一轮运行中到达:留出时间窗口,让"错误地打断"有机会暴露。
	_ = conn.WriteMessage(websocket.TextMessage, []byte("second"))
	time.Sleep(100 * time.Millisecond)
	release()

	first := readJSONFrame(t, conn, 2*time.Second)
	if first["id"] != "first" {
		t.Fatalf("第一帧 = %v, want id=first", first)
	}
	if first["canceled"] != false {
		t.Fatal("顺序模式下第一轮不应被新消息打断")
	}
	second := readJSONFrame(t, conn, 2*time.Second)
	if second["id"] != "second" {
		t.Fatalf("第二帧 = %v, want id=second", second)
	}
	drainNoEvent(t, events, EventTurnInterrupted)
}

// TestDispatchSequentialProcessesInOrder:顺序模式逐条串行处理且保持顺序。
func TestDispatchSequentialProcessesInOrder(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	var inFlight, peak atomic.Int32

	h := duplexHandlers(func(ctx context.Context, raw []byte, sink PushSink) error {
		recordPeak(&peak, inFlight.Add(1))
		defer inFlight.Add(-1)
		time.Sleep(20 * time.Millisecond)
		return sink.Push(ctx, map[string]any{"id": string(raw)})
	})
	srv := newTestSession(t, path, Options{Dispatch: DispatchSequential}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	subscribe(t, conn)
	want := []string{"m1", "m2", "m3"}
	for _, id := range want {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(id))
	}

	for _, id := range want {
		if got := readJSONFrame(t, conn, 3*time.Second); got["id"] != id {
			t.Fatalf("收到 %v, want id=%s(顺序被打乱)", got, id)
		}
	}
	if got := peak.Load(); got != 1 {
		t.Fatalf("同时在飞回调峰值 = %d, want 1", got)
	}
}

// TestDispatchConcurrentRunsInParallel:并发模式下多轮同时在飞。
//
// 若实现退化为串行,第一个回调会等到 3s 超时后返回 error,连接被收敛,读取失败。
func TestDispatchConcurrentRunsInParallel(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	const total = 3
	var arrived atomic.Int32
	allIn := make(chan struct{})
	var once sync.Once

	h := duplexHandlers(func(ctx context.Context, raw []byte, sink PushSink) error {
		if arrived.Add(1) == total {
			once.Do(func() { close(allIn) })
		}
		select {
		case <-allIn:
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
			return errors.New("并发调度未让多轮同时在飞")
		}
		return sink.Push(ctx, map[string]any{"id": string(raw)})
	})
	opts := Options{Dispatch: DispatchConcurrent, MaxConcurrentMessages: 4}
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	subscribe(t, conn)
	for _, id := range []string{"m1", "m2", "m3"} {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(id))
	}

	seen := make(map[string]bool, total)
	for range total {
		got := readJSONFrame(t, conn, 5*time.Second)
		id, _ := got["id"].(string)
		if id == "" {
			t.Fatalf("收到非预期帧 %v", got)
		}
		seen[id] = true
	}
	if len(seen) != total {
		t.Fatalf("收到 %d 条不同回帧, want %d", len(seen), total)
	}
}

// TestDispatchConcurrentRespectsLimit:并发上限为 1 时同时在飞不超过 1。
func TestDispatchConcurrentRespectsLimit(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	var inFlight, peak atomic.Int32

	h := duplexHandlers(func(ctx context.Context, raw []byte, sink PushSink) error {
		recordPeak(&peak, inFlight.Add(1))
		defer inFlight.Add(-1)
		time.Sleep(30 * time.Millisecond)
		return sink.Push(ctx, map[string]any{"id": string(raw)})
	})
	opts := Options{Dispatch: DispatchConcurrent, MaxConcurrentMessages: 1}
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	subscribe(t, conn)
	_ = conn.WriteMessage(websocket.TextMessage, []byte("m1"))
	_ = conn.WriteMessage(websocket.TextMessage, []byte("m2"))

	for range 2 {
		if got := readJSONFrame(t, conn, 3*time.Second); got["id"] == nil {
			t.Fatalf("收到非预期帧 %v", got)
		}
	}
	if got := peak.Load(); got != 1 {
		t.Fatalf("同时在飞回调峰值 = %d, want 1(受 MaxConcurrentMessages 限制)", got)
	}
}

// TestDispatchConcurrentCancelsInflightOnClose:连接收敛时取消所有在飞轮次。
func TestDispatchConcurrentCancelsInflightOnClose(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	const total = 2
	entered := make(chan struct{}, total)
	canceled := make(chan struct{}, total)

	h := duplexHandlers(func(ctx context.Context, _ []byte, _ PushSink) error {
		entered <- struct{}{}
		<-ctx.Done()
		canceled <- struct{}{}
		return ctx.Err()
	})
	opts := Options{Dispatch: DispatchConcurrent, MaxConcurrentMessages: 4}
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	subscribe(t, conn)
	_ = conn.WriteMessage(websocket.TextMessage, []byte("m1"))
	_ = conn.WriteMessage(websocket.TextMessage, []byte("m2"))
	for range total {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("3s 内回调未全部进入")
		}
	}

	_ = conn.Close() // 客户端断开 → 连接收敛
	for range total {
		select {
		case <-canceled:
		case <-time.After(3 * time.Second):
			t.Fatal("3s 内在飞回调未被取消")
		}
	}
}

// TestDispatchConcurrentStuckTurnEmitsEventAndCloses:并发模式下失约的回调
// (不监听 ctx)在收敛时超时,上报 EventTurnStuck 且连接不无限等待。
func TestDispatchConcurrentStuckTurnEmitsEventAndCloses(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 16)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	releaseTurn := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseTurn)

	h := duplexHandlers(func(_ context.Context, _ []byte, _ PushSink) error {
		started <- struct{}{}
		<-release // 失约:完全不监听 ctx
		return nil
	})
	opts := Options{
		Dispatch:              DispatchConcurrent,
		MaxConcurrentMessages: 2,
		TurnCloseTimeout:      50 * time.Millisecond,
		OnEvent:               func(_ context.Context, ev Event) { events <- ev },
	}
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	subscribe(t, conn)
	_ = conn.WriteMessage(websocket.TextMessage, []byte("stuck"))
	<-started

	// 客户端断开 → 连接收敛 → 取消在飞轮次 → 失约轮次超时
	_ = conn.Close()
	waitForEvent(t, events, EventTurnStuck)
	releaseTurn()
}

// TestDispatchSequentialRateLimited:限速语义在顺序模式下同样生效。
func TestDispatchSequentialRateLimited(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 32)
	opts := Options{
		Dispatch:             DispatchSequential,
		InboundRatePerSecond: 1,
		InboundRateBurst:     1,
		OnEvent:              func(_ context.Context, ev Event) { events <- ev },
	}
	h := duplexHandlers(func(ctx context.Context, raw []byte, sink PushSink) error {
		return sink.Push(ctx, map[string]any{"id": string(raw)})
	})
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	subscribe(t, conn)
	for _, id := range []string{"m1", "m2", "m3"} {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(id))
	}

	waitForEvent(t, events, EventRateLimited)
	if msg := readErrorFrame(t, conn); msg["code"] != float64(CodeTooManyConn) {
		t.Fatalf("限速提示帧 code = %v, want %d", msg["code"], CodeTooManyConn)
	}
}

// TestOptionsValidateDispatch:调度配置按 fail-closed 校验。
func TestOptionsValidateDispatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		opts    Options
		wantErr string
	}{
		{
			name: "零值即打断模式",
			opts: Options{},
		},
		{
			name: "顺序模式无需并发上限",
			opts: Options{Dispatch: DispatchSequential, MaxConcurrentMessages: 8},
		},
		{
			name:    "并发模式缺上限被拒",
			opts:    Options{Dispatch: DispatchConcurrent},
			wantErr: "MaxConcurrentMessages",
		},
		{
			name:    "并发模式上限为负被拒",
			opts:    Options{Dispatch: DispatchConcurrent, MaxConcurrentMessages: -1},
			wantErr: "MaxConcurrentMessages",
		},
		{
			name: "并发模式给了上限通过",
			opts: Options{Dispatch: DispatchConcurrent, MaxConcurrentMessages: 2},
		},
		{
			name:    "未定义模式被拒",
			opts:    Options{Dispatch: DispatchConcurrent + 1},
			wantErr: "unknown Dispatch mode",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.opts.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate() = %v, want 含 %q", err, tc.wantErr)
			}
		})
	}
}

// TestInterruptStuckTurnReportedOnce:打断式调度下失约轮次只上报一次
// EventTurnStuck,且 Serve 不被等两个 TurnCloseTimeout。
func TestInterruptStuckTurnReportedOnce(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	const turnTimeout = 200 * time.Millisecond
	events := make(chan Event, 16)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	releaseTurn := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseTurn)

	h := duplexHandlers(func(_ context.Context, raw []byte, _ PushSink) error {
		if string(raw) == "stuck" {
			started <- struct{}{}
			<-release // 失约:不监听 ctx
		}
		return nil
	})
	served := make(chan time.Duration, 1)
	opts := Options{
		TurnCloseTimeout: turnTimeout,
		OnEvent:          func(_ context.Context, ev Event) { events <- ev },
	}

	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		_ = Serve(r.Context(), w, r, opts, h)
		select {
		case served <- time.Since(start):
		default:
		}
	})
	srv := httptest.NewTestServer(t, mux) // 自动 Cleanup;handler panic 直接判 fail
	srv.Start()                           // gorilla 客户端走真实 TCP,需要 loopback 监听而非默认内存网络

	conn, _ := dial(t, wsURL(srv.URL, path))
	subscribe(t, conn)
	_ = conn.WriteMessage(websocket.TextMessage, []byte("stuck"))
	<-started
	_ = conn.WriteMessage(websocket.TextMessage, []byte("next")) // 触发打断

	waitForEvent(t, events, EventTurnStuck)
	select {
	case elapsed := <-served:
		if elapsed > 2*turnTimeout {
			t.Fatalf("Serve 耗时 %v,超过一个 TurnCloseTimeout(%v)的合理范围——失约轮次被等了两次",
				elapsed, turnTimeout)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("3s 内 Serve 未返回")
	}

	// 同一次失约只应上报一次
	extra := 0
	deadline := time.After(2 * turnTimeout)
	for {
		select {
		case ev := <-events:
			if ev.Type == EventTurnStuck {
				extra++
			}
		case <-deadline:
			if extra > 0 {
				t.Fatalf("EventTurnStuck 重复上报 %d 次", extra+1)
			}
			releaseTurn()
			return
		}
	}
}
