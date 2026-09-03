package wssession

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	gtkitjson "github.com/gtkit/json/v2"

	"github.com/gorilla/websocket"
)

// ============================================================================
// Run 返回 nil:flush 尾帧 + close(1000) 主动收敛,连接不悬挂
// ============================================================================

func TestRunNilSendsNormalClosureAndConverges(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	handlers := passthroughHandlers(func(ctx context.Context, sink PushSink) error {
		_ = sink.Push(ctx, map[string]any{"data": "final"})
		return nil
	})
	srv := newTestSession(t, path, Options{}, handlers)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"x"}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	// 尾帧先于 close 帧到达,不丢
	if got := readJSONFrame(t, conn, 2*time.Second); got["data"] != "final" {
		t.Fatalf("got = %v, want data=final", got)
	}

	// 随后应在远小于 MaxSessionDuration 的时间内收到 close(1000)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := conn.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
		t.Fatalf("err = %v, want close 1000", err)
	}
}

// ============================================================================
// 会话超时(服务端单方面终止)→ close(1001 GoingAway)
// ============================================================================

func TestSessionTimeoutSendsGoingAway(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	handlers := passthroughHandlers(func(ctx context.Context, sink PushSink) error {
		<-ctx.Done()
		return nil
	})
	opts := Options{MaxSessionDuration: 300 * time.Millisecond}
	srv := newTestSession(t, path, opts, handlers)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"x"}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := conn.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseGoingAway) {
		t.Fatalf("err = %v, want close 1001 (going away)", err)
	}
}

// ============================================================================
// tokenCap 占用覆盖整条连接生命周期
// ============================================================================

func TestTokenCapHeldForConnectionLifetime(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	opts := Options{ConnCapEnabled: true, ConnCapIPMax: 100, ConnCapKeyMax: 1}
	handlers := passthroughHandlers(func(ctx context.Context, sink PushSink) error {
		<-ctx.Done()
		return nil
	})
	srv := newTestSession(t, path, opts, handlers)

	conn1, _ := dial(t, wsURL(srv.URL, path))
	_ = conn1.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"cap-tok"}`))
	if f := readJSONFrame(t, conn1, 2*time.Second); f["event"] != "subscribed" {
		t.Fatalf("first conn frame = %v, want subscribed", f)
	}

	// 连接存活期间,同 token 第二条连接必须被 tokenCap 拒绝
	conn2, _ := dial(t, wsURL(srv.URL, path))
	_ = conn2.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"cap-tok"}`))
	f2 := readJSONFrame(t, conn2, 2*time.Second)
	if f2["event"] != "error" {
		t.Fatalf("second conn frame = %v, want error", f2)
	}
	if code, _ := f2["code"].(float64); int(code) != CodeTooManyConn {
		t.Fatalf("code = %v, want %d", f2["code"], CodeTooManyConn)
	}

	// 第一条连接关闭后 cap 释放(计数归零即条目删除)
	_ = conn1.Close()
	key := "token:cap-tok:" + path
	deadline := time.Now().Add(3 * time.Second)
	for connCounters.count(key) != 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := connCounters.count(key); n != 0 {
		t.Fatalf("counter %s = %d after conn closed, want 0", key, n)
	}
}

// ============================================================================
// 错误关闭完成 WS close 握手:error 帧后跟 1008 / 1011 close 帧
// ============================================================================

func TestErrorCloseHandshakePolicyViolation(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	srv := newTestSession(t, path, Options{}, passthroughHandlers(nil))

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"bogus"}`))

	f := readJSONFrame(t, conn, 2*time.Second)
	if f["event"] != "error" {
		t.Fatalf("frame = %v, want error", f)
	}
	if code, _ := f["code"].(float64); int(code) != CodeInvalidParam {
		t.Fatalf("code = %v, want %d", f["code"], CodeInvalidParam)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := conn.ReadMessage()
	if !websocket.IsCloseError(err, websocket.ClosePolicyViolation) {
		t.Fatalf("err = %v, want close 1008", err)
	}
}

func TestErrorCloseHandshakeInternalError(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	handlers := passthroughHandlers(func(context.Context, PushSink) error {
		return errors.New("business boom")
	})
	srv := newTestSession(t, path, Options{}, handlers)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"x"}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	f := readJSONFrame(t, conn, 2*time.Second)
	if code, _ := f["code"].(float64); int(code) != CodeInternal {
		t.Fatalf("code = %v, want %d", f["code"], CodeInternal)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := conn.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseInternalServerErr) {
		t.Fatalf("err = %v, want close 1011", err)
	}
}

// ============================================================================
// Kick 踢下线:error(409, reason) + close 1008,幂等
// ============================================================================

func TestSessionKickSends409AndPolicyViolation(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	sessCh := make(chan *Session, 1)
	h := Handlers{
		OnConnect: func(_ context.Context, s *Session) error {
			sessCh <- s
			return nil
		},
		ParseRequest: func(_ context.Context, _ []byte) (string, any, error) { return "tok", nil, nil },
		Run: func(ctx context.Context, _ any, _ PushSink) error {
			<-ctx.Done()
			return nil
		},
	}
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	sess := <-sessCh
	sess.Kick(t.Context(), "logged in elsewhere")
	sess.Kick(t.Context(), "second call must be ignored") // 幂等:只下发首帧

	f := readJSONFrame(t, conn, 2*time.Second)
	if f["event"] != "error" {
		t.Fatalf("frame = %v, want error", f)
	}
	if code, _ := f["code"].(float64); int(code) != CodeConflict {
		t.Fatalf("code = %v, want %d", f["code"], CodeConflict)
	}
	if f["reason"] != "logged in elsewhere" {
		t.Fatalf("reason = %v, want first kick reason", f["reason"])
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := conn.ReadMessage()
	if !websocket.IsCloseError(err, websocket.ClosePolicyViolation) {
		t.Fatalf("err = %v, want close 1008", err)
	}
}

// ============================================================================
// 双向模式后台 Run 错误处置与单向一致
// ============================================================================

func TestDuplexBackgroundRunErrorSendsErrorFrame(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := Handlers{
		ParseRequest: func(_ context.Context, _ []byte) (string, any, error) { return "tok", nil, nil },
		Run: func(context.Context, any, PushSink) error {
			return errors.New("bg boom")
		},
		OnMessage: func(ctx context.Context, _ []byte, sink PushSink) error {
			return sink.Push(ctx, map[string]any{"echo": 1})
		},
	}
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	f := readJSONFrame(t, conn, 2*time.Second)
	if f["event"] != "error" {
		t.Fatalf("frame = %v, want error", f)
	}
	if code, _ := f["code"].(float64); int(code) != CodeInternal {
		t.Fatalf("code = %v, want %d", f["code"], CodeInternal)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := conn.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseInternalServerErr) {
		t.Fatalf("err = %v, want close 1011", err)
	}
}

func TestDuplexBackgroundRunSlowConsumerEmitsEvent(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 16)
	h := Handlers{
		ParseRequest: func(_ context.Context, _ []byte) (string, any, error) { return "tok", nil, nil },
		Run: func(context.Context, any, PushSink) error {
			return ErrSlowConsumer
		},
		OnMessage: func(context.Context, []byte, PushSink) error { return nil },
	}
	opts := Options{OnEvent: func(_ context.Context, ev Event) { events <- ev }}
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	waitForEvent(t, events, EventSlowConsumer)

	f := readJSONFrame(t, conn, 2*time.Second)
	if code, _ := f["code"].(float64); int(code) != CodeTooManyConn {
		t.Fatalf("code = %v, want %d", f["code"], CodeTooManyConn)
	}
}

// ============================================================================
// turn 打断串行化:新轮仅在旧轮 goroutine 退出后启动
// ============================================================================

func TestDuplexInterruptWaitsForOldTurn(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	var running atomic.Int32
	var overlap atomic.Bool
	started := make(chan struct{}, 1)
	h := duplexHandlers(func(ctx context.Context, raw []byte, sink PushSink) error {
		if running.Add(1) > 1 {
			overlap.Store(true)
		}
		defer running.Add(-1)

		var m map[string]any
		_ = gtkitjson.Unmarshal(raw, &m)
		if m["slow"] == true {
			started <- struct{}{}
			<-ctx.Done()
			// 模拟收尾耗时:旧轮退出前,新轮不得进入 OnMessage
			time.Sleep(150 * time.Millisecond)
			return ctx.Err()
		}
		return sink.Push(ctx, map[string]any{"echo": m["text"]})
	})
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"slow":true}`))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("慢轮未启动")
	}

	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"text":"z"}`))
	if got := readJSONFrame(t, conn, 2*time.Second); got["echo"] != "z" {
		t.Fatalf("echo = %v, want z", got["echo"])
	}
	if overlap.Load() {
		t.Fatal("新旧 turn 并发运行:旧轮未退出时新轮已进入 OnMessage")
	}
}

// ============================================================================
// 限速提示帧 episode 化:连续限速期只下发一帧 429,事件仍逐条上报
// ============================================================================

func TestDuplexRateLimitNotifyOncePerEpisode(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	rateLimitedEvents := make(chan Event, 32)
	h := duplexHandlers(func(ctx context.Context, _ []byte, sink PushSink) error {
		return sink.Push(ctx, map[string]any{"ok": 1})
	})
	opts := Options{
		InboundRatePerSecond: 1,
		InboundRateBurst:     1,
		OnEvent: func(_ context.Context, ev Event) {
			if ev.Type == EventRateLimited {
				rateLimitedEvents <- ev
			}
		},
	}
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	// 快速连发 5 条:第 1 条过(burst=1),其余 4 条同一限速期内被丢
	for range 5 {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"q":1}`))
	}

	// 收集 800ms 窗口内所有帧:期望 1 帧 ok + 恰好 1 帧 error(429)
	errFrames := 0
	_ = conn.SetReadDeadline(time.Now().Add(800 * time.Millisecond))
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			break // 读超时,窗口结束
		}
		var f map[string]any
		_ = gtkitjson.Unmarshal(raw, &f)
		if f["event"] == "error" {
			errFrames++
		}
	}
	if errFrames != 1 {
		t.Fatalf("收到 %d 帧 error(429),want 1(同一限速期去重)", errFrames)
	}

	// 事件仍逐条上报:4 条被丢消息 ≥2 次事件(留时序余量)
	eventCount := len(rateLimitedEvents)
	if eventCount < 2 {
		t.Fatalf("EventRateLimited 事件数 = %d, want >= 2(逐条上报)", eventCount)
	}
}
