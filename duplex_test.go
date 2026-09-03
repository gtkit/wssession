package wssession

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	gtkitjson "github.com/gtkit/json/v2"
)

// duplexHandlers 构造双向模式 Handlers:首帧当订阅(任意内容),其后每条走 onMsg。
func duplexHandlers(onMsg func(ctx context.Context, raw []byte, sink PushSink) error) Handlers {
	return Handlers{
		ParseRequest: func(_ context.Context, _ []byte) (string, any, error) {
			return "tok", nil, nil
		},
		OnMessage: onMsg,
	}
}

// waitForEvent 等待指定类型事件,超时失败。
func waitForEvent(t *testing.T, events <-chan Event, want EventType) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-events:
			if ev.Type == want {
				return
			}
		case <-deadline:
			t.Fatalf("3s 内未收到事件 %v", want)
		}
	}
}

func TestDuplexMultiTurn(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := duplexHandlers(func(ctx context.Context, raw []byte, sink PushSink) error {
		var m map[string]any
		_ = gtkitjson.Unmarshal(raw, &m)
		return sink.Push(ctx, map[string]any{"echo": m["text"]})
	})
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	if sub := readJSONFrame(t, conn, 2*time.Second); sub["event"] != "subscribed" {
		t.Fatalf("first frame = %v, want subscribed", sub["event"])
	}

	// 同一连接多轮:每条触发一次 OnMessage,连接保持
	for _, want := range []string{"a", "b", "c"} {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"text":"`+want+`"}`))
		got := readJSONFrame(t, conn, 2*time.Second)
		if got["echo"] != want {
			t.Fatalf("echo = %v, want %s", got["echo"], want)
		}
	}
}

func TestDuplexInterrupt(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 16)
	started := make(chan struct{}, 4)
	h := duplexHandlers(func(ctx context.Context, raw []byte, sink PushSink) error {
		var m map[string]any
		_ = gtkitjson.Unmarshal(raw, &m)
		if m["slow"] == true {
			started <- struct{}{}
			<-ctx.Done() // 阻塞直到被打断
			return ctx.Err()
		}
		return sink.Push(ctx, map[string]any{"echo": m["text"]})
	})
	opts := Options{OnEvent: func(_ context.Context, ev Event) { events <- ev }}
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	// 慢消息:开启一轮并阻塞
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"slow":true}`))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("慢轮未启动")
	}

	// 新消息打断上一轮
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"text":"x"}`))
	waitForEvent(t, events, EventTurnInterrupted)

	if got := readJSONFrame(t, conn, 2*time.Second); got["echo"] != "x" {
		t.Fatalf("echo = %v, want x", got["echo"])
	}

	select {
	case ev := <-events:
		if ev.Type == EventTurnStuck {
			t.Fatal("守约 turn 不应上报 EventTurnStuck")
		}
	default:
	}
}

// TestDuplexInterruptLeakyErrorDoesNotClose:被打断的旧 turn 即使返回了普通业务 error
// (没如约传播 ctx 取消),也不得误关整条连接——新一轮应正常工作。
func TestDuplexInterruptLeakyErrorDoesNotClose(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	started := make(chan struct{}, 1)
	h := Handlers{
		ParseRequest: func(_ context.Context, _ []byte) (string, any, error) { return "tok", nil, nil },
		OnMessage: func(ctx context.Context, raw []byte, sink PushSink) error {
			var m map[string]any
			_ = gtkitjson.Unmarshal(raw, &m)
			if m["slow"] == true {
				started <- struct{}{}
				<-ctx.Done()
				return errors.New("leaky: did not propagate ctx cancellation") // 不老实
			}
			return sink.Push(ctx, map[string]any{"echo": m["text"]})
		},
	}
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"slow":true}`))
	<-started

	// 打断慢轮:慢轮会返回一个普通业务 error,但 turnCtx 已被取消 → 应静默,不关连接
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"text":"y"}`))

	got := readJSONFrame(t, conn, 2*time.Second)
	if got["event"] == "error" {
		t.Fatalf("连接被旧轮的泄漏错误误关:收到 %v", got)
	}
	if got["echo"] != "y" {
		t.Fatalf("echo = %v, want y", got["echo"])
	}
}

func TestDuplexInterruptStuckTurnEmitsEventAndCloses(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 8)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseTurn := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseTurn)

	h := duplexHandlers(func(ctx context.Context, raw []byte, sink PushSink) error {
		var m map[string]any
		_ = gtkitjson.Unmarshal(raw, &m)
		if m["stuck"] == true {
			started <- struct{}{}
			<-ctx.Done()
			<-release
			return ctx.Err()
		}
		return sink.Push(ctx, map[string]any{"echo": m["text"]})
	})
	opts := Options{
		TurnCloseTimeout: 50 * time.Millisecond,
		OnEvent:          func(_ context.Context, ev Event) { events <- ev },
	}
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"stuck":true}`))
	<-started

	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"text":"next"}`))
	waitForEvent(t, events, EventTurnStuck)

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var msg map[string]any
		_ = gtkitjson.Unmarshal(raw, &msg)
		if msg["event"] == "error" {
			return
		}
	}
}

func TestDuplexTurnCancelledOnClose(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	var cancelled atomic.Bool
	started := make(chan struct{}, 1)
	h := duplexHandlers(func(ctx context.Context, _ []byte, _ PushSink) error {
		started <- struct{}{}
		<-ctx.Done() // 阻塞直到连接收敛
		cancelled.Store(true)
		return ctx.Err()
	})
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"q":1}`))
	<-started

	_ = conn.Close() // 客户端断开 → 会话 ctx 取消 → turn ctx 取消

	deadline := time.After(3 * time.Second)
	for !cancelled.Load() {
		select {
		case <-deadline:
			t.Fatal("客户端 close 后 turn ctx 未被取消(可能泄漏)")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestDuplexCloseStuckTurnDoesNotWaitForever(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 16)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseTurn := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseTurn)
	h := duplexHandlers(func(context.Context, []byte, PushSink) error {
		started <- struct{}{}
		<-release // 忽略 ctx,直到测试释放
		return nil
	})
	opts := Options{
		TurnCloseTimeout: 50 * time.Millisecond,
		OnEvent:          func(_ context.Context, ev Event) { events <- ev },
	}
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"q":1}`))
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("stuck turn 未启动")
	}

	_ = conn.Close()
	waitForEvent(t, events, EventTurnStuck)
	releaseTurn()
}

func TestDuplexOnMessageError(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := duplexHandlers(func(context.Context, []byte, PushSink) error {
		return errors.New("business failure")
	})
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"q":1}`))

	msg := readJSONFrame(t, conn, 2*time.Second)
	if msg["event"] != "error" {
		t.Fatalf("event = %v, want error", msg["event"])
	}
	if code, _ := msg["code"].(float64); int(code) != CodeInternal {
		t.Fatalf("code = %v, want %d", msg["code"], CodeInternal)
	}
}

// TestDuplexWithBackgroundRun:双向模式下 Run 可选,作为后台主动推送与 OnMessage 并存。
func TestDuplexWithBackgroundRun(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := Handlers{
		ParseRequest: func(_ context.Context, _ []byte) (string, any, error) { return "tok", nil, nil },
		Run: func(ctx context.Context, _ any, sink PushSink) error {
			_ = sink.Push(ctx, map[string]any{"push": "bg"}) // 后台主动推一帧
			<-ctx.Done()
			return nil
		},
		OnMessage: func(ctx context.Context, _ []byte, sink PushSink) error {
			return sink.Push(ctx, map[string]any{"echo": "msg"})
		},
	}
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))

	// 订阅确认后,后台 Run 应推出 push:bg(与 subscribed 帧顺序不定,循环找)
	sawBg := false
	for range 4 {
		f := readJSONFrame(t, conn, 2*time.Second)
		if f["push"] == "bg" {
			sawBg = true
			break
		}
	}
	if !sawBg {
		t.Fatal("未收到后台 Run 推送的 push:bg")
	}
}

func TestDuplexOnMessageSlowConsumer(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := duplexHandlers(func(context.Context, []byte, PushSink) error {
		return ErrSlowConsumer
	})
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"q":1}`))

	msg := readJSONFrame(t, conn, 2*time.Second)
	if code, _ := msg["code"].(float64); int(code) != CodeTooManyConn {
		t.Fatalf("code = %v, want %d", msg["code"], CodeTooManyConn)
	}
}

func TestDuplexOnMessagePanic(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 16)
	h := duplexHandlers(func(context.Context, []byte, PushSink) error {
		panic("boom in OnMessage")
	})
	opts := Options{OnEvent: func(_ context.Context, ev Event) { events <- ev }}
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"q":1}`))

	waitForEvent(t, events, EventPanic) // OnMessage panic 被 recover 并上报
}

func TestDuplexRateLimit(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 32)
	h := duplexHandlers(func(ctx context.Context, _ []byte, sink PushSink) error {
		return sink.Push(ctx, map[string]any{"ok": 1})
	})
	opts := Options{
		InboundRatePerSecond: 1,
		InboundRateBurst:     1,
		OnEvent:              func(_ context.Context, ev Event) { events <- ev },
	}
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	// 快速连发,超出 1/s burst 1
	for range 5 {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"q":1}`))
	}
	waitForEvent(t, events, EventRateLimited)
}
