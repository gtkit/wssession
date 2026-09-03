package wssession

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// errBusinessFailure 模拟业务错误,触发服务端主动的错误关闭路径。
var errBusinessFailure = errors.New("business failure")

// waitForEventDetail 等待指定类型事件并返回它,超时失败。
func waitForEventDetail(t *testing.T, events <-chan Event, want EventType) Event {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-events:
			if ev.Type == want {
				return ev
			}
		case <-deadline:
			t.Fatalf("3s 内未收到事件 %v", want)
		}
	}
}

// TestEventClientCloseCarriesCustomCode:客户端以自定义 close code + 文案关闭时,
// 事件携带 code 与 reason。
func TestEventClientCloseCarriesCustomCode(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 8)
	opts := Options{OnEvent: func(_ context.Context, ev Event) { events <- ev }}
	srv := newTestSession(t, path, opts, passthroughHandlers(nil))

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"t"}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	_ = conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(4001, "user logout"),
		time.Now().Add(time.Second))

	ev := waitForEventDetail(t, events, EventClientClose)
	if ev.Code != 4001 {
		t.Fatalf("Event.Code = %d, want 4001", ev.Code)
	}
	if ev.Reason != "user logout" {
		t.Fatalf("Event.Reason = %q, want \"user logout\"", ev.Reason)
	}
}

// TestEventClientCloseNormalClosure:1000 无文案关闭同样上报,Reason 为空串。
func TestEventClientCloseNormalClosure(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 8)
	opts := Options{OnEvent: func(_ context.Context, ev Event) { events <- ev }}
	srv := newTestSession(t, path, opts, passthroughHandlers(nil))

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"t"}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	_ = conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(time.Second))

	ev := waitForEventDetail(t, events, EventClientClose)
	if ev.Code != websocket.CloseNormalClosure {
		t.Fatalf("Event.Code = %d, want 1000", ev.Code)
	}
	if ev.Reason != "" {
		t.Fatalf("Event.Reason = %q, want 空串", ev.Reason)
	}
}

// TestAbnormalCloseDoesNotEmitClientClose:1006(无 close 握手的异常断开)只走
// EventAbnormalClose,不重复上报 EventClientClose——两者语义不同。
func TestAbnormalCloseDoesNotEmitClientClose(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 16)
	opts := Options{OnEvent: func(_ context.Context, ev Event) { events <- ev }}
	srv := newTestSession(t, path, opts, passthroughHandlers(nil))

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"t"}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	// 直接关底层 TCP,不发 close 帧 → 服务端读到 1006
	_ = conn.UnderlyingConn().Close()

	deadline := time.After(3 * time.Second)
	for {
		select {
		case ev := <-events:
			if ev.Type == EventClientClose {
				t.Fatalf("1006 异常断开不应上报 EventClientClose(code=%d)", ev.Code)
			}
			if ev.Type == EventAbnormalClose {
				return // 预期路径
			}
		case <-deadline:
			t.Fatal("3s 内未收到 EventAbnormalClose")
		}
	}
}

// TestServerInitiatedCloseDoesNotEmitClientClose:服务端主动关闭时,客户端对我方
// close 的回应不应被计为客户端主动关闭(否则同一次关闭被双重计数)。
func TestServerInitiatedCloseDoesNotEmitClientClose(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	events := make(chan Event, 16)
	opts := Options{OnEvent: func(_ context.Context, ev Event) { events <- ev }}

	// Run 返回业务错误 → 服务端下发 error(500) + close(1011) 收敛连接
	h := passthroughHandlers(func(context.Context, PushSink) error {
		return errBusinessFailure
	})
	srv := newTestSession(t, path, opts, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"t"}`))

	// 读到连接关闭:gorilla 客户端在此期间会按协议回应服务端的 close 帧
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}

	// 给可能的误报留出到达窗口
	time.Sleep(100 * time.Millisecond)
	for {
		select {
		case ev := <-events:
			if ev.Type == EventClientClose {
				t.Fatalf("服务端主动关闭不应上报 EventClientClose(code=%d)", ev.Code)
			}
		default:
			return
		}
	}
}

// TestEventClientCloseTypeString:事件可读名稳定。
func TestEventClientCloseTypeString(t *testing.T) {
	t.Parallel()
	if got := EventClientClose.String(); got != "client_close" {
		t.Fatalf("EventClientClose.String() = %q, want client_close", got)
	}
}
