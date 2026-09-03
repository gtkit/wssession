package wssession

import (
	"context"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	gtkitjson "github.com/gtkit/json/v2"
)

// binaryHandlers 构造只处理二进制消息的双向 Handlers。
func binaryHandlers(onBin func(ctx context.Context, raw []byte, sink PushSink) error) Handlers {
	return Handlers{
		ParseRequest: func(_ context.Context, _ []byte) (string, any, error) {
			return "tok", nil, nil
		},
		OnBinaryMessage: onBin,
	}
}

// subscribe 发首帧并读掉 subscribed 确认帧。
func subscribe(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	if got := readJSONFrame(t, conn, 2*time.Second); got["event"] != "subscribed" {
		t.Fatalf("首帧回执 = %v, want subscribed", got)
	}
}

// readErrorFrame 读到第一帧 error JSON 并返回。
func readErrorFrame(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("等待 error 帧时读失败: %v", err)
		}
		var msg map[string]any
		if err := gtkitjson.Unmarshal(raw, &msg); err != nil {
			continue
		}
		if msg["event"] == "error" {
			return msg
		}
	}
}

// TestBinaryMessageAfterSubscribeTriggersHandler:订阅后的二进制帧触发 OnBinaryMessage。
func TestBinaryMessageAfterSubscribeTriggersHandler(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := binaryHandlers(func(ctx context.Context, raw []byte, sink PushSink) error {
		return sink.Push(ctx, map[string]any{"len": len(raw), "first": int(raw[0])})
	})
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	subscribe(t, conn)
	_ = conn.WriteMessage(websocket.BinaryMessage, []byte{0x07, 0x08, 0x09})

	got := readJSONFrame(t, conn, 2*time.Second)
	if got["len"] != float64(3) || got["first"] != float64(7) {
		t.Fatalf("OnBinaryMessage 收到的字节不对: %v", got)
	}
}

// TestBinaryEchoViaNewBinaryFrame:回调内经 NewBinaryFrame 回复二进制帧。
func TestBinaryEchoViaNewBinaryFrame(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := binaryHandlers(func(ctx context.Context, raw []byte, sink PushSink) error {
		return sink.Push(ctx, NewBinaryFrame(raw))
	})
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	subscribe(t, conn)
	want := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	_ = conn.WriteMessage(websocket.BinaryMessage, want)

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	msgType, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if msgType != websocket.BinaryMessage {
		t.Fatalf("messageType = %d, want BinaryMessage", msgType)
	}
	if string(raw) != string(want) {
		t.Fatalf("回帧 = %v, want %v", raw, want)
	}
}

// TestBinaryFirstFrameRejected:首帧必须是文本帧,二进制首帧下发专用 reason。
func TestBinaryFirstFrameRejected(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := binaryHandlers(func(context.Context, []byte, PushSink) error { return nil })
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.BinaryMessage, []byte{0x01})

	msg := readErrorFrame(t, conn)
	if msg["code"] != float64(CodeInvalidFrameType) {
		t.Fatalf("code = %v, want %d", msg["code"], CodeInvalidFrameType)
	}
	if msg["reason"] != ReasonBinaryFirstFrame {
		t.Fatalf("reason = %v, want %q", msg["reason"], ReasonBinaryFirstFrame)
	}
}

// TestTextFrameRejectedWhenOnlyBinaryHandler:仅提供 OnBinaryMessage 时,
// 订阅后的文本帧按既有协议违规路径处理(422)。
func TestTextFrameRejectedWhenOnlyBinaryHandler(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := binaryHandlers(func(context.Context, []byte, PushSink) error { return nil })
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	subscribe(t, conn)
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`extra`))

	msg := readErrorFrame(t, conn)
	if msg["code"] != float64(CodeInvalidParam) {
		t.Fatalf("code = %v, want %d", msg["code"], CodeInvalidParam)
	}
	if msg["reason"] != ReasonUnexpectedFrame {
		t.Fatalf("reason = %v, want %q", msg["reason"], ReasonUnexpectedFrame)
	}
}

// TestOnBinaryMessageAloneEnablesDuplex:只提供 OnBinaryMessage 即可启用双向模式,
// 不再要求 Run。
func TestOnBinaryMessageAloneEnablesDuplex(t *testing.T) {
	t.Parallel()
	h := binaryHandlers(func(context.Context, []byte, PushSink) error { return nil })
	if err := h.validate(); err != nil {
		t.Fatalf("validate() = %v, want nil", err)
	}
	if !h.duplexEnabled() {
		t.Fatal("只提供 OnBinaryMessage 应启用双向模式")
	}
}

// TestBinaryAndTextHandlersCoexist:两个回调同时提供时按帧类型分别派发。
func TestBinaryAndTextHandlersCoexist(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := Handlers{
		ParseRequest: func(_ context.Context, _ []byte) (string, any, error) {
			return "tok", nil, nil
		},
		OnMessage: func(ctx context.Context, _ []byte, sink PushSink) error {
			return sink.Push(ctx, map[string]any{"kind": "text"})
		},
		OnBinaryMessage: func(ctx context.Context, _ []byte, sink PushSink) error {
			return sink.Push(ctx, map[string]any{"kind": "binary"})
		},
	}
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	subscribe(t, conn)

	_ = conn.WriteMessage(websocket.TextMessage, []byte(`hi`))
	if got := readJSONFrame(t, conn, 2*time.Second); got["kind"] != "text" {
		t.Fatalf("文本帧派发到 = %v, want text", got)
	}
	_ = conn.WriteMessage(websocket.BinaryMessage, []byte{0x01})
	if got := readJSONFrame(t, conn, 2*time.Second); got["kind"] != "binary" {
		t.Fatalf("二进制帧派发到 = %v, want binary", got)
	}
}

// TestBinaryReadLimitExceeded:二进制帧同样受 ReadLimit 约束。
func TestBinaryReadLimitExceeded(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := binaryHandlers(func(context.Context, []byte, PushSink) error { return nil })
	srv := newTestSession(t, path, Options{ReadLimit: 32}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	subscribe(t, conn)
	_ = conn.WriteMessage(websocket.BinaryMessage, make([]byte, 64))

	// 超限触发 gorilla ErrReadLimit → 连接收敛,客户端后续读必然失败
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return // 预期:连接被关闭
		}
	}
}
