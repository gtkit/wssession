package wssession

import (
	"context"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestPipelinedBinaryAfterFirstFrameAccepted:客户端不等 subscribed 回执就把
// 首帧与随后的二进制帧连续发出(语音分片等场景的常见写法)时,第二帧**不是**首帧,
// 不得被判成"首帧是二进制帧"。
//
// 回归防护:准入判定曾复用 subscribed(它表示 ParseRequest+tokenCap 已完成),
// 而 readLoop 与 processLoop 是两个 goroutine + inbox 有缓冲,导致 readLoop 能在
// subscribed 置位前就读到第二帧,把合法客户端误杀。ParseRequest 内的延时把这个
// 窗口放大成 100% 稳定复现。
func TestPipelinedBinaryAfterFirstFrameAccepted(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := Handlers{
		ParseRequest: func(_ context.Context, _ []byte) (string, any, error) {
			time.Sleep(50 * time.Millisecond) // 放大 readLoop 与 processLoop 的时间差
			return "tok", nil, nil
		},
		OnBinaryMessage: func(ctx context.Context, raw []byte, sink PushSink) error {
			return sink.Push(ctx, map[string]any{"got": len(raw)})
		},
	}
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	// 连续发出:首帧(文本)+ 业务帧(二进制),不等 subscribed
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = conn.WriteMessage(websocket.BinaryMessage, []byte{0x01, 0x02, 0x03})

	if got := readJSONFrame(t, conn, 3*time.Second); got["event"] != "subscribed" {
		t.Fatalf("首帧回执 = %v, want subscribed", got)
	}
	got := readJSONFrame(t, conn, 3*time.Second)
	if got["event"] == "error" {
		t.Fatalf("流水线发送的第二帧被误判为首帧: code=%v reason=%v", got["code"], got["reason"])
	}
	if got["got"] != float64(3) {
		t.Fatalf("二进制帧未被正常处理: %v", got)
	}
}

// TestPipelinedTextViolationReports422:只提供 OnBinaryMessage 时,流水线发出的
// 第二个文本帧属协议违规,必须是 error(422),不能落到内部错误 500。
func TestPipelinedTextViolationReports422(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := Handlers{
		ParseRequest: func(_ context.Context, _ []byte) (string, any, error) {
			time.Sleep(50 * time.Millisecond)
			return "tok", nil, nil
		},
		OnBinaryMessage: func(context.Context, []byte, PushSink) error { return nil },
	}
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`second`))

	msg := readErrorFrame(t, conn)
	if msg["code"] != float64(CodeInvalidParam) {
		t.Fatalf("code = %v, want %d(协议违规),不应是内部错误", msg["code"], CodeInvalidParam)
	}
	if msg["reason"] != ReasonUnexpectedFrame {
		t.Fatalf("reason = %v, want %q", msg["reason"], ReasonUnexpectedFrame)
	}
}

// TestPipelinedTextInUnidirectionalReports422:单向模式下(仅 Run)流水线发出的
// 第二个文本帧同样应立即被判协议违规,而不是被静默留在 inbox 里无人处理。
func TestPipelinedTextInUnidirectionalReports422(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	h := Handlers{
		ParseRequest: func(_ context.Context, _ []byte) (string, any, error) {
			time.Sleep(50 * time.Millisecond)
			return "tok", nil, nil
		},
		Run: func(ctx context.Context, _ any, _ PushSink) error {
			<-ctx.Done()
			return nil
		},
	}
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`second`))

	msg := readErrorFrame(t, conn)
	if msg["code"] != float64(CodeInvalidParam) {
		t.Fatalf("code = %v, want %d", msg["code"], CodeInvalidParam)
	}
}
