package wssession

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// newOutboxSession 构造一个只有出站队列的裸 Session,用于不经真实连接测试推送路径。
func newOutboxSession(capacity int, opts Options) *Session {
	if opts.QueueOfferTimeout == 0 {
		opts.QueueOfferTimeout = time.Second
	}
	return &Session{
		outbox:  make(chan outboundMessage, capacity),
		options: opts,
	}
}

// TestFrameReusedAcrossSessionsSerializesOnce:同一个 Frame 推给多个连接时,
// 各连接入队的是同一份字节(未重复序列化)。
func TestFrameReusedAcrossSessionsSerializesOnce(t *testing.T) {
	t.Parallel()
	f, err := NewFrame(map[string]any{"event": "notice"})
	if err != nil {
		t.Fatalf("NewFrame error = %v", err)
	}

	a := newOutboxSession(1, Options{})
	b := newOutboxSession(1, Options{})
	if err := a.Push(t.Context(), f); err != nil {
		t.Fatalf("Push a error = %v", err)
	}
	if err := b.Push(t.Context(), f); err != nil {
		t.Fatalf("Push b error = %v", err)
	}

	msgA, msgB := <-a.outbox, <-b.outbox
	if string(msgA.data) != string(msgB.data) {
		t.Fatalf("两个连接收到的字节不一致: %s vs %s", msgA.data, msgB.data)
	}
	// 指向同一底层数组即证明没有再次 Marshal。
	if &msgA.data[0] != &msgB.data[0] {
		t.Fatal("Frame 被重复序列化:两次入队的 data 不共享底层数组")
	}
	if msgA.messageType != websocket.TextMessage {
		t.Fatalf("messageType = %d, want TextMessage", msgA.messageType)
	}
}

// TestNewBinaryFramePushedAsBinary:NewBinaryFrame 经 Push 以二进制帧入队。
func TestNewBinaryFramePushedAsBinary(t *testing.T) {
	t.Parallel()
	payload := []byte{0x01, 0x02, 0x03}
	s := newOutboxSession(1, Options{})

	if err := s.Push(t.Context(), NewBinaryFrame(payload)); err != nil {
		t.Fatalf("Push error = %v", err)
	}
	msg := <-s.outbox
	if msg.messageType != websocket.BinaryMessage {
		t.Fatalf("messageType = %d, want BinaryMessage", msg.messageType)
	}
	if string(msg.data) != string(payload) {
		t.Fatalf("data = %v, want %v", msg.data, payload)
	}
}

// TestNewBinaryFrameEmptyIsPushable:空二进制帧是合法帧,不被当成零值 Frame 拒绝。
func TestNewBinaryFrameEmptyIsPushable(t *testing.T) {
	t.Parallel()
	s := newOutboxSession(1, Options{})

	if err := s.Push(t.Context(), NewBinaryFrame(nil)); err != nil {
		t.Fatalf("Push 空二进制帧 error = %v", err)
	}
	if msg := <-s.outbox; msg.messageType != websocket.BinaryMessage || len(msg.data) != 0 {
		t.Fatalf("msg = %+v, want 空二进制帧", msg)
	}
}

// TestNewFrameMarshalError:序列化失败在构造期暴露,不留到推送时。
func TestNewFrameMarshalError(t *testing.T) {
	t.Parallel()
	if _, err := NewFrame(map[string]any{"bad": make(chan int)}); err == nil {
		t.Fatal("NewFrame 对不可序列化 payload 应返回 error")
	}
}

// TestPushZeroValueFrameRejected:零值 Frame(未经构造函数创建)不可推送。
func TestPushZeroValueFrameRejected(t *testing.T) {
	t.Parallel()
	s := newOutboxSession(1, Options{})

	err := s.Push(t.Context(), Frame{})
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("Push error = %v, want ErrInvalidFrame", err)
	}
	select {
	case <-s.outbox:
		t.Fatal("零值 Frame 不应入队")
	default:
	}
}

// TestPushFrameRespectsMaxOutboundFrameBytes:连接级出站上限对 Frame 同样生效。
func TestPushFrameRespectsMaxOutboundFrameBytes(t *testing.T) {
	t.Parallel()
	f, err := NewFrame(map[string]any{"status": "ok"})
	if err != nil {
		t.Fatalf("NewFrame error = %v", err)
	}
	s := newOutboxSession(1, Options{MaxOutboundFrameBytes: len(`{"status":"ok"}`) - 1})

	if err := s.Push(t.Context(), f); !errors.Is(err, ErrOutboundFrameTooLarge) {
		t.Fatalf("Push error = %v, want ErrOutboundFrameTooLarge", err)
	}
	select {
	case <-s.outbox:
		t.Fatal("超限 Frame 不应入队")
	default:
	}
}

// TestTryPushFullQueueFailsImmediately:队列满时 TryPush 立即返回,不等 QueueOfferTimeout。
func TestTryPushFullQueueFailsImmediately(t *testing.T) {
	t.Parallel()
	s := newOutboxSession(1, Options{QueueOfferTimeout: 5 * time.Second})
	// 占满容量为 1 的队列
	if err := s.Push(t.Context(), map[string]any{"n": 1}); err != nil {
		t.Fatalf("预填充 Push error = %v", err)
	}

	start := time.Now()
	err := s.TryPush(t.Context(), map[string]any{"n": 2})
	elapsed := time.Since(start)

	if !errors.Is(err, ErrSlowConsumer) {
		t.Fatalf("TryPush error = %v, want ErrSlowConsumer", err)
	}
	if elapsed > time.Second {
		t.Fatalf("TryPush 等待了 %v,应立即返回而不是等 QueueOfferTimeout", elapsed)
	}
	if len(s.outbox) != 1 {
		t.Fatalf("outbox 长度 = %d, want 1(失败的帧不应入队)", len(s.outbox))
	}
}

// TestTryPushQueuesWhenSpaceAvailable:有空位时 TryPush 正常入队。
func TestTryPushQueuesWhenSpaceAvailable(t *testing.T) {
	t.Parallel()
	s := newOutboxSession(1, Options{})

	if err := s.TryPush(t.Context(), map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("TryPush error = %v", err)
	}
	if got := string((<-s.outbox).data); got != `{"status":"ok"}` {
		t.Fatalf("data = %s", got)
	}
}

// TestTryPushCanceledContext:ctx 已取消时 TryPush 返回 ctx 错误且不入队。
func TestTryPushCanceledContext(t *testing.T) {
	t.Parallel()
	s := newOutboxSession(1, Options{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := s.TryPush(ctx, map[string]any{"status": "ok"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("TryPush error = %v, want context.Canceled", err)
	}
	select {
	case <-s.outbox:
		t.Fatal("ctx 已取消时不应入队")
	default:
	}
}

// benchFanOut 对比扇出时"每连接各自序列化"与"复用同一个 Frame"的开销。
func benchFanOut(b *testing.B, useFrame bool) {
	const fanOut = 16
	payload := map[string]any{"event": "notice", "text": "系统维护通知", "seq": 1}

	sessions := make([]*Session, fanOut)
	for i := range sessions {
		sessions[i] = newOutboxSession(1, Options{})
	}
	ctx := b.Context()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var frame Frame
		if useFrame {
			var err error
			if frame, err = NewFrame(payload); err != nil {
				b.Fatalf("NewFrame error = %v", err)
			}
		}
		for _, s := range sessions {
			var err error
			if useFrame {
				err = s.Push(ctx, frame)
			} else {
				err = s.Push(ctx, payload)
			}
			if err != nil {
				b.Fatalf("Push error = %v", err)
			}
			<-s.outbox // 立即取走,避免队列满影响计时
		}
	}
}

// BenchmarkFanOutPushPayload 每个连接各自序列化同一 payload。
func BenchmarkFanOutPushPayload(b *testing.B) { benchFanOut(b, false) }

// BenchmarkFanOutPushFrame 一次序列化后复用同一个 Frame。
func BenchmarkFanOutPushFrame(b *testing.B) { benchFanOut(b, true) }

// TestTryPushFanOutSkipsSlowConnection:扇出时慢连接立即失败,其余连接照常入队。
func TestTryPushFanOutSkipsSlowConnection(t *testing.T) {
	t.Parallel()
	f, err := NewFrame(map[string]any{"event": "broadcast"})
	if err != nil {
		t.Fatalf("NewFrame error = %v", err)
	}

	slow := newOutboxSession(1, Options{QueueOfferTimeout: 5 * time.Second})
	fast := newOutboxSession(1, Options{QueueOfferTimeout: 5 * time.Second})
	if err := slow.Push(t.Context(), map[string]any{"n": 0}); err != nil { // 占满 slow
		t.Fatalf("预填充 error = %v", err)
	}

	start := time.Now()
	errSlow := slow.TryPush(t.Context(), f)
	errFast := fast.TryPush(t.Context(), f)
	elapsed := time.Since(start)

	if !errors.Is(errSlow, ErrSlowConsumer) {
		t.Fatalf("慢连接 error = %v, want ErrSlowConsumer", errSlow)
	}
	if errFast != nil {
		t.Fatalf("正常连接 error = %v, want nil", errFast)
	}
	if elapsed > time.Second {
		t.Fatalf("整轮扇出耗时 %v,慢连接不应拖住遍历", elapsed)
	}
}
