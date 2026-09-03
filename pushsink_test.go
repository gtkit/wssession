package wssession

import (
	"errors"
	"testing"
	"time"
)

// TestPushMarshalErrorReturned:不可序列化的 payload 应让 Push 立即返回 error,且不入队任何帧。
func TestPushMarshalErrorReturned(t *testing.T) {
	t.Parallel()
	s := &Session{
		outbox:  make(chan outboundMessage, 1),
		options: Options{QueueOfferTimeout: time.Second},
	}
	sink := PushSink(s) // Session 自身实现 PushSink

	// channel 无法被 JSON 序列化
	err := sink.Push(t.Context(), map[string]any{"bad": make(chan int)})
	if err == nil {
		t.Fatal("Push 不可序列化 payload 应返回 error")
	}

	select {
	case <-s.outbox:
		t.Fatal("序列化失败不应入队任何帧")
	default:
	}
}

func TestPushOutboundFrameTooLargeNotQueued(t *testing.T) {
	t.Parallel()
	s := &Session{
		outbox: make(chan outboundMessage, 1),
		options: Options{
			QueueOfferTimeout:     time.Second,
			MaxOutboundFrameBytes: len(`{"status":"ok"}`) - 1,
		},
	}

	err := s.Push(t.Context(), map[string]any{"status": "ok"})
	if !errors.Is(err, ErrOutboundFrameTooLarge) {
		t.Fatalf("Push error = %v, want ErrOutboundFrameTooLarge", err)
	}
	select {
	case <-s.outbox:
		t.Fatal("超限帧不应入队")
	default:
	}
}

func TestPushOutboundFrameSizeEqualLimitQueued(t *testing.T) {
	t.Parallel()
	const payload = `{"status":"ok"}`
	s := &Session{
		outbox: make(chan outboundMessage, 1),
		options: Options{
			QueueOfferTimeout:     time.Second,
			MaxOutboundFrameBytes: len(payload),
		},
	}

	if err := s.Push(t.Context(), map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("Push error = %v", err)
	}
	if got := string((<-s.outbox).data); got != payload {
		t.Fatalf("data = %s, want %s", got, payload)
	}
}

func TestPushOutboundFrameDefaultUnlimited(t *testing.T) {
	t.Parallel()
	s := &Session{
		outbox:  make(chan outboundMessage, 1),
		options: Options{QueueOfferTimeout: time.Second},
	}

	if err := s.Push(t.Context(), map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("Push error = %v", err)
	}
	if len((<-s.outbox).data) == 0 {
		t.Fatal("默认不限时应正常入队")
	}
}

// TestPushSerializesToBytes:正常 payload 被序列化为文本帧字节后入队。
func TestPushSerializesToBytes(t *testing.T) {
	t.Parallel()
	s := &Session{
		outbox:  make(chan outboundMessage, 1),
		options: Options{QueueOfferTimeout: time.Second},
	}
	sink := PushSink(s) // Session 自身实现 PushSink

	if err := sink.Push(t.Context(), map[string]any{"status": "ok"}); err != nil {
		t.Fatalf("Push error = %v", err)
	}
	msg := <-s.outbox
	if len(msg.data) == 0 {
		t.Fatal("入队帧 data 为空")
	}
	if string(msg.data) != `{"status":"ok"}` {
		t.Fatalf("data = %s, want {\"status\":\"ok\"}", msg.data)
	}
}
