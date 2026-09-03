package wssession

import (
	"context"
	"time"

	gtkitjson "github.com/gtkit/json/v2"

	"github.com/gorilla/websocket"
)

// jsonFrame 把一个内部帧结构序列化为待发送的文本帧。
//
// 仅用于包内固定结构(subscribedFrame / errorFrame),其序列化不会失败,
// 故忽略 Marshal 错误;业务 payload 走 PushSink.Push,那里会回传序列化错误。
func jsonFrame(payload any) outboundMessage {
	data, _ := gtkitjson.Marshal(payload)
	return outboundMessage{messageType: websocket.TextMessage, data: data}
}

// outboundMessage 是 outbox channel 中的一条待发送帧。
//
// data 是**已序列化**的字节,writeLoop 只做 WriteMessage(messageType, data) 纯 IO——
// JSON 序列化在入队前(Push / 帧构造侧)用 gtkitjson 完成,不在 writeLoop 内做。
//
// done 字段是可选的同步信号:writeLoop 写出该帧后会 close(done),
// 让需要"等待 flush 完成"的调用方(如 closeWithError 下发 error 帧后才能关连接)
// 同步等待。业务侧 PushSink.Push 不使用 done(异步推帧无需等)。
//
// 仅包内使用;业务通过 PushSink.Push 间接入队。
type outboundMessage struct {
	messageType int
	data        []byte
	done        chan struct{}
}

// queue 把消息塞进 outbox channel,实现有界队列 + 反压超时。
//
// 反压三段式:
//  1. 立即非阻塞 send(channel 有空位则纳秒返回)
//  2. ctx done / channel send 都阻塞时,启 QueueOfferTimeout 定时器
//  3. 任一信号到达即返回(ctx.Err / nil / ErrSlowConsumer)
//
// 调用方:
//   - pushSink.Push(JSON 业务帧)
//   - Session.closeWithError(error 帧)
//   - Session.queueSubscribed(订阅确认帧)
func (s *Session) queue(ctx context.Context, msg outboundMessage) error {
	return s.queueWithTimeout(ctx, msg, s.options.QueueOfferTimeout)
}

// offer 尝试把消息塞进 outbox 但**不等待**空位:满则立即返回 ErrSlowConsumer。
//
// 调用方:Session.TryPush(扇出场景不能被单个慢连接拖住整轮遍历)。
func (s *Session) offer(ctx context.Context, msg outboundMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.outbox <- msg:
		return nil
	default:
		return ErrSlowConsumer
	}
}

func (s *Session) queueWithTimeout(ctx context.Context, msg outboundMessage, timeout time.Duration) error {
	// 先确定性检查 ctx:若已取消,select 在"ctx.Done 与入队同时就绪"时会随机
	// 二选一,可能把帧塞进 writeLoop 已退出(drain 已结束)的 outbox——
	// 帧不会被写出,done 信号也无人兑现,等待方只能靠兜底超时解围。
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.outbox <- msg:
		return nil
	default:
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.outbox <- msg:
		return nil
	case <-timer.C:
		return ErrSlowConsumer
	}
}
