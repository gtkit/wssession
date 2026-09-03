package wssession

import (
	"context"
	"fmt"

	gtkitjson "github.com/gtkit/json/v2"

	"github.com/gorilla/websocket"
)

// PushSink 是业务侧向客户端推帧的入口。
//
// 实现细节(outbox channel + writeLoop)对业务透明;
// 业务只需调 sink.Push(payload) 即可,payload 在 Push 内用 gtkitjson 序列化为文本帧。
// 需要推二进制帧、或复用一份已序列化的帧时,把 Frame 当 payload 传进来
// (见 NewFrame / NewBinaryFrame)——接口保持单方法,不因新增帧形态而变。
//
// 队列满 + QueueOfferTimeout(默认 5s)仍无消费 → 返回 ErrSlowConsumer,
// 业务侧应 return 让 wssession 收敛连接(close 慢客户端)。
//
// *Session 实现本接口:Run / OnMessage / OnBinaryMessage 收到的 sink 即所属 Session;
// 持有 *Session(OnConnect 注入)的代码也可直接 Push(如 sessionhub 定向推送)。
type PushSink interface {
	Push(ctx context.Context, payload any) error
}

// Frame 是一次序列化完成、可跨连接复用的出站帧。
//
// 用于把同一 payload 推给多个连接的扇出场景:NewFrame 只序列化一次,之后把同一个
// Frame 传给各连接的 Push / TryPush,避免每个连接重复 Marshal;也是推送二进制帧的
// 载体(NewBinaryFrame)。
//
// Frame 自身的字段构造后不再变更,同一个 Frame 可被多个 goroutine 并发推送。
// 零值不可推送(推送时返回包装 ErrInvalidFrame 的错误)——必须经 NewFrame /
// NewBinaryFrame 创建。
//
// **注意**:NewBinaryFrame 持有调用方字节切片的**别名**(不复制),所以"不可变"
// 只覆盖 Frame 的字段,不代表底层字节安全——详见 NewBinaryFrame。
type Frame struct {
	// messageType 为 0 即零值 Frame(未经构造函数创建);合法值只有文本 / 二进制。
	messageType int
	data        []byte
}

// NewFrame 把 payload 用 gtkitjson 序列化为一个待推送的文本帧。
//
// 序列化失败时返回错误,此时返回的 Frame 不可用于推送。
// 出站单帧上限(Options.MaxOutboundFrameBytes)是连接级配置,在推送时按目标连接
// 校验,不在这里检查。
func NewFrame(payload any) (Frame, error) {
	data, err := gtkitjson.Marshal(payload)
	if err != nil {
		return Frame{}, fmt.Errorf("wssession: marshal frame payload: %w", err)
	}
	return Frame{messageType: websocket.TextMessage, data: data}, nil
}

// NewBinaryFrame 以给定字节构造一个待推送的二进制帧。data 为 nil 或空切片时
// 构造出的是一个合法的空二进制帧。
//
// **data 不被复制,且出帧是异步的**:Push / TryPush 返回 nil 只代表帧已进入出站
// 队列,真正写到线路上发生在之后的写循环里。因此推送之后**不能**再修改或复用
// 这段字节——典型事故是从 sync.Pool 取 buffer、Push 返回后立刻归还或覆写,
// 客户端会收到损坏数据(-race 下可见为 writeLoop 与业务 goroutine 的数据竞争)。
//
// 需要复用 buffer 时自己先复制:NewBinaryFrame(bytes.Clone(buf))。
// NewFrame 无此问题——序列化产生的是新分配的字节。
func NewBinaryFrame(data []byte) Frame {
	return Frame{messageType: websocket.BinaryMessage, data: data}
}

// Push 把 payload 序列化后塞进该连接的出站队列(实现 PushSink)。
//
// payload 为 Frame 或 *Frame 时原样入队(不重复序列化);其它值用 gtkitjson 序列化为文本帧。
//
// 并发安全,可与 Run / OnMessage 的推送并存(出帧由 writeLoop 串行写出,
// 帧序由入队时刻决定)。返回 ErrSlowConsumer / ErrOutboundFrameTooLarge /
// ErrInvalidFrame / ctx.Err 时调用方应停止或丢弃该业务推送。
func (s *Session) Push(ctx context.Context, payload any) error {
	msg, err := s.outboundFor(payload)
	if err != nil {
		return err
	}
	return s.queue(ctx, msg)
}

// TryPush 与 Push 相同,但**不等待**队列腾出空位:队列已满时立即返回
// ErrSlowConsumer,该帧不入队。
//
// 用于扇出场景——把同一个 Frame 推给大量连接时,阻塞式 Push 会让整轮遍历被单个
// 慢客户端拖住最多 QueueOfferTimeout(默认 5s);TryPush 让慢连接立即失败,
// 其余连接照常推送。
//
// 与 Push 共享同一出站队列与帧序语义。
func (s *Session) TryPush(ctx context.Context, payload any) error {
	msg, err := s.outboundFor(payload)
	if err != nil {
		return err
	}
	return s.offer(ctx, msg)
}

// outboundFor 把业务 payload 归一化为待发送帧。
//
// Frame 与 *Frame 都按预序列化帧原样入队——Frame 只有未导出字段,若漏掉指针形态,
// *Frame 会落到 JSON 序列化被静默推成 "{}"。其它值用 gtkitjson 序列化为文本帧,
// 序列化在业务 goroutine 侧完成(可并行),writeLoop 只做纯 IO。
func (s *Session) outboundFor(payload any) (outboundMessage, error) {
	var frame Frame
	switch p := payload.(type) {
	case Frame:
		frame = p
	case *Frame:
		if p == nil {
			return outboundMessage{}, fmt.Errorf("wssession: nil *Frame is not pushable: %w", ErrInvalidFrame)
		}
		frame = *p
	default:
		data, err := gtkitjson.Marshal(payload)
		if err != nil {
			return outboundMessage{}, fmt.Errorf("wssession: marshal push payload: %w", err)
		}
		if err := s.checkOutboundSize(len(data)); err != nil {
			return outboundMessage{}, err
		}
		return outboundMessage{messageType: websocket.TextMessage, data: data}, nil
	}

	if frame.messageType == 0 {
		return outboundMessage{}, fmt.Errorf("wssession: zero-value Frame is not pushable, create it with NewFrame or NewBinaryFrame: %w", ErrInvalidFrame)
	}
	if err := s.checkOutboundSize(len(frame.data)); err != nil {
		return outboundMessage{}, err
	}
	return outboundMessage{messageType: frame.messageType, data: frame.data}, nil
}

// checkOutboundSize 按 Options.MaxOutboundFrameBytes 校验单帧字节数(<=0 不限)。
func (s *Session) checkOutboundSize(size int) error {
	if s.options.MaxOutboundFrameBytes > 0 && size > s.options.MaxOutboundFrameBytes {
		return fmt.Errorf("wssession: outbound frame size %d exceeds MaxOutboundFrameBytes %d: %w", size, s.options.MaxOutboundFrameBytes, ErrOutboundFrameTooLarge)
	}
	return nil
}

// Kick 把该连接踢下线:下发 error(409, reason) 帧并以 close 1008 完成关闭
// 握手后收敛连接。用于单点登录顶号、管理端强制下线等场景
// (典型配合 sessionhub:遍历 Conns(userID) 逐个 Kick)。
//
// 幂等:与其它错误关闭路径共享同一幂等域,重复调用只下发首帧。
// 踢下线是预期关闭,被踢连接的 Serve 返回 nil,不作为错误上抛。
// 客户端契约:收到 error(409) 应提示被顶下线且不自动重连。
func (s *Session) Kick(ctx context.Context, reason string) {
	s.closeWithError(ctx, CodeConflict, reason, nil)
}
