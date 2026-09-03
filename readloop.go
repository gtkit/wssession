package wssession

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

// inboundFrame 是 readLoop → processLoop 的载体。
//
// 包内私有(业务侧不直接处理 inbound,首帧由 processLoop 调 Handlers.ParseRequest)。
type inboundFrame struct {
	raw []byte
	// messageType 是 WebSocket 帧类型(文本 / 二进制),双向调度器据此选择
	// OnMessage 还是 OnBinaryMessage。
	messageType int
}

// readLoop 是底层唯一 reader。
//
// 职责切分:
//   - 读 wsConn → 帧类型准入 → 扔进 inbox channel
//   - 维护 read deadline(配合 PongHandler)
//   - 单向模式下首帧之后再收到业务帧 → 判协议违规并收敛连接
//   - 业务解析(JSON parse)由 processLoop 完成
//
// 这样 readLoop 在 ParseRequest / Run 内做慢操作时仍可继续读 Pong。
func (s *Session) readLoop(ctx context.Context, cancel context.CancelFunc) (err error) {
	defer func() {
		if p := recover(); p != nil {
			// panic 兜底:转成 error 经 errgroup 上抛(不再静默吞没),
			// 并 cancel 让其余 goroutine 收敛。
			err = fmt.Errorf("wssession: panic in readLoop: %v", p)
			s.options.emit(ctx, Event{Type: EventPanic, Reason: "panic in readLoop", Err: err})
			cancel()
		}
	}()

	s.wsConn.SetReadLimit(s.options.ReadLimit)
	if err := s.wsConn.SetReadDeadline(time.Now().Add(s.options.PongWait)); err != nil {
		return err
	}
	s.wsConn.SetPongHandler(func(string) error {
		return s.wsConn.SetReadDeadline(time.Now().Add(s.options.PongWait))
	})

	// firstFrameRead 标记首帧(ParseRequest 的鉴权 / 订阅帧)是否已被读取并投递。
	//
	// 用 readLoop 的局部状态而非 s.subscribed:后者表示"processLoop 已跑完
	// ParseRequest + tokenCap",而 readLoop 与 processLoop 是两个 goroutine、inbox
	// 又有缓冲,readLoop 完全可能在 subscribed 置位前就读到第二帧。用它做"首帧"
	// 判定会把不等 subscribed 回执就连续发帧的合法客户端误杀。readLoop 是唯一
	// reader,局部变量天然无竞态。
	firstFrameRead := false

	for {
		msgType, raw, err := s.wsConn.ReadMessage()
		if err != nil {
			// 包括 close 帧 / 网络断 / ReadLimit 超 / deadline 超 → 都让 errgroup 收敛
			s.emitClientClose(ctx, err)
			return err
		}
		// 收到任何业务帧都续 read deadline(Pong 控制帧由 PongHandler 单独处理,
		// gorilla 内部消化不返回 messageType)
		if err := s.wsConn.SetReadDeadline(time.Now().Add(s.options.PongWait)); err != nil {
			return err
		}

		// 帧类型准入(见 admitInbound):不接受的帧已在其内部下发 error 帧并收敛连接
		if err := s.admitInbound(ctx, msgType, firstFrameRead); err != nil {
			return err
		}

		// 扔进 inbox channel,processLoop 消费
		frame := inboundFrame{raw: raw, messageType: msgType}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.inbox <- frame:
			firstFrameRead = true
			// continue 继续读下一帧(为了在订阅后继续处理 Pong / close 帧)
		}
	}
}

// admitInbound 校验入站帧类型是否被当前 Handlers 与连接状态接受。
//
// firstFrameRead 表示首帧(ParseRequest 的鉴权 / 订阅帧)是否已被读取。它来自
// readLoop 的局部状态,与"processLoop 是否已完成 ParseRequest"无关——见 readLoop
// 内的说明。
//
// 不接受时下发对应 error 帧、收敛连接,并返回原因供 readLoop 上抛:
//   - 文本帧:首帧永远放行;首帧之后的文本帧需要 OnMessage,否则是协议违规(422)
//   - 二进制帧:未提供 OnBinaryMessage → 415(不支持);首帧不得是二进制帧 → 415
//   - 其它类型:415(防御分支,gorilla 只会返回文本 / 二进制)
func (s *Session) admitInbound(ctx context.Context, msgType int, firstFrameRead bool) error {
	switch msgType {
	case websocket.TextMessage:
		if firstFrameRead && s.handlers.OnMessage == nil {
			s.closeWithError(ctx, CodeInvalidParam, ReasonUnexpectedFrame, ErrUnexpectedFrame)
			return ErrUnexpectedFrame
		}
		return nil
	case websocket.BinaryMessage:
		if s.handlers.OnBinaryMessage == nil {
			s.closeWithError(ctx, CodeInvalidFrameType, ReasonBinaryFrameUnsupported, ErrInvalidFrame)
			return ErrInvalidFrame
		}
		if !firstFrameRead {
			s.closeWithError(ctx, CodeInvalidFrameType, ReasonBinaryFirstFrame, ErrInvalidFrame)
			return ErrInvalidFrame
		}
		return nil
	default:
		s.closeWithError(ctx, CodeInvalidFrameType, ReasonBinaryFrameUnsupported, ErrInvalidFrame)
		return ErrInvalidFrame
	}
}

// emitClientClose 在读到客户端 close 帧时上报 EventClientClose(携带 close code 与文案)。
//
// 两类情况不在此上报,否则会污染关闭原因统计:
//   - 1006:gorilla 也用 CloseError{Code:1006} 表示"没有 close 握手就断了",那不是
//     客户端的关闭意图,已由 Serve 层的 EventAbnormalClose 覆盖;
//   - 服务端已主动发起 close 握手:读到的只是客户端对我方 close 的回应,携带的是
//     服务端自己下发的 code,该次关闭已由发起方的路径反映。
func (s *Session) emitClientClose(ctx context.Context, err error) {
	if s.serverClosing.Load() {
		return
	}
	closeErr, ok := errors.AsType[*websocket.CloseError](err)
	if !ok || closeErr.Code == websocket.CloseAbnormalClosure {
		return
	}
	// Reason 是**客户端可控文本**:按与 error 帧同一上限截断,调用方落日志前
	// 应自行处理换行等控制字符(见 EventClientClose 的说明)。
	s.options.emit(ctx, Event{
		Type:   EventClientClose,
		Reason: truncateErrorReason(closeErr.Text),
		Code:   closeErr.Code,
	})
}
