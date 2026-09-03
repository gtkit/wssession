package wssession

import (
	"context"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

// writeLoop 是底层唯一 writer。
//
// 所有出帧(业务推送、订阅确认、error 帧、Ping 心跳)都从这个 goroutine 串行写出,
// 满足 gorilla/websocket 对"单连接单 writer"的要求。
//
// 退出路径:
//   - ctx.Done() → 自然结束
//   - outbox 收到的帧 WriteMessage 失败 → return err 让 errgroup 收敛
//   - Ping 失败(客户端假死) → return err
func (s *Session) writeLoop(ctx context.Context, cancel context.CancelFunc) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("wssession: panic in writeLoop: %v", p)
			s.options.emit(ctx, Event{Type: EventPanic, Reason: "panic in writeLoop", Err: err})
			cancel()
		}
	}()

	// 退出时 drain outbox:滞留帧不再写出,但兑现其 done 信号,
	// 让 closeWithError / closeNormal 的等待方立即解除阻塞而非等满 1s 兜底。
	// drain 之后才入队的帧由等待方的兜底超时覆盖。
	defer func() {
		for {
			select {
			case msg := <-s.outbox:
				if msg.done != nil {
					close(msg.done)
				}
			default:
				return
			}
		}
	}()

	pingTicker := time.NewTicker(s.options.PingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case msg := <-s.outbox:
			if err := s.writeOutbound(msg); err != nil {
				return err
			}

		case <-pingTicker.C:
			if err := s.wsConn.SetWriteDeadline(time.Now().Add(s.options.WriteWait)); err != nil {
				return err
			}
			if err := s.wsConn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return err
			}
		}
	}
}

func (s *Session) writeOutbound(msg outboundMessage) error {
	if msg.done != nil {
		defer close(msg.done)
	}
	if err := s.wsConn.SetWriteDeadline(time.Now().Add(s.options.WriteWait)); err != nil {
		return err
	}
	// data 已是序列化字节,writeLoop 只做纯 IO。
	return s.wsConn.WriteMessage(msg.messageType, msg.data)
}
