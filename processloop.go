package wssession

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// processLoop 是首帧编排 + Run 调度。
//
// 流程(详见 docs/wsmsg-flow.md §1 主路径):
//
//	① 启动 firstFrameTimer
//	② 等 inbox 首帧(超时 / 收到帧 / ctx done 三选一)
//	③ Handlers.ParseRequest → (key, req, err) → 失败下发 error(422) + close
//	④ tokenCap.tryAcquire(key)            → 满下发 error(429) + close
//	⑤ 下发 subscribed 确认帧
//	⑥ 单向模式(无消息回调):Handlers.Run 同步调用 + 返回值分发
//	   双向模式(OnMessage / OnBinaryMessage 至少一个非 nil):进入 duplexLoop
//	   按 Options.Dispatch 调度后续消息(详见 duplex.go)
//
// 单向模式下 Run 在本 goroutine 内**同步**调用——这是有意为之:
//   - Run 自身是 blocking 业务循环(snapshot / poll / push),不是消息回调
//   - readLoop 已经独立 goroutine,不被 Run 阻塞
//   - 单 Session 内不需要并发处理多条 inbound 帧(单向协议约定"一连接一订阅")
func (s *Session) processLoop(ctx context.Context, cancel context.CancelFunc) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("wssession: panic in processLoop: %v", p)
			s.options.emit(ctx, Event{Type: EventPanic, Reason: "panic in processLoop", Err: err})
			cancel()
		}
	}()

	// ① 首帧超时 timer:到时**抢占** claim,成功才下发 408 + close。
	// 与下面"首帧到达"路径用同一个 firstFrameClaimed CAS 互斥,避免竞态。
	firstFrameTimer := time.AfterFunc(s.options.FirstFrameTimeout, func() {
		if s.firstFrameClaimed.CompareAndSwap(false, true) {
			s.closeWithError(ctx, CodeFirstFrameTimeout, ReasonFirstFrameTimeout)
		}
	})

	// ② 等首帧
	var firstFrame inboundFrame
	select {
	case <-ctx.Done():
		firstFrameTimer.Stop()
		return ctx.Err()
	case firstFrame = <-s.inbox:
		firstFrameTimer.Stop()
		// 抢占 claim:抢不到说明超时回调已先动连接,直接退出。
		if !s.firstFrameClaimed.CompareAndSwap(false, true) {
			return ErrFirstFrameTimeout
		}
	}

	// ③ Handlers.ParseRequest
	key, req, parseErr := s.handlers.ParseRequest(ctx, firstFrame.raw)
	if parseErr != nil {
		s.closeWithError(ctx, CodeInvalidParam, parseErr.Error())
		return parseErr
	}
	// 存下业务请求对象,供 Session.Value() 跨 goroutine 读取(双向模式各轮回调)。
	s.req.Store(&req)

	// ④ token 维度 connCap(key 为空时跳过,让 handler 业务自己拒)
	if key != "" && s.options.ConnCapEnabled {
		tokenCapKey := "token:" + key + ":" + s.path
		_, ok := tryAcquire(tokenCapKey, s.options.ConnCapKeyMax)
		if !ok {
			s.options.emit(ctx, Event{Type: EventCapRejected, Reason: ReasonTooManyTokenConn, Key: tokenCapKey})
			s.closeWithError(ctx, CodeTooManyConn, ReasonTooManyTokenConn)
			return errors.New("wssession: token connCap exceeded")
		}
		// 释放挂在 Serve 退出路径(见 Session.tokenCapKey):cap 占用覆盖整条
		// 连接的生命周期,不随 processLoop 提前退出而提前释放。
		s.tokenCapKey = tokenCapKey
	}

	// ⑤ 下发订阅确认帧(首帧之后的帧准入由 readLoop 自己的局部状态判定,见 admitInbound)
	subscribedAt := time.Now().UTC()
	if err := s.queue(ctx, jsonFrame(subscribedFrame{
		Event:     "subscribed",
		Timestamp: subscribedAt.Format(time.RFC3339Nano),
	})); err != nil {
		// subscribed 帧入队失败(反压 / ctx done)→ 直接结束,不再尝试 Run
		return err
	}

	// Session 自身实现 PushSink(见 pushsink.go)
	sink := PushSink(s)

	// ⑥ 双向模式(OnMessage / OnBinaryMessage 至少一个非 nil):
	// 进入消息调度循环(Run 若提供则后台并行)
	if s.handlers.duplexEnabled() {
		return s.duplexLoop(ctx, cancel, req, sink)
	}

	// ⑥ 单向模式:Handlers.Run 同步 blocking 业务循环
	// ⑦ Run 返回值分发
	return s.dispatchRunError(ctx, s.handlers.Run(ctx, req, sink))
}

// dispatchRunError 把(单向 Run / 双向后台 Run)的返回值映射到连接级 close 行为:
// nil→close(1000) 握手 + 主动收敛;ErrSlowConsumer→429+close;
// ctx 取消(预期)静默;其它→500+close。
func (s *Session) dispatchRunError(ctx context.Context, runErr error) error {
	switch {
	case runErr == nil:
		// 正常结束:flush 完在途业务帧后发 close(1000) 握手并关闭连接——
		// 不能只 return nil:errgroup 仅在非 nil error 时取消 ctx,
		// 否则 readLoop/writeLoop 会靠 Ping/Pong 悬挂到 MaxSessionDuration。
		s.closeNormal(ctx)
		return nil
	case errors.Is(runErr, ErrSlowConsumer):
		s.options.emit(ctx, Event{Type: EventSlowConsumer, Reason: ReasonSlowConsumer, Err: runErr})
		s.closeWithError(ctx, CodeTooManyConn, ReasonSlowConsumer)
		return runErr
	case errors.Is(runErr, context.Canceled), errors.Is(runErr, context.DeadlineExceeded):
		// ctx 取消是预期路径(客户端断 / 30 分钟超时 / turn 被打断),不下发 error 帧
		return runErr
	default:
		// 未预期的业务错误统一按内部错误处理,客户端只收到稳定 reason。
		// 错误通过 Serve 返回值上抛给调用方,由调用方决定是否记录日志。
		s.closeWithError(ctx, CodeInternal, ReasonInternalError)
		return runErr
	}
}
