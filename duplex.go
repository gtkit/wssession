package wssession

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// turn 表示双向模式下一轮消息回调的执行句柄。
type turn struct {
	cancel context.CancelFunc
	done   chan struct{} // 回调 goroutine 退出时关闭
}

// inboundGate 把入站限速判定与"同一连续限速期只下发一帧 429 提示"的状态收在一起,
// 供三种调度模式共用。
type inboundGate struct {
	limiter *rateLimiter
	// notified 标记当前连续限速期内是否已下发过 429 提示帧,
	// 任一消息重新通过限速即复位——避免限速风暴下重复出帧。
	notified bool
}

// duplexLoop 是双向模式的消息调度循环。
//
// 职责:
//   - 首帧已被 ParseRequest 当订阅/鉴权帧消费;本循环处理其后的每条消息,
//     按帧类型派发到 OnMessage(文本)或 OnBinaryMessage(二进制);
//   - 调度语义由 Options.Dispatch 决定(打断 / 顺序 / 并发,见 DispatchMode);
//   - 入站限速:超额消息丢弃 + 上报 EventRateLimited,同一连续限速期只下发
//     一帧 error(429) 提示,不关连接;
//   - 收敛:ctx 取消时取消在飞轮次并等待退出;失约轮次超时后上报 EventTurnStuck
//     并收敛连接。
//
// 双向模式下 Run 可选:若提供,在后台 goroutine 并行运行(用于主动推送),
// 其错误处置与单向模式一致(dispatchRunError)。
func (s *Session) duplexLoop(ctx context.Context, cancel context.CancelFunc, req any, sink PushSink) error {
	gate := &inboundGate{limiter: newRateLimiter(s.options.InboundRatePerSecond, s.options.InboundRateBurst)}

	var wg sync.WaitGroup
	s.startBackgroundRun(ctx, cancel, &wg, req, sink)

	switch s.options.Dispatch {
	case DispatchSequential:
		return s.dispatchSequential(ctx, cancel, &wg, gate, sink)
	case DispatchConcurrent:
		return s.dispatchConcurrent(ctx, cancel, &wg, gate, sink)
	default:
		// DispatchInterrupt(零值);未定义值已被 Options.Validate 拒绝。
		return s.dispatchInterrupt(ctx, cancel, &wg, gate, sink)
	}
}

// startBackgroundRun 在双向模式下把可选的 Handlers.Run 作为后台主动推送循环启动。
//
// 错误处置复用 dispatchRunError(事件 + error 帧 + close),与单向模式一致;
// 返回 nil 表示推送循环自然结束,连接保持(对话由消息回调继续)。
func (s *Session) startBackgroundRun(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, req any, sink PushSink) {
	if s.handlers.Run == nil {
		return
	}
	wg.Go(func() {
		defer func() {
			if p := recover(); p != nil {
				panicErr := fmt.Errorf("wssession: panic in background Run: %v", p)
				s.options.emit(ctx, Event{Type: EventPanic, Reason: "panic in background Run", Err: panicErr})
				s.closeWithError(ctx, CodeInternal, ReasonInternalError, panicErr)
				cancel()
			}
		}()
		if err := s.handlers.Run(ctx, req, sink); err != nil &&
			!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			_ = s.dispatchRunError(ctx, err)
			cancel() // 后台 Run 异常 → 收敛连接
		}
	})
}

// dispatchInterrupt 是打断式调度(DispatchInterrupt,默认):
// 新消息到达时若上一轮仍在运行则 cancel 它并等其退出,再开启新一轮——
// 同一连接任一时刻严格至多一个回调在运行,被打断的旧轮不会在新轮启动后继续推过期帧。
func (s *Session) dispatchInterrupt(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, gate *inboundGate, sink PushSink) error {
	var active *turn
	// stuckHandled 标记失约轮次已在打断路径处置过(已上报 EventTurnStuck 并收敛连接)。
	// 不加这个标记,defer 会对同一个失约轮次再等一个完整 TurnCloseTimeout 并重复上报。
	stuckHandled := false

	// 收敛:打断活跃轮次 + 等所有轮次(含后台 Run)退出。
	defer func() {
		if stuckHandled {
			return
		}
		if active != nil {
			active.cancel()
			if !s.waitTurnDone(ctx, active, "turn stuck during connection close") {
				cancel()
				return
			}
		}
		wg.Wait()
	}()

	for {
		frame, ok, err := s.recvFrame(ctx, gate)
		if err != nil {
			return err
		}
		if !ok {
			continue // 被入站限速丢弃
		}

		// 打断仍在运行的上一轮(已自然结束的不算打断),并等其退出:
		// 守约的回调(契约要求监听 turnCtx)毫秒级返回;失约的会经
		// inbox→readLoop 反压,最终由 PongWait 终结连接。
		if active != nil {
			select {
			case <-active.done:
			default:
				active.cancel()
				s.options.emit(ctx, Event{Type: EventTurnInterrupted, Reason: "interrupted by new message"})
				if !s.waitTurnDone(ctx, active, "turn stuck after interrupt") {
					stuckHandled = true
					// cause 为 nil:失约已由 EventTurnStuck 上报,不再作为 Serve 错误。
					s.closeWithError(ctx, CodeInternal, ReasonInternalError, nil)
					cancel()
					return errTurnStuck
				}
			}
		}
		active = s.startTurn(ctx, cancel, wg, frame, sink)
	}
}

// dispatchSequential 是顺序式调度(DispatchSequential):
// 上一轮完整执行结束后才取下一条消息,任何情况下都不打断正在运行的回调。
//
// 回调在本 goroutine 内联执行——顺序模式无需打断,也就不需要每轮一个 goroutine;
// 回调的 ctx 即连接 ctx,连接收敛时自然传播。
func (s *Session) dispatchSequential(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, gate *inboundGate, sink PushSink) error {
	defer wg.Wait() // 等后台 Run 退出

	for {
		frame, ok, err := s.recvFrame(ctx, gate)
		if err != nil {
			return err
		}
		if !ok {
			continue // 被入站限速丢弃
		}
		// 业务错误 / panic 会在 runTurn 内 cancel 连接,下一轮 recvFrame 随即返回 ctx.Err。
		s.runTurn(ctx, cancel, frame, sink)
	}
}

// dispatchConcurrent 是并发式调度(DispatchConcurrent):
// 每条消息各自一轮并发执行,同时在飞轮次数不超过 MaxConcurrentMessages;
// 达到上限时调度器等待空位,对入站形成反压而不是无界起 goroutine。
//
// 所有在飞轮次共享一个派生 ctx:收敛时一次 cancel 全部,再等它们退出。
func (s *Session) dispatchConcurrent(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, gate *inboundGate, sink PushSink) error {
	turnsCtx, turnsCancel := context.WithCancel(ctx)
	var turns sync.WaitGroup
	slots := make(chan struct{}, s.options.MaxConcurrentMessages)

	// 收敛顺序必须是"先取消全部在飞轮次,再等它们退出":反序会白等到超时。
	defer func() {
		turnsCancel()
		if !s.waitTurnsDone(ctx, &turns) {
			cancel()
			return
		}
		wg.Wait()
	}()

	for {
		frame, ok, err := s.recvFrame(ctx, gate)
		if err != nil {
			return err
		}
		if !ok {
			continue // 被入站限速丢弃
		}

		// 达到并发上限则等空位(或连接收敛)。
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		turns.Go(func() {
			defer func() { <-slots }()
			s.runTurn(turnsCtx, cancel, frame, sink)
		})
	}
}

// recvFrame 等待下一条可处理的入站业务帧。
//
// 返回 ok=false 表示该帧被入站限速丢弃(调用方应继续取下一条);
// 返回 err 非 nil 表示连接已收敛,调度循环应退出。
func (s *Session) recvFrame(ctx context.Context, gate *inboundGate) (inboundFrame, bool, error) {
	select {
	case <-ctx.Done():
		return inboundFrame{}, false, ctx.Err()
	case frame := <-s.inbox:
		if !gate.limiter.allow() {
			s.options.emit(ctx, Event{Type: EventRateLimited, Reason: "inbound rate limited"})
			if !gate.notified {
				gate.notified = true
				s.offerRateLimitedFrame()
			}
			return inboundFrame{}, false, nil
		}
		gate.notified = false
		return frame, true, nil
	}
}

// startTurn 为一条消息派生 turnCtx 并起 goroutine 执行一轮回调(打断式调度用)。
func (s *Session) startTurn(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, frame inboundFrame, sink PushSink) *turn {
	turnCtx, turnCancel := context.WithCancel(ctx)
	t := &turn{cancel: turnCancel, done: make(chan struct{})}

	wg.Go(func() {
		defer close(t.done)
		defer turnCancel() // 释放 turnCtx,避免 context 泄漏
		s.runTurn(turnCtx, cancel, frame, sink)
	})

	return t
}

// runTurn 执行一轮消息回调,并把返回值与 panic 映射到连接级处置。
//
// 三种调度模式共用:打断式 / 并发式在各自的 goroutine 内调用,顺序式内联调用——
// 处置后果一致(业务错误与 panic 都收敛整条连接,并作为根因记入 termErr 由 Serve 返回)。
func (s *Session) runTurn(turnCtx context.Context, cancel context.CancelFunc, frame inboundFrame, sink PushSink) {
	defer func() {
		if p := recover(); p != nil {
			panicErr := fmt.Errorf("wssession: panic in message handler: %v", p)
			s.options.emit(turnCtx, Event{Type: EventPanic, Reason: "panic in message handler", Err: panicErr})
			s.closeWithError(turnCtx, CodeInternal, ReasonInternalError, panicErr)
			cancel()
		}
	}()

	err := s.invokeHandler(turnCtx, frame, sink)
	switch {
	case turnCtx.Err() != nil:
		// 被新消息打断 / 会话结束(turnCtx 已取消):视为预期,不关连接——
		// 无论业务是否如约把 ctx 取消传播为返回值,都不误杀整条连接。
	case err == nil:
		// 该轮正常结束,连接保持
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// 业务自身 ctx 取消:预期,静默
	case errors.Is(err, ErrSlowConsumer):
		s.options.emit(turnCtx, Event{Type: EventSlowConsumer, Reason: ReasonSlowConsumer, Err: err})
		s.closeWithError(turnCtx, CodeTooManyConn, ReasonSlowConsumer, err)
		cancel() // 慢消费者 → 收敛整连接
	default:
		s.closeWithError(turnCtx, CodeInternal, ReasonInternalError, err)
		cancel() // 业务错误 → 收敛整连接
	}
}

// invokeHandler 按帧类型派发到对应的业务回调。
//
// 帧类型准入已在 readLoop 完成(见 admitInbound),这里的 handler 必然非 nil;
// 兜底返回内部错误而非 panic,以防未来准入规则与派发规则脱节。
func (s *Session) invokeHandler(ctx context.Context, frame inboundFrame, sink PushSink) error {
	if frame.messageType == websocket.BinaryMessage {
		if s.handlers.OnBinaryMessage == nil {
			return errors.New("wssession: binary frame dispatched without OnBinaryMessage handler")
		}
		return s.handlers.OnBinaryMessage(ctx, frame.raw, sink)
	}
	if s.handlers.OnMessage == nil {
		return errors.New("wssession: text frame dispatched without OnMessage handler")
	}
	return s.handlers.OnMessage(ctx, frame.raw, sink)
}

// waitTurnDone 等单个轮次退出,超过 TurnCloseTimeout 上报 EventTurnStuck 并返回 false。
func (s *Session) waitTurnDone(ctx context.Context, active *turn, reason string) bool {
	timer := time.NewTimer(s.options.TurnCloseTimeout)
	defer timer.Stop()

	select {
	case <-active.done:
		return true
	case <-timer.C:
		s.emitTurnStuck(ctx, reason)
		return false
	}
}

// waitTurnsDone 等所有在飞轮次退出(并发式调度用),超过 TurnCloseTimeout 上报
// EventTurnStuck 并返回 false。
//
// Go 无法强杀 goroutine:失约轮次与本函数的等待 goroutine 会一直悬挂到业务自己返回,
// 调用方据此收敛连接,不再等待。返回 false 时调用方也会跳过后台 Run 的 wg.Wait(),
// 因此那条 goroutine 同样滞留到它自己退出。
func (s *Session) waitTurnsDone(ctx context.Context, turns *sync.WaitGroup) bool {
	done := make(chan struct{})
	go func() {
		turns.Wait()
		close(done)
	}()

	timer := time.NewTimer(s.options.TurnCloseTimeout)
	defer timer.Stop()

	select {
	case <-done:
		return true
	case <-timer.C:
		s.emitTurnStuck(ctx, "turns stuck during connection close")
		return false
	}
}

// emitTurnStuck 上报轮次失约事件,附带连接 ctx 的错误(若已取消)。
func (s *Session) emitTurnStuck(ctx context.Context, reason string) {
	ev := Event{Type: EventTurnStuck, Reason: reason}
	if err := ctx.Err(); err != nil {
		ev.Err = err
	}
	s.options.emit(ctx, ev)
}

// offerRateLimitedFrame 向客户端**非阻塞**下发一帧 error(429) 限速提示,不关闭连接。
//
// 用非阻塞 send:限速恰是高频刷消息时触发,若用阻塞的 queue,满了会卡住调度循环
// 最多 QueueOfferTimeout——那等于让攻击者拖慢正常处理。outbox 满则丢弃这帧提示。
// 调用频率由 inboundGate.notified 控制:同一连续限速期只发一帧。
func (s *Session) offerRateLimitedFrame() {
	frame := jsonFrame(errorFrame{
		Event:     "error",
		Code:      CodeTooManyConn,
		Reason:    "rate limited",
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	})
	select {
	case s.outbox <- frame:
	default: // outbox 满 → 丢弃限速提示,绝不阻塞调度循环
	}
}
