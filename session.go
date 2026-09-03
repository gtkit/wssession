package wssession

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"
)

// Session 代表一个被 wssession 接管的 WebSocket 连接。
//
// 字段对外只读暴露(通过方法),内部状态由 readLoop / processLoop / writeLoop 协作维护。
type Session struct {
	wsConn   *websocket.Conn
	options  Options
	handlers Handlers

	// httpReq 是发起本连接的 HTTP 请求,构造后只读,经 Request() 暴露。
	httpReq *http.Request

	// clientIP 在 Serve 入口按 TrustedProxyCount 解析一次(与 IP cap key 同口径),
	// 构造后只读,经 ClientIP() 暴露——避免每次查询重复切分 X-Forwarded-For。
	clientIP string

	// req 保存 Handlers.ParseRequest 返回的业务请求对象,供 Value() 跨 goroutine 读取。
	// 用 atomic.Pointer 而非 atomic.Value:req 是业务自定义的 any,可能为 nil,
	// 而 atomic.Value 不接受 nil 接口且要求具体类型一致。
	req atomic.Pointer[any]

	// closed 在 Close 完成后置 true,供 IsClosed() 查询。
	closed atomic.Bool

	// serverClosing 标记服务端已主动发起 close 握手。此后读到的客户端 close 帧
	// 只是对我方 close 的回应(携带的是服务端自己下发的 code),不代表客户端的
	// 关闭意图,不再上报 EventClientClose——否则同一次关闭会被双重计数。
	serverClosing atomic.Bool

	// deadlineMu 守护下面两个字段的读写(ExtendDeadline 可来自任意 goroutine)。
	deadlineMu sync.Mutex
	// deadlineTimer 仅在 Options.SessionDeadlineExtendable 为 true 时非 nil,
	// 触发即把会话 ctx 以 DeadlineExceeded 为 cause 取消(cancel 由定时器闭包持有);
	// ExtendDeadline 重设它。
	deadlineTimer *time.Timer
	// sessCtx 是会话 ctx,与 deadlineTimer 同生命周期,供 ExtendDeadline 判定会话
	// 是否已结束——不能只看 closed:ctx 取消与 Close() 之间存在真实窗口
	// (Close 由 ctx watcher 在 ctx.Done 之后才调用,且其内部 close 帧写出最多等 1s)。
	sessCtx context.Context

	// inbox 是 readLoop → processLoop 的有界 channel(默认容量 4)。
	inbox chan inboundFrame

	// outbox 是业务 → writeLoop 的有界 channel(默认容量 128),
	// 通过 PushSink.Push 间接写入,不暴露给业务直接 send。
	outbox chan outboundMessage

	// path 用于 connCap key 拼接,在 Serve 入口从 r.URL.Path 提取。
	path string

	// tokenCapKey 在 processLoop 内 tryAcquire 成功后写入,由 Serve 在连接
	// 真正结束(group.Wait 之后)释放——cap 占用与连接生命周期对齐,
	// 不随 processLoop 提前退出而提前释放。
	// 并发安全:processLoop 写、Serve 在 group.Wait 返回后读,Wait 构成 happens-before。
	tokenCapKey string

	// firstFrameClaimed 裁决"首帧到达"与"首帧超时"的竞态:二者各自 CAS,
	// 只有抢到的一方能对连接采取动作,避免首帧已收到却被误判超时关闭。
	firstFrameClaimed atomic.Bool

	// errFrameOnce 保证并发错误关闭时只下发首个 error 帧。
	errFrameOnce sync.Once

	// termErr 记录首个服务端主动错误关闭的根因,在 errFrameOnce 的 Once 内写入,
	// 与下发的 error 帧对应同一次关闭。Serve 在 errgroup 收敛后优先返回它:
	// 主动关闭底层连接会同时唤醒 readLoop 返回 net.ErrClosed,哪个 loop 先把 error
	// 交给 errgroup 取决于调度,不能拿 errgroup 的首个 error 当"连接为何结束"的依据。
	// 用 atomic.Pointer:写入方可能是 Kick 调用方、首帧定时器等 errgroup 之外的 goroutine。
	termErr atomic.Pointer[error]

	closeOnce sync.Once
}

const errorFrameQueueOfferTimeout = 500 * time.Millisecond

// closeFrameWriteTimeout 是 ctx 收敛路径 best-effort 发送 close 控制帧的写超时;
// 客户端假死时最多延迟这么久再裸关,不阻碍连接关闭。
const closeFrameWriteTimeout = time.Second

// handshakeTimeout 是 Upgrade 阶段写出 101 响应的超时兜底(gorilla Upgrader.HandshakeTimeout),
// 调用方可经 Options.ConfigureUpgrader 覆盖。
const handshakeTimeout = 10 * time.Second

// wsWriteBufferPool 供所有连接共享写缓冲(gorilla 仅在写帧瞬间占用),
// 避免每连接常驻 4KB 写缓冲。
var wsWriteBufferPool = &sync.Pool{}

// Serve 完成一个 WebSocket 连接的完整托管流程。
//
// 流程:
//
//	⓪ 停机准入(parent ctx 已取消 → HTTP 503,不 Upgrade)
//	① IP 维度 connCap(Upgrade 之前;失败 HTTP 429,不 Upgrade)
//	② HTTP Upgrade(Origin 校验在 Upgrader.CheckOrigin 内完成)
//	③ context.WithTimeout(parent, MaxSessionDuration)
//	④ OnConnect hook(可选)
//	⑤ 启动 readLoop / processLoop / writeLoop(errgroup)
//	⑥ 等所有 goroutine 收敛 + 释放资源
//
// 返回值:
//   - nil          : 预期关闭——Run 自然 return / 客户端 close / 会话超时或上游取消 / Kick
//   - non-nil err  : 服务端主动错误关闭的根因(首帧超时 / ParseRequest 错误 / token cap 满 /
//     业务回调返回的非预期错误或 panic / 帧准入拒绝),以及停机期拒连 / IP cap 满 /
//     Upgrade 失败 / OnConnect err / loop panic。主动错误关闭的根因记录在 Session.termErr,
//     由 Serve 确定性返回,不依赖 errgroup 收到各 loop error 的先后。
func Serve(parent context.Context, w http.ResponseWriter, r *http.Request, options Options, handlers Handlers) error {
	if err := handlers.validate(); err != nil {
		return err
	}
	if err := options.Validate(); err != nil {
		return err
	}

	// ⓪ 停机准入:parent 已取消(典型:进程级 shutdown ctx)时不再受理新连接。
	// 提前拒比"Upgrade 成功后立刻收敛"干净——客户端拿到 503 而不是闪断。
	// 配置校验保持在前:配置错误与停机是两类问题,不该被停机期掩盖。
	if err := parent.Err(); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":503,"msg":"` + ReasonServerShuttingDown + `","data":null}`))
		return fmt.Errorf("wssession: %s: %w", ReasonServerShuttingDown, err)
	}

	opts := normalizeOptions(options)

	path := r.URL.Path
	clientAddr := clientIP(r, opts.TrustedProxyCount)

	// ① IP 维度 connCap(Upgrade 之前)
	var ipCapKey string
	if opts.ConnCapEnabled {
		ipCapKey = "ip:" + clientAddr + ":" + path
		if _, ok := tryAcquire(ipCapKey, opts.ConnCapIPMax); !ok {
			opts.emit(parent, Event{Type: EventCapRejected, Reason: ReasonTooManyIPConn, Key: ipCapKey})
			// 普通 HTTP 响应,不进入 WS 协议层
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"code":429,"msg":"` + ReasonTooManyIPConn + `","data":null}`))
			return fmt.Errorf("%w: ip dimension", ErrConnCapExceeded)
		}
		defer release(ipCapKey)
	}

	// ② HTTP Upgrade(Origin 校验在 CheckOrigin 内)
	upgrader := websocket.Upgrader{
		HandshakeTimeout: handshakeTimeout,
		ReadBufferSize:   4096,
		WriteBufferSize:  4096,
		WriteBufferPool:  wsWriteBufferPool,
		CheckOrigin:      newOriginChecker(opts.AllowedOrigins),
	}
	// 逃生阀:调用方可设 Subprotocols / EnableCompression / HandshakeTimeout 等
	// gorilla 原生字段(也可覆盖 CheckOrigin,此时 Origin 责任转移给调用方)。
	if opts.ConfigureUpgrader != nil {
		opts.ConfigureUpgrader(&upgrader)
	}
	wsConn, err := upgrader.Upgrade(w, r, opts.ResponseHeader)
	if err != nil {
		// gorilla 已写 HTTP 4xx,包装后上抛
		return fmt.Errorf("wssession: upgrade: %w", err)
	}

	sess := &Session{
		wsConn:   wsConn,
		options:  opts,
		handlers: handlers,
		httpReq:  r,
		clientIP: clientAddr,
		inbox:    make(chan inboundFrame, opts.InboundBufferSize),
		outbox:   make(chan outboundMessage, opts.OutboundBufferSize),
		path:     path,
	}
	defer func() { _ = sess.Close() }()

	// ③ 会话存活上限(MaxSessionDuration),按是否允许续期选择实现
	ctx, cancel := sess.newSessionContext(parent)
	defer cancel()

	// token cap 在 processLoop 内 acquire,但占用必须覆盖整条连接的生命周期,
	// 故释放挂在 Serve 退出(group.Wait 之后),与上面 ipCapKey 的时序一致。
	defer func() {
		if sess.tokenCapKey != "" {
			release(sess.tokenCapKey)
		}
	}()

	// 当 ctx 取消时(MaxSessionDuration 到期 / 上游取消)收敛连接,把 readLoop
	// 从 ReadMessage 阻塞中踹醒。close 握手兜底(1001)在 Close 内统一完成,
	// 避免多个收敛者(本 watcher / closeNormal / closeWithError)竞争出帧。
	go func() {
		<-ctx.Done()
		_ = sess.Close()
	}()

	// ④ OnConnect hook(可选)
	if handlers.OnConnect != nil {
		if err := handlers.OnConnect(ctx, sess); err != nil {
			return err
		}
	}

	// ⑤ 启动 3 个 goroutine(errgroup)
	group, runCtx := errgroup.WithContext(ctx)
	groupCancel := func() {
		// errgroup 自身收敛 ctx,这里提供给各 loop 的 panic recovery 调用
		cancel()
	}
	group.Go(func() error { return sess.readLoop(runCtx, groupCancel) })
	group.Go(func() error { return sess.processLoop(runCtx, groupCancel) })
	group.Go(func() error { return sess.writeLoop(runCtx, groupCancel) })

	// ⑥ 等所有 goroutine 收敛
	waitErr := group.Wait()

	// 服务端主动错误关闭:根因已在 closeWithError 内记录,优先返回它——
	// 主动关闭会同时唤醒 readLoop 返回 net.ErrClosed,errgroup 的首个 error 不可靠。
	if p := sess.termErr.Load(); p != nil {
		return *p
	}
	if waitErr == nil {
		return nil
	}

	// 1006 异常断开:上报事件,但不作为错误返回(避免网络抖动变成误报)
	if isAbnormalClose(waitErr) {
		opts.emit(ctx, Event{Type: EventAbnormalClose, Reason: "abnormal closure", Err: waitErr})
		return nil
	}

	// 过滤其余预期 close 错误
	if !IsExpectedClose(waitErr) {
		return waitErr
	}
	return nil
}

// Close 幂等关闭底层 WS 连接,关闭前 best-effort 补发 close 握手帧。
//
// 在 Serve defer、ctx.Done 监听 goroutine、closeNormal / closeWithError 内
// 均会调用,sync.Once 保证只关一次。
//
// 持有 *Session 的调用方直接调用时注意语义:未发过 close 帧的连接会以 1001
// GoingAway 完成握手,按客户端重连决策表这表示"请退避重连"。要拒绝或踢掉一个
// 连接应使用 Kick(error 409 + close 1008,客户端不重连),而不是 Close。
//
// 兜底握手:若此前已通过 outbox 写出过 close 帧(Run 正常结束的 1000 /
// 错误关闭的 1008/1011),gorilla 对重复 close 帧返回 ErrCloseSent、不上写,
// 客户端只看到先到的那帧;若尚未发过(会话超时 / 上游取消 / flush 失败的
// 服务端单方面终止),则以 1001 GoingAway 完成握手,提示客户端走重连分支,
// 避免裸关 TCP 让客户端只看到 1006。
func (s *Session) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		s.serverClosing.Store(true)
		if s.wsConn != nil {
			_ = s.wsConn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseGoingAway, ""),
				time.Now().Add(closeFrameWriteTimeout))
			closeErr = s.wsConn.Close()
		}
		s.closed.Store(true)
	})
	return closeErr
}

// newSessionContext 构造承载 MaxSessionDuration 的会话 ctx。
//
// 默认(SessionDeadlineExtendable 为 false)用 context.WithTimeout——固定 deadline,
// 到期 ctx.Err() 为 DeadlineExceeded 且 ctx.Deadline() 可读,与历史行为逐字节一致。
//
// 开启续期时改用"可重设定时器 + WithCancelCause":context 的 deadline 不可变,
// 要支持中途延长只能自己拿定时器管。代价是 ctx.Err() 变成 Canceled、ctx.Deadline()
// 失效,故用 cause 携带 DeadlineExceeded 让业务仍能区分"会话到期"与"上游取消"
// (见 Options.SessionDeadlineExtendable 的说明)。
func (s *Session) newSessionContext(parent context.Context) (context.Context, context.CancelFunc) {
	if !s.options.SessionDeadlineExtendable {
		return context.WithTimeout(parent, s.options.MaxSessionDuration)
	}

	ctx, cancelCause := context.WithCancelCause(parent)
	timer := time.AfterFunc(s.options.MaxSessionDuration, func() {
		cancelCause(context.DeadlineExceeded)
	})

	s.deadlineMu.Lock()
	s.deadlineTimer = timer
	s.sessCtx = ctx
	s.deadlineMu.Unlock()

	return ctx, func() {
		timer.Stop()
		cancelCause(context.Canceled)
	}
}

// ExtendDeadline 把会话截止时间**重设**为 now+d(不是在原截止时间上追加),
// 用于凭证刷新等"连接不断、只换凭证"的场景。
//
// 需要 Options.SessionDeadlineExtendable 为 true;未开启时返回
// ErrDeadlineNotExtendable 且不改变截止时间。d <= 0 或连接已收敛同样返回错误。
//
// 并发安全,可从任意 goroutine 调用(典型是某一轮 OnMessage 内校验完新 token 后)。
// 续期次数与总时长不受限制——MaxSessionDuration 的 fd 保护作用随之转移给调用方。
func (s *Session) ExtendDeadline(d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("wssession: ExtendDeadline duration must be > 0, got %v", d)
	}

	s.deadlineMu.Lock()
	defer s.deadlineMu.Unlock()

	if s.deadlineTimer == nil {
		return ErrDeadlineNotExtendable
	}
	// 已结束的会话不再复活。必须同时看 ctx 与 closed:
	//   - ctx 已取消但 Close 尚未跑完(停机 / 会话到期的必经窗口)→ 续期毫无意义,
	//     且 Reset 会把 cancel 已 Stop 的定时器重新武装,让 ctx 链在定时器堆里滞留 d;
	//   - 错误路径下 Close 先于会话 ctx 取消(closeWithError)→ 靠 closed 兜住。
	if err := s.sessCtx.Err(); err != nil {
		return fmt.Errorf("wssession: ExtendDeadline on an ended session: %w", err)
	}
	if s.closed.Load() {
		return errors.New("wssession: ExtendDeadline on a closed session")
	}
	s.deadlineTimer.Reset(d)
	return nil
}

// Value 返回 Handlers.ParseRequest 为本连接返回的业务请求对象(req)。
//
// 首帧解析成功之前返回 nil。并发安全:写入只发生一次(首帧解析成功后),之后可被
// 任意 goroutine 读取——双向模式的每轮 OnMessage / OnBinaryMessage 由此拿到连接级
// 会话对象,不必依赖闭包捕获可变状态(见 Handlers 的隔离警告)。
func (s *Session) Value() any {
	if p := s.req.Load(); p != nil {
		return *p
	}
	return nil
}

// Request 返回发起本连接的 HTTP 请求,用于读取 Header / URL / RemoteAddr 等握手期元数据。
//
// 连接已被 hijack,请求体不可再读;返回值只应用于读取,不要修改。
func (s *Session) Request() *http.Request {
	return s.httpReq
}

// ClientIP 返回按 Options.TrustedProxyCount 规则解析出的客户端 IP,
// 与 IP 维度连接 cap key 使用的取值完全一致。
func (s *Session) ClientIP() string {
	return s.clientIP
}

// IsClosed 报告底层连接是否已关闭。
//
// 供持有连接句柄的一方(如 sessionhub 注册表)在推送前过滤已收敛的连接。
// 返回 false 不保证紧接着的推送一定成功——连接可能在两次调用之间收敛,
// 推送失败仍需按返回的 error 处置。
func (s *Session) IsClosed() bool {
	return s.closed.Load()
}

// closeWithError 依次下发一帧 error JSON 与一帧 close 控制帧,**同步**等
// writeLoop flush 完后再 close 底层连接,完成 WebSocket 关闭握手。
//
// 行为约定:
//   - 入队 error 帧,再入队带 done 信号的 close 控制帧(channel FIFO 保证先后)
//   - **同步**等 done 关闭(writeLoop 已写出两帧)或 1s 兜底超时
//   - 主动 close 底层 conn,踹醒 readLoop 立刻退出
//
// 同步等待是关键:若立即 close,writeLoop 会因 wsConn 关闭而 WriteMessage 失败,
// error 帧丢失,客户端只看到 abnormal closure 而无错误码/原因。
//
// cause 是本次关闭的根因:非 nil 时记入 termErr 并由 Serve 返回;nil 表示该关闭
// 不作为 Serve 的错误上抛(Kick、已由 EventTurnStuck 上报的失约轮次)。
//
// 调用方应在调用本方法后 return,让所在的 loop 退出 → errgroup 收敛 → defer Close。
func (s *Session) closeWithError(ctx context.Context, code int, reason string, cause error) {
	// 幂等:并发触发只下发首个 error 帧,避免客户端收到矛盾的错误码;
	// termErr 在同一个 Once 内决定,与下发的 error 帧始终对应同一次关闭。
	s.errFrameOnce.Do(func() {
		if cause != nil {
			s.termErr.Store(&cause)
		}
		frame := errorFrame{
			Event:     "error",
			Code:      code,
			Reason:    truncateErrorReason(reason),
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		}
		if err := s.queueWithTimeout(ctx, jsonFrame(frame), errorFrameQueueOfferTimeout); err != nil {
			// 入队失败(ctx done / slow consumer)→ 不等 flush,落到下面统一 Close
			return
		}
		// close 握手帧排在 error 帧之后写出,done 挂在最后一帧上,单次等待覆盖两帧。
		s.flushCloseFrame(ctx, wsCloseCode(code), errorFrameQueueOfferTimeout)
	})
	_ = s.Close()
}

// closeNormal 发送 close(1000) 握手帧并等 flush 后关闭连接。
//
// 用于单向模式 Run 正常返回后的主动收敛:channel FIFO 保证 Run 已 Push 的
// 尾部业务帧先于 close 帧写出。conn 关闭后 readLoop 解阻塞返回预期 close
// 错误,errgroup 随之收敛,连接不再悬挂等客户端断开或 MaxSessionDuration。
func (s *Session) closeNormal(ctx context.Context) {
	s.flushCloseFrame(ctx, websocket.CloseNormalClosure, s.options.QueueOfferTimeout)
	_ = s.Close()
}

// flushCloseFrame 入队一帧 close 控制帧并同步等 writeLoop 写出;
// ctx 取消(连接已在收敛,close 握手由 Serve 的 ctx watcher best-effort 兜底)
// 或入队失败时直接放弃,由调用方裸关。1s 兜底防 writeLoop 恰在入队后退出。
func (s *Session) flushCloseFrame(ctx context.Context, wsCode int, offerTimeout time.Duration) {
	// 标记"服务端已发起 close 握手":客户端随后回应的 close 帧不再计为客户端主动关闭。
	s.serverClosing.Store(true)

	done := make(chan struct{})
	msg := outboundMessage{
		messageType: websocket.CloseMessage,
		data:        websocket.FormatCloseMessage(wsCode, ""),
		done:        done,
	}
	if err := s.queueWithTimeout(ctx, msg, offerTimeout); err != nil {
		return
	}
	select {
	case <-done:
	case <-ctx.Done():
	case <-time.After(time.Second):
	}
}

// wsCloseCode 把下发给客户端的业务错误码映射为 WebSocket close code:
// 500 → 1011(internal error),其余(408/415/422/429)→ 1008(policy violation)。
func wsCloseCode(code int) int {
	if code == CodeInternal {
		return websocket.CloseInternalServerErr
	}
	return websocket.ClosePolicyViolation
}

// truncateErrorReason 把 reason 截到 maxErrorReasonLen 字节以内,并回退到 rune 边界,
// 不把多字节字符切成非法 UTF-8。
func truncateErrorReason(reason string) string {
	if len(reason) <= maxErrorReasonLen {
		return reason
	}
	cut := maxErrorReasonLen
	for cut > 0 && !utf8.RuneStart(reason[cut]) {
		cut--
	}
	return reason[:cut]
}

// IsExpectedClose 用于识别浏览器主动断开 / 正常 EOF / errgroup 内部 cancel 触发的 close,
// 这类"错误"不应作为服务端异常上报。
func IsExpectedClose(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	if errors.Is(err, errTurnStuck) {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if websocket.IsCloseError(err,
		websocket.CloseNormalClosure,
		websocket.CloseGoingAway,
		websocket.CloseNoStatusReceived,
	) {
		return true
	}
	return false
}

// isAbnormalClose 识别 1006(CloseAbnormalClosure):无正常 close 握手的断开。
//
// 1006 不再归为预期 close(见 IsExpectedClose):它可能是客户端网络抖动,
// 也可能掩盖服务端写超时等真实问题,故单独识别并通过 OnEvent 上报,
// 但不作为 Serve 错误返回(避免把常见网络抖动变成调用方的错误误报)。
func isAbnormalClose(err error) bool {
	return websocket.IsCloseError(err, websocket.CloseAbnormalClosure)
}

// clientIP 提取用于 IP 维度 connCap 的客户端 IP。
//
// trustedProxyCount<=0 时只用传输层 RemoteAddr,忽略客户端可伪造的
// X-Forwarded-For;>0 时从 X-Forwarded-For 列表由右向左取第 trustedProxyCount
// 跳(可信代理把上游地址追加在右侧)。列表条目数少于 trustedProxyCount 说明请求
// 没有经过完整的可信链,列表里没有任何一跳能确认是可信代理写入的,一律退回
// RemoteAddr(fail-closed),不取客户端可控的最左端。
func clientIP(r *http.Request, trustedProxyCount int) string {
	remote := remoteHost(r)
	if trustedProxyCount <= 0 {
		return remote
	}

	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		return remote
	}

	parts := strings.Split(xff, ",")
	if len(parts) < trustedProxyCount {
		return remote
	}
	if ip := strings.TrimSpace(parts[len(parts)-trustedProxyCount]); ip != "" {
		return ip
	}
	return remote
}

// remoteHost 返回 RemoteAddr 的 host 部分(无端口);解析失败时原样返回。
func remoteHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
