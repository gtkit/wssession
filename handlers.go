package wssession

import "context"

// Handlers 由业务侧注入,wssession 通过这些函数完成"协议无关 + 业务无关"。
//
// 设计原则:
//   - 函数式注入而非 interface,业务侧写匿名函数即可,不需要新建 type(详见 design.md D-2)
//   - ParseRequest 必填;Run / OnMessage / OnBinaryMessage 三者至少一个;OnConnect 可选
//   - ParseRequest 必须**快**(只做 JSON 解析 + 字段校验,不调 DB / 网络);
//     若需要重操作放到 Run 内,因为 Run 跑在独立的 processLoop 串行段不阻塞 readLoop
//
// 隔离警告:若把同一个 Handlers 值复用于多次 Serve 调用(典型:提升为包级变量挂在
// 路由上),其中的函数闭包捕获的任何可变状态都会被**该路由的所有连接(所有用户)共享**,
// 既是数据竞争也是用户间串台。连接级状态一律经 ParseRequest 返回的 req 传递
// (wssession 会原样带给 Run / 后续轮次),或在每次请求内现场构造 Handlers,
// 让闭包只捕获 per-request 局部量(README 示例即此模式)。
type Handlers struct {
	// OnConnect 可选,Upgrade 成功 + 进 lifecycle goroutine 之前调一次。
	//
	// 适用场景:连接级 setup(连接 ID 注册 / 准入审计日志 / 自定义心跳计数器);
	// 当前订单业务不使用此 hook。
	//
	// 返回 error 时 Session 立即 close,不下发任何业务帧。
	OnConnect func(ctx context.Context, sess *Session) error

	// ParseRequest 解析客户端首帧,返回:
	//   - key:用于 token 维度 connCap 计数的 key(空字符串 → 跳过 tokenCap,继续 Run)
	//   - req:业务请求对象,会原样传给 Run(无类型约束,业务自定义)
	//   - err:解析失败 → 桥接层下发 error 帧(code=422)+ close
	//
	// 必填。
	ParseRequest func(ctx context.Context, raw []byte) (key string, req any, err error)

	// Run 业务推送循环。
	//   - 内部通过 sink.Push(payload) 推帧(payload 由 wssession 用 JSON 序列化)
	//   - return nil → 自然结束:单向模式下 wssession flush 完在途帧后下发
	//     close(1000) 握手并主动关闭连接;双向模式下连接保持(对话由 OnMessage 继续)
	//   - return ErrSlowConsumer → 桥接层下发 error(429) + close
	//   - return 其他 err → 桥接层下发 error(500) + close
	//
	// 单向与双向模式的错误处置一致(均经 dispatchRunError 分发)。
	//
	// Run 是 blocking 调用,跑在 processLoop 内;不要在 Run 内 spawn goroutine 后立即 return,
	// 否则桥接层会以为业务已结束。如需异步处理,在 Run 内自己用 errgroup 编排再 return。
	//
	// **必须监听 ctx 并及时返回**:Go 无法强杀 goroutine,桥接层只能等 Run 自己退出。
	// 不响应 ctx 的 Run 会让 Serve 一直不返回(该 HTTP handler 与其 goroutine 随之滞留),
	// 单向与双向模式皆然;双向模式下 TurnCloseTimeout 只约束消息回调,不约束 Run。
	//
	// 单向模式(无消息回调)下必填;双向模式(OnMessage / OnBinaryMessage 非 nil)下可选——
	// 若提供,作为后台主动推送循环与消息回调并存。
	Run func(ctx context.Context, req any, sink PushSink) error

	// OnMessage 可选;非 nil 时启用**双向模式**。
	//
	// 双向模式下,首帧仍由 ParseRequest 处理(订阅/鉴权);此后客户端发送的每条业务帧
	// 都触发一次 OnMessage(turnCtx, raw, sink),不阻塞读循环。
	//
	// **调度语义由 Options.Dispatch 决定**(默认 DispatchInterrupt):
	//   - DispatchInterrupt:新消息 cancel 上一轮的 turnCtx(打断正在进行的生成),
	//     等其退出后才启动新一轮——任一时刻至多一个回调在运行;
	//   - DispatchSequential:上一轮完整结束后才取下一条,永不打断;
	//   - DispatchConcurrent:多轮并发运行(上限 MaxConcurrentMessages),不保证顺序。
	//
	// turnCtx 在以下情况被取消:被新消息打断(仅 DispatchInterrupt)/ 会话超时 / 客户端断开。
	// OnMessage 实现**必须监听 turnCtx 并及时返回**(如把它传给 LLM 流式调用),
	// 否则打断会阻塞后续消息的调度、连接关闭时会等待其退出。这与 ParseRequest
	// 的"必须快"同性质。
	//
	// 返回值处置:nil → 该轮正常结束,连接保持;ErrSlowConsumer → error(429)+close;
	// ctx.Canceled/DeadlineExceeded(被打断/超时)→ 静默;其它 err → error(500)+close。
	OnMessage func(ctx context.Context, raw []byte, sink PushSink) error

	// OnBinaryMessage 可选;非 nil 即**接受二进制入站帧**(也同时启用双向模式),
	// 与 OnMessage 启用双向模式是同一套惯例——不另设布尔开关。
	//
	// 订阅完成后收到的每个二进制帧触发一次 OnBinaryMessage,其调度(Options.Dispatch)、
	// 限速、打断与错误处置与 OnMessage 完全一致,并与 OnMessage 共用同一轮次槽位
	// (同一连接的文本与二进制回调不会同时运行,除 DispatchConcurrent 外)。
	//
	// 首帧仍必须是文本帧(ParseRequest 的鉴权 / 订阅帧):订阅前收到二进制帧下发
	// error(415, ReasonBinaryFirstFrame) 并收敛连接。本字段为 nil 时,任何二进制帧
	// 下发 error(415, ReasonBinaryFrameUnsupported) 并收敛连接。
	//
	// 回复二进制帧:sink.Push(ctx, wssession.NewBinaryFrame(data))。
	// 与 OnMessage 同样**必须监听 ctx 并及时返回**。
	OnBinaryMessage func(ctx context.Context, raw []byte, sink PushSink) error
}

// duplexEnabled 报告是否启用双向模式:文本或二进制消息回调至少一个非 nil。
func (h Handlers) duplexEnabled() bool {
	return h.OnMessage != nil || h.OnBinaryMessage != nil
}

// validate 检查 Handlers 关键字段非 nil。
//
// ParseRequest 必填;Run / OnMessage / OnBinaryMessage 三者至少提供一个
// (单向至少 Run,双向至少一个消息回调)。
func (h Handlers) validate() error {
	if h.ParseRequest == nil {
		return ErrHandlersIncomplete
	}
	if h.Run == nil && !h.duplexEnabled() {
		return ErrHandlersIncomplete
	}
	return nil
}
