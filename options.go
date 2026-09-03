// Package wssession — 通用 WebSocket 桥接层(协议无关 / 业务无关)。
//
// 文件分布:
//   - options.go      配置与默认值
//   - errors.go       sentinel error + error/subscribed 帧 schema
//   - handlers.go     业务注入函数式 hooks(OnConnect / ParseRequest / Run / OnMessage)
//   - pushsink.go     PushSink 接口 + Frame(预序列化帧):业务 → outbox 唯一入口
//   - session.go      Session struct + Serve(lifecycle 编排) + close 路径 + 只读信息面
//   - readloop.go     readLoop:WS → inbox(帧类型准入 + 客户端关闭上报)
//   - processloop.go  processLoop:inbox → ParseRequest → connCap → Run / 双向调度
//   - duplex.go       双向模式消息调度(打断 / 顺序 / 并发三模式 + 限速)
//   - ratelimit.go    入站消息令牌桶限速器
//   - writeloop.go    writeLoop:outbox → WS(含 Ping 心跳)
//   - outbound.go     outboundMessage + queue 反压
//   - connlimit.go    IP/key 维度连接 cap(分片 mutex 计数表,归零删除 key)
//   - origin.go       Origin 白名单
//
// 完整流程文档见 docs/wsmsg-flow.md。
package wssession

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// EventType 标识 wssession 在连接生命周期内上报的事件类别。
type EventType int

const (
	// EventPanic 某 loop goroutine 发生 panic(已被 recover 并转为 error)。
	EventPanic EventType = iota + 1
	// EventSlowConsumer 出站队列满 + QueueOfferTimeout 超时,客户端消费跟不上。
	EventSlowConsumer
	// EventCapRejected 连接被 IP 或 token 维度连接 cap 拒绝。
	EventCapRejected
	// EventAbnormalClose 连接以 1006(无正常 close 握手)异常断开。
	EventAbnormalClose
	// EventRateLimited 双向模式下入站消息超过速率限制,被丢弃。
	EventRateLimited
	// EventTurnInterrupted 双向模式下上一轮 OnMessage 因新消息到达被打断。
	EventTurnInterrupted
	// EventTurnStuck 双向模式下上一轮 OnMessage 被取消后未在超时内退出。
	EventTurnStuck
	// EventClientClose 客户端主动发来 close 帧,Event.Code / Reason 为其携带的
	// close code 与文案。1006(无 close 握手的异常断开)不走本事件,见 EventAbnormalClose。
	//
	// best-effort:服务端已先发起 close 握手时不上报(那只是客户端对我方 close 的
	// 回应);客户端关闭与服务端收敛并发时,本事件可能不上报。
	// Reason 是客户端可控文本(已按 256 字节截断),落日志前请自行处理控制字符。
	EventClientClose
)

// String 返回事件类型的可读名,便于日志。
func (t EventType) String() string {
	switch t {
	case EventPanic:
		return "panic"
	case EventSlowConsumer:
		return "slow_consumer"
	case EventCapRejected:
		return "cap_rejected"
	case EventAbnormalClose:
		return "abnormal_close"
	case EventRateLimited:
		return "rate_limited"
	case EventTurnInterrupted:
		return "turn_interrupted"
	case EventTurnStuck:
		return "turn_stuck"
	case EventClientClose:
		return "client_close"
	default:
		return "unknown"
	}
}

// DispatchMode 决定双向模式下入站业务消息的调度语义(仅在双向模式生效)。
type DispatchMode int

const (
	// DispatchInterrupt 打断式(零值,默认):新消息到达时 cancel 仍在运行的上一轮
	// 并等其退出后再开新一轮,上报 EventTurnInterrupted。
	// 适合"新问题作废旧回答"的场景(如 LLM 多轮对话)。
	DispatchInterrupt DispatchMode = iota

	// DispatchSequential 顺序式:上一轮完整执行结束后才取下一条消息,任何情况下都不
	// 打断正在运行的回调。适合每条消息都必须处理完的场景(信令 / 协作编辑 / 命令通道)。
	//
	// 容量约束:入站积压靠 inbox(InboundBufferSize,默认 4)反压 readLoop,而 readLoop
	// 阻塞期间不读 Pong,读超时由 PongWait(默认 70s)终结连接。因此"单轮耗时 ×
	// inbox 深度"应显著小于 PongWait,否则应调大 InboundBufferSize 或改用 DispatchConcurrent。
	DispatchSequential

	// DispatchConcurrent 并发式:每条消息各自一轮并发执行,同时在飞轮次数不超过
	// MaxConcurrentMessages(必填);达到上限时调度器等待空位,对入站形成反压。
	// **不保证处理顺序**,需要顺序请用 DispatchSequential。
	DispatchConcurrent
)

// Event 是 wssession 通过 Options.OnEvent 上报给调用方的生命周期事件。
//
// 字段语义:
//   - Type   事件类别
//   - Reason 人类可读原因文案
//   - Err    关联错误(可能为 nil)
//   - Key    cap 相关事件的 cap key(如 "ip:...:path" / "token:...:path"),其它事件为空
//   - Code   WebSocket close code,仅 EventClientClose 携带,其它事件为 0
type Event struct {
	Type   EventType
	Reason string
	Err    error
	Key    string
	Code   int
}

// Options 控制 wssession Session 的所有可调行为。
//
// 所有 Duration / 数值字段在 normalizeOptions() 内回退默认值;
// AllowedOrigins 空切片走 same-origin 校验,非空则严格白名单。
type Options struct {
	// AllowedOrigins WebSocket 握手期 Origin 白名单(空切片 = same-origin)。
	AllowedOrigins []string

	// ConfigureUpgrader 可选的握手期逃生阀:在桥接层设完自身默认值(缓冲区、
	// 共享写缓冲池、按 AllowedOrigins 构造的 CheckOrigin)之后、执行 Upgrade 之前
	// 调用一次,用于设置 gorilla 原生字段——Subprotocols(子协议协商)、
	// EnableCompression(permessage-deflate)、HandshakeTimeout、缓冲区大小等。
	//
	// **安全警告**:回调可以覆盖 CheckOrigin;一旦覆盖,AllowedOrigins 的
	// same-origin / 白名单保护即失效,Origin 校验责任完全转移给调用方。
	//
	// nil(默认)时握手行为与不配置本字段完全一致。
	ConfigureUpgrader func(u *websocket.Upgrader)

	// ResponseHeader 握手成功(HTTP 101)响应上附加的头,如 Set-Cookie。
	// nil(默认)时不附加额外响应头。
	//
	// 并发约定:同一个 Options 复用于多次 Serve 时,这个 map 会被多个连接**并发读取**
	// (握手期只读,不会被本包或 gorilla 修改)。因此它必须在首次 Serve 之前构造完成,
	// 之后不得再写入——否则是 map 的并发读写。需要按连接定制响应头时,
	// 在每次请求内现场构造 Options。
	//
	// 单独出字段而非并入 ConfigureUpgrader:响应头是 Upgrade 的参数,不是
	// Upgrader 的字段,回调覆盖不到。子协议协商结果由 gorilla 依据
	// Upgrader.Subprotocols 自动写入,不需要在这里手工设置。
	ResponseHeader http.Header

	// FirstFrameTimeout Upgrade 后无任何 inbound 帧的最大等待。
	// 超时下发 error(408) 帧 + close。默认 10s。
	FirstFrameTimeout time.Duration

	// MaxSessionDuration 单 Session 绝对存活上限(防 fd 长期占用)。默认 30 min。
	//
	// 默认是**固定**上限:会话 ctx 由 context.WithTimeout 构造,到期后 ctx.Err()
	// 为 context.DeadlineExceeded 且 ctx.Deadline() 可读。要中途延长请看
	// SessionDeadlineExtendable。
	MaxSessionDuration time.Duration

	// SessionDeadlineExtendable 开启后允许业务用 Session.ExtendDeadline 在会话中途
	// 延长存活上限(典型用途:凭证刷新——JWT 有效期远短于连接期望存活时间时,
	// 换 token 而不断连接,避免重连丢掉 ParseRequest 建立的连接级状态)。
	//
	// **语义差异(仅在开启时出现)**:会话 ctx 不再由固定 deadline 构造,因此
	//   - 到期时 ctx.Err() 为 context.Canceled 而非 context.DeadlineExceeded;
	//     用 context.Cause(ctx) 仍可拿到 context.DeadlineExceeded 以区分"会话
	//     到期"与"上游主动取消";
	//   - ctx.Deadline() 不再返回 deadline。需要 deadline 的下游调用请在业务回调内
	//     自行派生 context.WithTimeout。
	//
	// 零值 false 时会话 ctx 的构造与行为与不配置本字段完全一致;此时调用
	// ExtendDeadline 返回 ErrDeadlineNotExtendable。
	//
	// 注意:开启后连续续期可无限延长会话,MaxSessionDuration 的 fd 保护作用随之
	// 转移给调用方——续期策略(校验什么、最多续多久)由业务负责。
	SessionDeadlineExtendable bool

	// ReadLimit 单 inbound 帧最大字节数;超出 gorilla 返回 ErrReadLimit。默认 4096。
	ReadLimit int64

	// PingInterval 服务端 Ping 周期。默认 25s。
	PingInterval time.Duration

	// PongWait 无 Pong 后判定连接死亡的最大时长。默认 70s。
	PongWait time.Duration

	// WriteWait 单帧写超时。默认 10s。
	WriteWait time.Duration

	// OutboundBufferSize outbox channel 容量。默认 128。
	OutboundBufferSize int

	// MaxOutboundFrameBytes 单条业务出站文本帧序列化后的最大字节数。
	// <=0(默认)=不限制。超限时 Push 返回 ErrOutboundFrameTooLarge,且不入队该帧。
	MaxOutboundFrameBytes int

	// QueueOfferTimeout 业务 sink.Push 入队超时;超时返回 ErrSlowConsumer。默认 5s。
	QueueOfferTimeout time.Duration

	// InboundBufferSize inbox channel 容量;本场景首帧 1 条即够,默认 4 留余量。
	InboundBufferSize int

	// ConnCapEnabled 连接 cap 总开关(false 时两层 cap 透传)。
	ConnCapEnabled bool

	// ConnCapIPMax 单 client_ip + path 同时活跃连接数上限。
	// ConnCapEnabled=true 时必须 > 0。默认 50。
	ConnCapIPMax int

	// ConnCapKeyMax 单 token + path 同时活跃连接数上限(ParseRequest 返回的 key)。
	// ConnCapEnabled=true 时必须 > 0。默认 5。
	ConnCapKeyMax int

	// TrustedProxyCount 信任的反向代理跳数,决定客户端 IP 的取值来源。
	//
	//   - 0(默认):忽略 X-Forwarded-For,客户端 IP 取自 RemoteAddr。
	//     X-Forwarded-For 客户端可任意伪造,默认信任会导致 IP 维度 connCap
	//     被绕过,并放大连接计数表的 key 膨胀,故默认不信任。
	//   - n>0:从 X-Forwarded-For 列表**由右向左**数第 n 跳取客户端 IP
	//     (可信代理把上游地址追加在列表右侧);n 超过列表长度时回退到列表
	//     最左端,列表为空时回退 RemoteAddr。
	//
	// 部署在 Nginx / 网关等反向代理后时,应设为可信代理的跳数。
	TrustedProxyCount int

	// InboundRatePerSecond 双向模式下单连接入站业务消息的速率上限(条/秒)。
	// 0(默认)= 不限速。仅在 Handlers.OnMessage 非 nil 时生效。
	InboundRatePerSecond float64

	// InboundRateBurst 双向模式下入站消息的突发额度(令牌桶容量)。
	// <=0 时回退为 max(1, ceil(InboundRatePerSecond))。仅在限速启用时生效。
	InboundRateBurst int

	// TurnCloseTimeout 双向模式下打断旧 OnMessage 或连接收敛时等待旧 turn 退出的最长时间。
	// 超时上报 EventTurnStuck 并收敛连接。默认 5s。
	TurnCloseTimeout time.Duration

	// Dispatch 双向模式下入站业务消息的调度语义,零值 DispatchInterrupt 即既有
	// 打断式行为(见 DispatchMode)。单向模式(仅 Run)忽略本字段。
	Dispatch DispatchMode

	// MaxConcurrentMessages 仅 Dispatch 为 DispatchConcurrent 时生效:单连接同时
	// 在飞的消息回调数上限,达到上限时调度器等待空位。
	//
	// DispatchConcurrent 下必须 > 0——并发上限是容量决策,库不替调用方猜默认值
	// (Validate 会拒绝缺失配置)。其它调度模式下本字段被忽略。
	MaxConcurrentMessages int

	// OnEvent 可选的生命周期事件回调,用于接入调用方自己的日志 / metrics。
	//
	// 上报时机:panic / 慢消费者 / 连接 cap 拒绝 / 1006 异常断开 /
	// 入站限速 / turn 打断 / turn 卡住(见 EventType)。
	// nil 时桥接层跳过上报。事件的记录方式由调用方决定。
	//
	// 回调必须**快且非阻塞**(同步调用,会短暂参与连接收敛路径),且必须
	// **并发安全**:同一连接的 readLoop / processLoop / turn goroutine / ctx
	// watcher 都可能触发上报,多连接场景下并发度更高。回调内的 panic
	// 会被桥接层 recover,不影响连接生命周期。
	OnEvent func(ctx context.Context, ev Event)
}

// emit 安全触发 OnEvent:nil 跳过,并 recover 用户回调内的 panic
// (回调 panic 不应影响连接收敛,也不再触发 EventPanic 递归)。
func (o Options) emit(ctx context.Context, ev Event) {
	if o.OnEvent == nil {
		return
	}
	defer func() { _ = recover() }()
	o.OnEvent(ctx, ev)
}

// Validate 校验运行所需的关键参数。
//
// 仅在 Options.ConnCapEnabled=true 时校验两个 cap;调度模式按 fail-closed 校验
// (未定义模式、并发模式缺上限一律报错,不静默回退);其余字段空值由
// normalizeOptions 兜底默认。
func (o Options) Validate() error {
	if o.ConnCapEnabled {
		if o.ConnCapIPMax <= 0 {
			return errors.New("wssession: ConnCapIPMax must be > 0 when ConnCapEnabled")
		}
		if o.ConnCapKeyMax <= 0 {
			return errors.New("wssession: ConnCapKeyMax must be > 0 when ConnCapEnabled")
		}
	}
	switch o.Dispatch {
	case DispatchInterrupt, DispatchSequential:
	case DispatchConcurrent:
		if o.MaxConcurrentMessages <= 0 {
			return errors.New("wssession: MaxConcurrentMessages must be > 0 when Dispatch is DispatchConcurrent")
		}
	default:
		return fmt.Errorf("wssession: unknown Dispatch mode %d", o.Dispatch)
	}
	return nil
}

// normalizeOptions 为调用方未显式配置的字段填充生产可用的默认值。
func normalizeOptions(o Options) Options {
	if o.FirstFrameTimeout <= 0 {
		o.FirstFrameTimeout = 10 * time.Second
	}
	if o.MaxSessionDuration <= 0 {
		o.MaxSessionDuration = 30 * time.Minute
	}
	if o.ReadLimit <= 0 {
		o.ReadLimit = 4096
	}
	if o.PingInterval <= 0 {
		o.PingInterval = 25 * time.Second
	}
	if o.PongWait <= 0 {
		o.PongWait = 70 * time.Second
	}
	if o.WriteWait <= 0 {
		o.WriteWait = 10 * time.Second
	}
	if o.OutboundBufferSize <= 0 {
		o.OutboundBufferSize = 128
	}
	if o.QueueOfferTimeout <= 0 {
		o.QueueOfferTimeout = 5 * time.Second
	}
	if o.TurnCloseTimeout <= 0 {
		o.TurnCloseTimeout = 5 * time.Second
	}
	if o.InboundBufferSize <= 0 {
		o.InboundBufferSize = 4
	}
	if o.InboundRatePerSecond > 0 && o.InboundRateBurst <= 0 {
		// burst 至少 1,且不小于每秒速率(向上取整)
		burst := int(math.Ceil(o.InboundRatePerSecond))
		o.InboundRateBurst = max(burst, 1)
	}
	return o
}
