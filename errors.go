package wssession

import "errors"

// Sentinel errors — 桥接层和业务层共同识别的错误标记。
var (
	// ErrSlowConsumer outbox 入队 5s 超时,客户端消费速度跟不上推送速度。
	// 业务侧 Run 收到此 error 时应 return,wssession 收敛并下发 error(429) 帧 + close。
	ErrSlowConsumer = errors.New("wssession: slow consumer (outbox queue full)")

	// ErrFirstFrameTimeout Upgrade 后 Options.FirstFrameTimeout 内无任何 inbound 帧。
	// 由 processLoop 在 timer 触发时主动 close。
	ErrFirstFrameTimeout = errors.New("wssession: first frame timeout")

	// ErrInvalidFrame 帧不被接受:入站侧为帧类型不被当前 Handlers 接受
	// (未启用二进制帧、或首帧不是文本帧);出站侧为推送了零值 Frame。
	ErrInvalidFrame = errors.New("wssession: invalid frame")

	// ErrUnexpectedFrame 已订阅状态下收到的额外业务帧(协议约定:首帧后不应再发)。
	ErrUnexpectedFrame = errors.New("wssession: unexpected frame after subscribed")

	// ErrHandlersIncomplete Handlers 缺必填字段:ParseRequest 为 nil,
	// 或 Run / OnMessage / OnBinaryMessage 三者全为 nil。
	ErrHandlersIncomplete = errors.New("wssession: handlers.ParseRequest is required, plus at least one of Run / OnMessage / OnBinaryMessage")

	// ErrOutboundFrameTooLarge 出站 payload 序列化后超过 Options.MaxOutboundFrameBytes。
	// Push 返回该错误时不会向 outbox 入队该帧。
	ErrOutboundFrameTooLarge = errors.New("wssession: outbound frame too large")

	// ErrDeadlineNotExtendable 在未开启 Options.SessionDeadlineExtendable 的连接上
	// 调用了 Session.ExtendDeadline。会话截止时间不变。
	ErrDeadlineNotExtendable = errors.New("wssession: session deadline is not extendable, set Options.SessionDeadlineExtendable")
)

// 错误码常量 — 与 docs/wsmsg-flow.md §5 错误码映射表对齐。
const (
	CodeFirstFrameTimeout = 408
	// CodeConflict 会话冲突:连接被同一身份的新会话顶下线(Session.Kick 下发)。
	// 客户端收到 error(409) 应提示"已在其他设备登录"且**不**自动重连
	// (与 close 1001 的"应重连"语义相反);WS 层 close code 映射为 1008。
	CodeConflict         = 409
	CodeInvalidFrameType = 415
	CodeInvalidParam     = 422
	CodeTooManyConn      = 429
	CodeInternal         = 500
)

const maxErrorReasonLen = 256

var errTurnStuck = errors.New("wssession: turn stuck")

// ReasonFirstFrameTimeout 等是下发给客户端 error 帧的标准 reason 文案（对外契约）。
const (
	ReasonFirstFrameTimeout      = "first frame timeout"
	ReasonBinaryFrameUnsupported = "binary frame not supported"
	ReasonUnexpectedFrame        = "unexpected frame after subscribed"
	ReasonTooManyIPConn          = "too many concurrent connections from this ip"
	ReasonTooManyTokenConn       = "too many concurrent connections for this token"
	ReasonSlowConsumer           = "slow consumer"
	ReasonInternalError          = "internal error"
	// ReasonBinaryFirstFrame 首帧是二进制帧:首帧固定为 ParseRequest 的文本鉴权 /
	// 订阅帧,与"服务端不支持二进制"(ReasonBinaryFrameUnsupported)区分开,
	// 便于客户端分辨是"发早了"还是"服务端不支持"。
	ReasonBinaryFirstFrame = "first frame must be a text frame"
	// ReasonServerShuttingDown 服务端正在停机,不再受理新连接(HTTP 503,不 Upgrade)。
	ReasonServerShuttingDown = "server shutting down"
)

// errorFrame 是服务端下发给客户端的错误事件载体(对外 JSON schema 契约)。
//
// JSON 形态:{"event":"error","code":422,"reason":"...","timestamp":"..."}.
type errorFrame struct {
	Event     string `json:"event"`
	Code      int    `json:"code"`
	Reason    string `json:"reason"`
	Timestamp string `json:"timestamp"`
}

// subscribedFrame 是服务端在 ParseRequest + tokenCap 通过后立即下发的订阅确认帧。
//
// JSON 形态:{"event":"subscribed","timestamp":"..."}.
type subscribedFrame struct {
	Event     string `json:"event"`
	Timestamp string `json:"timestamp"`
}
