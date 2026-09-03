# wssession

`github.com/gtkit/wssession` 提供与业务无关的 **WebSocket** 实时传输底座：

| 包 | import 路径 | 用途 |
|---|---|---|
| wssession | `github.com/gtkit/wssession` | 生产级 WS 会话生命周期：心跳、反压、连接 cap、Origin 白名单、首帧超时、panic 恢复；支持文本/二进制双向消息与三种入站调度模式 |
| sessionhub | `github.com/gtkit/wssession/sessionhub` | 可选：按 userID 管理同一用户的多个并发连接（多设备 / 多标签页），定向推送与踢下线 |

**业务无关**：领域语义全部由调用方经回调 / 接口注入；**日志栈同样由调用方选择**，
错误通过返回值上抛，由调用方决定如何记录。

## 安装

```bash
go get github.com/gtkit/wssession
```

要求 Go 1.26+。

---

## 适合什么项目

这是一个**通用、与业务无关的 WebSocket 长连接底座**，适合用 Go + gin 写后端、需要"服务端持续向客户端推状态"或"单连接内多轮双向对话"的场景：

- **AI / LLM 多轮对话**：逐 token 流式下发，且**用户发新消息能打断上一轮生成**
- **订单 / 支付状态流**：下单后前端订阅，服务端轮询到"已支付 / 已发货"实时推送（本包的原始场景）
- **登录 / 在线状态**：单点登录顶号（`Session.Kick`）、多端连接管理（`sessionhub`）
- **任务 / 作业进度**：长任务（导出、转码、训练）的进度条与阶段事件推送
- **需要生产级连接治理**：要心跳保活、慢消费者反压、单 IP/token 并发上限、Origin 白名单、会话时长封顶、panic 不崩进程的 WebSocket 长连接

---

## wssession

通用 WebSocket 桥接层。业务通过 `Handlers{ParseRequest, Run, OnConnect, OnMessage, OnBinaryMessage}` 函数式注入，
通过 `PushSink` 推帧；`Options` 控制心跳、超时、缓冲、连接 cap、Origin 白名单。

### 引用

```go
import "github.com/gtkit/wssession"
```

### 完整示例：首帧订阅 + 循环推送

```go
package main

import (
	"context"
	"errors"
	"time"

	"github.com/gin-gonic/gin"
	gtkitjson "github.com/gtkit/json/v2"
	"github.com/gtkit/wssession"
)

func main() {
	r := gin.New()
	r.GET("/orders/query/wsmsg", handleWSMsg)
	_ = r.Run(":8080")
}

// 客户端连接后发的首帧（文本帧）：{"token":"abc123"}
type subscribeReq struct {
	Token string `json:"token"`
}

// handleWSMsg 只负责接线：把 Options 与 Handlers 交给 Serve，再处理返回值。
func handleWSMsg(c *gin.Context) {
	err := wssession.Serve(c.Request.Context(), c.Writer, c.Request, wsOptions(), wsHandlers())

	// Serve 已过滤客户端正常断开等预期 close（见 wssession.IsExpectedClose）；
	// 返回 non-nil 即真异常，日志由调用方按自己的方式记录。
	if err != nil {
		// 例如：logger.Warn("wsmsg serve failed", zap.Error(err))
		_ = err
	}
}

// wsOptions 集中配置：心跳 / 超时 / 连接 cap / Origin / 事件回调。
func wsOptions() wssession.Options {
	return wssession.Options{
		AllowedOrigins:         []string{"https://example.com"}, // 空切片 = same-origin
		MaxSessionDuration:     30 * time.Minute,
		PingInterval:           25 * time.Second,
		FirstFrameTimeout:      10 * time.Second,
		ConnCapEnabled:         true,
		ConnCapIPMax:           50,      // 单 IP+path 并发上限
		ConnCapKeyMax:          5,       // 单 key+path 并发上限（key 来自 ParseRequest）
		TrustedProxyCount:      1,       // 部署在 1 层可信反代后；0（默认）则忽略 X-Forwarded-For，IP 取自 RemoteAddr
		MaxOutboundFrameBytes:  1 << 20, // 单条业务出站帧最大 1 MiB；0 表示不限
		OnEvent:                onWSEvent,
	}
}

// wsHandlers 注入业务逻辑。Handlers 是函数式注入，可直接传具名函数，无需匿名闭包。
func wsHandlers() wssession.Handlers {
	return wssession.Handlers{
		ParseRequest: parseSubscribe,
		Run:          runPush,
		// OnConnect 可选：Upgrade 成功后、进 Run 前调一次（连接级 setup / 审计）。
		// OnConnect: onConnect,
	}
}

// parseSubscribe 解析首帧，返回 (限流 key, 业务请求对象, err)。
// 必须快：只做解析 + 字段校验，不查 DB / 不发网络。
func parseSubscribe(_ context.Context, raw []byte) (key string, req any, err error) {
	var r subscribeReq
	if err := gtkitjson.Unmarshal(raw, &r); err != nil {
		return "", nil, err // → 下发 error(422) 帧并 close
	}
	if r.Token == "" {
		return "", nil, errors.New("token required")
	}
	// 返回的 key 用于 token 维度连接 cap；返回的 req 原样传给 Run
	return r.Token, r, nil
}

// runPush 业务推送循环，blocking 调用。通过 sink.Push 推帧；return 即结束连接。
func runPush(ctx context.Context, req any, sink wssession.PushSink) error {
	r := req.(subscribeReq)
	poll := time.NewTicker(3 * time.Second)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err() // 客户端断开 / 30min 超时（预期 close）
		case <-poll.C:
			done, payload := pollOrder(r.Token)
			if err := sink.Push(ctx, payload); err != nil {
				return err // ErrSlowConsumer / ErrOutboundFrameTooLarge 等
			}
			if done {
				return nil // 正常结束 → normal closure
			}
		}
	}
}

// onWSEvent 接入自己的日志 / metrics。
func onWSEvent(_ context.Context, ev wssession.Event) {
	// 例：logger.Warn("ws event", zap.Stringer("type", ev.Type), zap.Error(ev.Err))
	_ = ev
}

func pollOrder(token string) (done bool, payload any) {
	return true, gin.H{"code": 200, "status": "paid"}
}
```

### 首帧鉴权

身份鉴权由业务注入。**HTTP 凭据**（`Authorization` header / cookie）在调 `Serve` 之前的 gin 中间件里验；**首帧里携带的业务 token** 按下面的分工落地：

- `ParseRequest` 必须**快**（在关键路径上）：只做格式校验 + 提取 token（顺便当 token 维度 connCap 的 key），**不查 DB / 不验签**。
- 需要查库 / 调鉴权服务的**重验证放 `Run` 开头**：失败时先推一帧明确的错误码，再结束连接。

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	gtkitjson "github.com/gtkit/json/v2"
	"github.com/gtkit/wssession"
)

func main() {
	r := gin.New()
	r.GET("/orders/stream/authed", handleAuthedWS)
	_ = r.Run(":8080")
}

// 首帧（文本帧）：{"token":"<jwt / session token>"}
type authFrame struct {
	Token string `json:"token"`
}

// authedReq 鉴权通过后传给推送循环。
type authedReq struct {
	token  string
	userID string
}

func handleAuthedWS(c *gin.Context) {
	_ = wssession.Serve(
		c.Request.Context(), c.Writer, c.Request,
		wssession.Options{
			FirstFrameTimeout: 10 * time.Second, // 连上不发鉴权帧 → 10s 后 408 + close
			ConnCapEnabled:    true,
			ConnCapIPMax:      50,
			ConnCapKeyMax:     5, // 以 token 为 key：限制单用户并发连接数
		},
		wssession.Handlers{
			// ParseRequest 必须快：只解析 + 提取 token，不查 DB / 不验签。
			ParseRequest: func(_ context.Context, raw []byte) (key string, req any, err error) {
				var f authFrame
				if err := gtkitjson.Unmarshal(raw, &f); err != nil {
					return "", nil, fmt.Errorf("bad auth frame: %w", err) // → error(422) + close
				}
				if f.Token == "" {
					return "", nil, errors.New("token required")
				}
				return f.Token, authedReq{token: f.Token}, nil // token 同时作为 cap key
			},
			// Run 跑在独立 goroutine：可查 DB / 调鉴权服务做重验证。
			Run: func(ctx context.Context, req any, sink wssession.PushSink) error {
				r := req.(authedReq)

				// —— 鉴权落点：重验证放这里 ——
				userID, err := verifyToken(ctx, r.token)
				if err != nil {
					// 先推一帧明确的 401，再正常结束（见下方说明）
					_ = sink.Push(ctx, gin.H{"event": "error", "code": 401, "reason": "unauthorized"})
					return nil
				}
				r.userID = userID

				// —— 鉴权通过，开始推送 ——
				poll := time.NewTicker(3 * time.Second)
				defer poll.Stop()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-poll.C:
						done, payload := loadUserUpdates(r.userID)
						if err := sink.Push(ctx, payload); err != nil {
							return err // ErrSlowConsumer / ErrOutboundFrameTooLarge 等
						}
						if done {
							return nil
						}
					}
				}
			},
		},
	)
}

// verifyToken 鉴权占位：真实实现验 JWT 签名 / 查 session 存储 / 调鉴权服务。
func verifyToken(_ context.Context, token string) (userID string, err error) {
	if token == "valid-token" {
		return "user-42", nil
	}
	return "", errors.New("invalid or expired token")
}

func loadUserUpdates(userID string) (done bool, payload any) {
	return true, gin.H{"user": userID, "status": "ok"}
}
```

> **为什么鉴权失败要先 `sink.Push` 再 `return nil`**：框架默认把 `Run` 返回的非 sentinel error 统一映射为 `error(500, "internal error")`，客户端拿不到"鉴权失败"的语义。先主动推一帧 `{"code":401,...}` 再 `return nil`（正常结束），客户端就能收到精确的失败原因。若服务端也想记录这次失败，在 `verifyToken` 出错处自行打日志 / metrics 即可。
>
> **配套保护**：`FirstFrameTimeout`（默认 10s）兜底"连上不发鉴权帧"的连接；`ConnCapKeyMax` 以 token 为 key 限制单用户并发连接数；token 走首帧 body 而非 URL query，不会泄漏进 access log。
>
> **握手前鉴权**：若用 cookie / `Authorization` 等 HTTP 凭据，应在调 `Serve` 之前的中间件里验，鉴权失败直接返回 401、根本不 Upgrade；把结果塞进 `c.Request.Context()`，`ParseRequest` / `Run` 都能取到。

### 双向多轮对话（LLM 流式）

默认 wssession 是"订阅后只收不发"的单向模型。要在**单个连接里多轮双向对话**（用户反复发消息、AI 流式回复、可被新消息打断），提供 `Handlers.OnMessage` 即进入**双向模式**：首帧仍由 `ParseRequest` 处理（会话初始化 / 鉴权），其后每条客户端消息触发一轮 `OnMessage`，在独立 goroutine 运行；**新消息到达会 cancel 上一轮的 `turnCtx`（打断正在进行的生成），并等上一轮 goroutine 退出后才启动新一轮**——同一连接任一时刻严格至多一个 `OnMessage` 在运行，被打断的旧轮不会在新轮启动后继续推过期帧。

> 双向模式下 `OnMessage` **必须监听 `turnCtx` 并及时返回**（把它传给 LLM 流式调用），否则打断会阻塞后续消息的调度、连接关闭时会等待其退出——与 `ParseRequest`"必须快"同性质。

入站限速（`InboundRatePerSecond`）触发时，被丢弃的每条消息都会上报 `EventRateLimited` 事件，但**同一连续限速期内只向客户端下发一帧 `error(429)` 提示**（限速恢复后再次超限会重新提示一帧）。双向模式下后台 `Run` 的错误处置与单向模式一致：`ErrSlowConsumer` → `error(429)` + close，其它错误 → `error(500)` + close。

若旧 `OnMessage` 被打断后没有在 `TurnCloseTimeout`（默认 5s）内退出，`wssession` 会上报 `EventTurnStuck` 并收敛连接，避免该连接继续处理新消息；这不能强杀业务 goroutine，所以 `OnMessage` 仍必须监听 `ctx`。

服务端：

```go
package main

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gtkit/wssession"
)

func main() {
	r := gin.New()
	r.GET("/chat", handleChat)
	_ = r.Run(":8080")
}

func handleChat(c *gin.Context) {
	_ = wssession.Serve(c.Request.Context(), c.Writer, c.Request,
		wssession.Options{
			FirstFrameTimeout:    10 * time.Second,
			ReadLimit:            64 << 10, // 默认 4096 字节对长 prompt 偏小；超限会以 close 1009 断连
			InboundRatePerSecond: 2,        // 单连接每秒最多 2 条用户消息（防刷）
			InboundRateBurst:     3,
		},
		wssession.Handlers{
			// 首帧：会话初始化 / 鉴权（不是对话消息）
			ParseRequest: func(_ context.Context, raw []byte) (key string, req any, err error) {
				return "session-1", nil, nil // 占位：解析鉴权 / 会话 ID
			},
			// 每条用户消息触发一轮；新消息会打断上一轮（turnCtx 被 cancel）
			OnMessage: func(ctx context.Context, raw []byte, sink wssession.PushSink) error {
				prompt := string(raw)
				for token := range llmStream(ctx, prompt) { // 把 ctx 传给 LLM，支持打断
					if err := sink.Push(ctx, gin.H{"event": "token", "text": token}); err != nil {
						return err // ErrSlowConsumer / ctx 取消
					}
				}
				return sink.Push(ctx, gin.H{"event": "done"})
			},
		},
	)
}

// llmStream 占位：真实实现调 LLM 流式 API，并在 ctx 取消时停止生成。
func llmStream(ctx context.Context, prompt string) <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)
		for _, tok := range []string{"Hello", ", ", "world"} {
			select {
			case <-ctx.Done():
				return // 被新消息打断 / 连接断开
			case ch <- tok:
			}
		}
	}()
	return ch
}
```

浏览器客户端（多轮收发）：

```js
const ws = new WebSocket("wss://example.com/chat");
ws.onopen = () => ws.send(JSON.stringify({ token: "abc" })); // 首帧：会话初始化

let current = "";
ws.onmessage = (e) => {
  const msg = JSON.parse(e.data);
  switch (msg.event) {
    case "subscribed": sendPrompt("你好"); break; // 订阅确认后开始第一轮
    case "token":      current += msg.text; break; // 累积流式 token
    case "done":       console.log("本轮完成:", current); current = ""; break;
    case "error":      console.error(msg.code, msg.reason); break; // 含 429 限速
  }
};

// 每条用户消息触发一轮；途中再发会打断上一轮生成
function sendPrompt(text) { ws.send(text); }
```

> **与单向模式的关系**：`OnMessage` 为 nil 时行为完全不变（订阅后再发帧仍被拒）。双向模式下 `Run` 可选——若同时提供，作为后台主动推送循环与 `OnMessage` 并存。超过 `InboundRatePerSecond` 的消息会被丢弃并下发 `error(429)`（不关连接），同时通过 `OnEvent` 上报 `EventRateLimited`；打断会上报 `EventTurnInterrupted`。

### 入站调度模式（打断 / 顺序 / 并发）

上面的"新消息打断上一轮"是默认语义，适合"新问题作废旧回答"的对话场景。但信令、协作编辑、命令通道这类场景里**每条消息都必须处理完**——打断等于丢消息。`Options.Dispatch` 选择调度语义，零值保持既有行为：

| `Dispatch` | 语义 | 适用 |
|---|---|---|
| `DispatchInterrupt`（零值，默认） | 新消息 cancel 仍在运行的上一轮，等其退出后再开新一轮，上报 `EventTurnInterrupted` | LLM 多轮对话、搜索建议 |
| `DispatchSequential` | 上一轮**完整执行结束**后才取下一条，永不打断 | 信令、协作编辑、命令通道 |
| `DispatchConcurrent` | 每条消息各自一轮并发执行，上限 `MaxConcurrentMessages`，**不保证顺序** | 相互独立的短任务 |

```go
// 顺序处理：每条消息都完整执行，不会被后来的消息打断
_ = wssession.Serve(ctx, w, r,
	wssession.Options{Dispatch: wssession.DispatchSequential},
	wssession.Handlers{ParseRequest: parseReq, OnMessage: handleSignal},
)

// 并发处理：最多 8 轮同时在飞，达到上限时调度器等待空位（对入站形成反压）
_ = wssession.Serve(ctx, w, r,
	wssession.Options{
		Dispatch:              wssession.DispatchConcurrent,
		MaxConcurrentMessages: 8, // 并发模式必填，缺失时 Serve 返回配置错误
	},
	wssession.Handlers{ParseRequest: parseReq, OnMessage: handleTask},
)
```

三种模式共享同一套限速、错误处置（`ErrSlowConsumer` → `error(429)` + close；其它错误 → `error(500)` + close）、panic 兜底与收敛等待（`TurnCloseTimeout` 到期上报 `EventTurnStuck` 并收敛连接）。

> **顺序模式的容量约束**：入站积压靠 inbox（`InboundBufferSize`，默认 4）反压读循环，而读循环阻塞期间不处理 Pong，读超时由 `PongWait`（默认 70s）终结连接。因此**单轮耗时 × inbox 深度应显著小于 `PongWait`**；否则调大 `InboundBufferSize`，或改用 `DispatchConcurrent`。
>
> **`MaxConcurrentMessages` 必填**是有意的 fail-closed：并发上限是容量决策，库不替调用方猜默认值。未定义的 `Dispatch` 值同样被 `Serve` 拒绝，不静默回退。

### 二进制帧（上行 / 下行）

`Handlers.OnBinaryMessage` 非 nil 即接受二进制入站帧——与 `OnMessage` 启用双向模式同一套惯例：提供哪个回调，就接受哪类帧。

```go
_ = wssession.Serve(ctx, w, r, wssession.Options{},
	wssession.Handlers{
		ParseRequest: parseReq, // 首帧仍是文本鉴权帧
		// 上行：客户端的每个二进制帧触发一轮（如实时语音分片）
		OnBinaryMessage: func(ctx context.Context, raw []byte, sink wssession.PushSink) error {
			// 下行：NewBinaryFrame 经既有 Push 写出二进制帧
			return sink.Push(ctx, wssession.NewBinaryFrame(transcode(raw)))
		},
	},
)
```

**首帧固定为文本帧**（`ParseRequest` 的鉴权 / 订阅帧）。首帧位置收到二进制帧时下发 `error(415)`，reason 为 `first frame must be a text frame`——与下表的 `binary frame not supported` 分开，客户端据此可分辨是"发早了"还是"该连接只收文本帧"。

订阅完成后，接受哪类帧由提供的回调决定：

| 提供的回调 | 订阅后接受 | 其它帧类型的应答 |
|---|---|---|
| 仅 `OnMessage` | 文本帧 | 二进制帧 → `error(415)`，reason `binary frame not supported` |
| 仅 `OnBinaryMessage` | 二进制帧 | 文本帧 → `error(422)`，reason `unexpected frame after subscribed` |
| 两者都提供 | 文本帧与二进制帧，按类型分别派发 | — |

- `OnMessage` 与 `OnBinaryMessage` 任一非 nil 即启用双向模式。
- 二进制帧的调度、限速、打断、错误处置与文本帧完全一致，两者共用同一轮次槽位。
- `ReadLimit` 对二进制帧同样生效。

### 帧协议（对外 JSON schema）

| 时机 | 帧 |
|---|---|
| ParseRequest + 连接 cap 通过后 | `{"event":"subscribed","timestamp":"..."}` |
| 业务推送（`sink.Push` 的 payload） | 由业务 payload 决定（原样 JSON 序列化） |
| 各类错误 / 超时 | `{"event":"error","code":<码>,"reason":"...","timestamp":"..."}` |

错误码：`408` 首帧超时、`409` 被顶下线（`Session.Kick`，客户端**不应**自动重连）、
`415` 帧类型不被接受（二进制帧未启用 / 首帧不是文本帧）、
`422` 解析失败或协议违规、`429` 连接超限/慢消费/入站限速、`500` 内部错误
（常量见 `errors.go`：`CodeFirstFrameTimeout` / `CodeConflict` / `CodeInvalidParam` / `CodeTooManyConn` 等）。

**关闭语义（WebSocket close 握手）**：服务端主动关闭一律先完成 close 握手再断开——
`Run` 正常结束（返回 nil）→ flush 完在途帧后发 close `1000`；错误关闭 → 先发上表的 `error` JSON 帧，
再发 close 帧（`408/409/415/422/429` → `1008`，`500` → `1011`）；会话超时 / 上游取消（服务端单方面终止，
客户端应重连）→ best-effort 发 close `1001`。客户端 `onclose` 收到 `1006` 即代表真实网络异常（非服务端主动关闭）。

> **唯一例外：`ReadLimit` 超限**。入站帧超过 `Options.ReadLimit` 时，底层 `gorilla/websocket` 会直接写出
> close `1009`（`CloseMessageTooBig`）后终止读取，**不会**有本库统一的 `{"event":"error",...}` 帧。
> 这是所有拒绝路径里唯一"只有 close code、没有 error 帧"的一条，客户端需单独处理。
> close code 语义本身是标准且明确的（消息过大），客户端据此**不应**原样重发同一帧。

### 客户端重连决策表

服务端会发出的每个 close code 对应固定的客户端策略。照表实现即可，不需要从上面的正文里推断：

| close code | 触发原因 | 客户端应自动重连？ | 应提示用户？ |
|---|---|---|---|
| `1000` | `Run` 正常结束，业务流已完成 | 否（本次订阅已正常结束，按业务决定是否重新订阅） | 否 |
| `1001` | 会话超时（`MaxSessionDuration`）/ 进程停机 / 上游取消 | **是**（退避重连，通常会连到新实例） | 否 |
| `1008` + `error(409)` | 被同身份新会话顶下线（`Session.Kick`） | **否**（重连会和新端互踢） | 是（"已在其他设备登录"） |
| `1008` + `error(408)` | 首帧超时（Upgrade 后迟迟不发订阅帧） | 是（但需修正客户端：连上后应立即发首帧） | 否 |
| `1008` + `error(415/422)` | 帧类型不被接受 / 解析失败 / 协议违规 | **否**（重连必然再次失败，属客户端 bug） | 否（应上报到前端监控） |
| `1008` + `error(429)` | 连接数超限 / 慢消费者 | 是，但**必须退避**（立即重连会继续撞上限） | 视产品而定 |
| `1009` | 入站帧超过 `ReadLimit`（无 `error` 帧，见上） | **否**，除非先减小帧体积 | 否 |
| `1011` + `error(500)` | 服务端内部错误 | 是（退避重连） | 否 |
| `1006` | 无 close 握手的异常断开（真实网络问题） | **是**（退避重连） | 否 |

> 另有一类不关连接的提示帧：双向模式入站限速时下发 `error(429)` 但**不**关闭连接，客户端只需降速，不要重连。

### 会话续期（凭证刷新 / 长会话）

`MaxSessionDuration`（默认 30 分钟）是会话绝对存活上限，默认**固定不可变**。当凭证有效期短于连接期望存活时间时（典型：JWT 15 分钟过期，但一次 AI 对话要持续一小时），靠客户端断线重连会丢掉 `ParseRequest` 建立的全部连接级状态。开启 `SessionDeadlineExtendable` 后可用 `Session.ExtendDeadline` 续期——**连接不断，只换凭证**：

```go
var sess atomic.Pointer[wssession.Session]

_ = wssession.Serve(ctx, w, r,
	wssession.Options{
		MaxSessionDuration:        15 * time.Minute, // 与 token 有效期对齐
		SessionDeadlineExtendable: true,
	},
	wssession.Handlers{
		OnConnect: func(_ context.Context, s *wssession.Session) error {
			sess.Store(s)
			return nil
		},
		ParseRequest: parseFirstFrame,
		OnMessage: func(ctx context.Context, raw []byte, sink wssession.PushSink) error {
			// 业务自己识别客户端的刷新帧（帧格式由你定，本库不介入）
			if token, ok := parseRefreshFrame(raw); ok {
				if err := verifyToken(token); err != nil {
					return err // 校验失败 → error(500) + close，让客户端重新登录
				}
				// 校验通过：这条连接再活 15 分钟
				if err := sess.Load().ExtendDeadline(15 * time.Minute); err != nil {
					return err
				}
				return sink.Push(ctx, map[string]any{"event": "refreshed"})
			}
			return handleChat(ctx, raw, sink)
		},
	},
)
```

- `ExtendDeadline(d)` 把截止时间**重设为 `now+d`**，与调用前剩余多少时间无关。
- 未开启 `SessionDeadlineExtendable` 时返回 `ErrDeadlineNotExtendable`（fail-closed），截止时间不变；`d <= 0` 或连接已收敛同样返回错误。
- 并发安全，可从任意 goroutine 调用。

> **开启后的语义差异**（仅在开启时出现）：会话 ctx 不再由固定 deadline 构造，因此到期时 `ctx.Err()` 是 `context.Canceled` 而非 `context.DeadlineExceeded`——用 `context.Cause(ctx)` 仍可拿到 `context.DeadlineExceeded`，以区分"会话到期"与"上游主动取消"；`ctx.Deadline()` 不再返回 deadline，需要 deadline 的下游调用请在回调内自行 `context.WithTimeout` 派生。默认（不开启）时这两点与既有行为完全一致。
>
> **续期不设总时长上限**：连续续期可无限延长会话，`MaxSessionDuration` 的 fd 保护作用随之转移给调用方——续多久、校验什么由你的刷新策略决定。

### 连接句柄与只读信息面

`OnConnect` 注入的 `*Session` 既是推送句柄（`Push` / `TryPush` / `Kick`），也提供四个只读查询：

| 方法 | 返回 | 用途 |
|---|---|---|
| `Value() any` | `ParseRequest` 返回的 `req` | 双向模式各轮回调访问连接级会话对象 |
| `Request() *http.Request` | 发起连接的 HTTP 请求 | 读握手期 Header / URL 元数据 |
| `ClientIP() string` | 按 `TrustedProxyCount` 解析的客户端 IP | 审计日志（与 IP cap key 同口径） |
| `IsClosed() bool` | 连接是否已关闭 | 注册表推送前过滤已收敛连接 |

> `Close()` 也是导出方法：幂等关闭连接，未发过 close 帧时以 **1001 GoingAway** 完成握手——按上文重连决策表，客户端收到 1001 会**退避重连**。要拒绝或踢掉一个连接用 `Kick`（409 + close 1008，客户端不重连）；`Close()` 只适合"这条连接该换个实例重连"的场景。

`Value()` 解决的是"消息回调拿不到会话对象"：`OnMessage` / `OnBinaryMessage` 的签名只有 `(ctx, raw, sink)`，而「关键约束」又要求闭包不得捕获可变状态。持有 `*Session` 即可跨轮次取到 `req`：

```go
var sess atomic.Pointer[wssession.Session]

_ = wssession.Serve(ctx, w, r, opts, wssession.Handlers{
	OnConnect: func(_ context.Context, s *wssession.Session) error {
		sess.Store(s)
		return nil
	},
	ParseRequest: func(_ context.Context, raw []byte) (string, any, error) {
		user := parseUser(raw)
		return user.ID, user, nil // 作为 req 返回
	},
	OnMessage: func(ctx context.Context, raw []byte, sink wssession.PushSink) error {
		user, _ := sess.Load().Value().(*User) // 每轮都能取到，无需闭包捕获可变状态
		return sink.Push(ctx, reply(user, raw))
	},
})
```

> `Value()` 在首帧解析成功前返回 nil；`Request()` 的请求体已被 hijack 不可读，只用于读元数据；`IsClosed()` 返回 false 不保证紧接着的推送一定成功（连接可能在两次调用之间收敛），推送失败仍按返回的 error 处置。

### 客户端 IP 与可信代理

IP 维度连接 cap 使用的客户端 IP 默认取自传输层 `RemoteAddr`，**忽略客户端可伪造的 `X-Forwarded-For`**。
部署在反向代理（Nginx / 网关）后时，把 `Options.TrustedProxyCount` 设为可信代理跳数，
`wssession` 会从 `X-Forwarded-For` 列表**由右向左**取第 N 跳作为客户端 IP；列表条目数少于 N（请求没有经过完整的可信链）时**退回 `RemoteAddr`**，不取客户端可控的最左端。未配置（0）时，
所有请求按真实 `RemoteAddr` 计入 cap，伪造 XFF 无法绕过上限。

> loop goroutine 内发生 panic 会被恢复并转为 error 经 `Serve` 返回值上抛（不会让进程崩溃，也不会被静默吞没）。

### 握手定制（子协议 / 压缩 / 响应头）

握手期的 gorilla `Upgrader` 默认由桥接层配置（缓冲区、共享写缓冲池、10s 握手超时、按 `AllowedOrigins` 构造的 `CheckOrigin`）。需要更多控制时用 `Options.ConfigureUpgrader` 逃生阀——它在桥接层设完默认值之后、Upgrade 之前被调用一次：

```go
_ = wssession.Serve(ctx, w, r, wssession.Options{
	ConfigureUpgrader: func(u *websocket.Upgrader) {
		u.Subprotocols = []string{"chat.v1"}  // Sec-WebSocket-Protocol 协商
		u.EnableCompression = true            // permessage-deflate，长文本流式推送省带宽
		u.HandshakeTimeout = 5 * time.Second
	},
	ResponseHeader: http.Header{"X-Conn-Id": []string{connID}}, // 101 响应附加头
}, handlers)
```

> **安全警告**：回调可以覆盖 `CheckOrigin`。一旦覆盖，`AllowedOrigins` 的 same-origin / 白名单保护即失效，Origin 校验责任完全转移给调用方。
>
> `ResponseHeader` 单独出字段而非并入回调：响应头是 Upgrade 的参数、不是 `Upgrader` 的字段。子协议协商结果由 gorilla 依据 `Subprotocols` 自动写入 101 响应，不需要手工设置。

### 可观测性（OnEvent + 连接数快照）

日志 / metrics 由调用方接入，本包通过两个口子把运行状态暴露出来：

**`Options.OnEvent`** —— 可选回调，在以下事件发生时被调用（`nil` 则跳过；回调内 panic 会被桥接层 recover）：

| `Event.Type` | 含义 | 关键字段 |
|---|---|---|
| `EventPanic` | 某 loop goroutine 发生 panic | `Err` |
| `EventSlowConsumer` | 出站队列满超时，客户端消费跟不上 | `Err` |
| `EventCapRejected` | 连接被 IP / token cap 拒绝 | `Key`（cap key） |
| `EventAbnormalClose` | 1006 异常断开（无正常 close 握手） | `Err` |
| `EventRateLimited` | 双向模式入站消息超过速率限制 | `Reason` |
| `EventTurnInterrupted` | 双向模式上一轮被新消息打断 | `Reason` |
| `EventTurnStuck` | 双向模式上一轮取消后未及时退出 | `Reason` / `Err` |
| `EventClientClose` | 客户端主动发来 close 帧 | `Code`（close code） / `Reason`（客户端文案） |

> `OnEvent` 必须**快且非阻塞**（同步参与连接收敛路径），与 `ParseRequest` 同约定。

**`wssession.ConnCapSnapshot() map[string]int64`** —— 返回当前所有活跃 cap key 及其连接数的独立副本快照，供 metrics 拉取 / 运维查询（key 形态 `ip:<ip>:<path>` / `token:<key>:<path>`，归零的 key 不出现）：

```go
for key, n := range wssession.ConnCapSnapshot() {
	metrics.Gauge("ws_active_conns", float64(n), "key", key)
}
```

> 注：1006 异常断开会通过 `OnEvent` 上报 `EventAbnormalClose`，但**不**作为 `Serve` 的错误返回——避免把常见的客户端网络抖动变成调用方的错误误报。
>
> `EventClientClose` 与 `EventAbnormalClose` 互斥：前者是客户端带 close code 的明确关闭意图（含自定义 `4xxx` 码），后者是没有 close 握手的异常断开（1006）。做关闭原因分布统计时用 `Event.Code` 分桶，不会被网络抖动污染。

### 多端会话管理（sessionhub）

同一用户可能有多个并发连接（多设备 / 多标签页）。可选子包 `wssession/sessionhub` 提供按 userID 管理活跃连接的轻量注册表：**枚举元数据**（`List` / `Count` / `Users` / `Total`）、**定向推送 / 踢下线**（`RegisterConn` 登记连接句柄 + `Conns` 枚举操作）。它与核心包零 import 依赖（`*wssession.Session` 结构性满足 `sessionhub.Conn` 接口），集成靠 `Serve` 是阻塞调用——`register → defer release → Serve`：

```go
package main

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gtkit/wssession"
	"github.com/gtkit/wssession/sessionhub"
)

var hub = sessionhub.New()

func main() {
	r := gin.New()
	r.GET("/ws", handleWS)
	// 运维 / 在线状态：列出某用户当前所有活跃连接
	r.GET("/users/:id/conns", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"count": hub.Count(c.Param("id")),
			"conns": hub.List(c.Param("id")),
		})
	})
	_ = r.Run(":8080")
}

func handleWS(c *gin.Context) {
	// userID 来自首帧 token：用闭包把 release 从 ParseRequest 回传到 handler 的 defer
	var sess *wssession.Session
	var release func()
	defer func() {
		if release != nil {
			release()
		}
	}()

	_ = wssession.Serve(c.Request.Context(), c.Writer, c.Request,
		wssession.Options{},
		wssession.Handlers{
			// OnConnect 拿到 *Session 作为连接句柄（Push / Kick）
			OnConnect: func(_ context.Context, s *wssession.Session) error {
				sess = s
				return nil
			},
			ParseRequest: func(ctx context.Context, raw []byte) (key string, req any, err error) {
				userID := parseUserID(raw)
				// 单点登录：踢掉同 userID 的旧连接（被踢端收到 error 409 + close 1008，不应自动重连）
				for _, old := range hub.Conns(userID) {
					old.Kick(ctx, "logged in elsewhere")
				}
				_, release = hub.RegisterConn(userID, sess) // 登记；Serve 返回（连接结束）时 defer 注销
				return userID, userID, nil
			},
			Run: func(ctx context.Context, _ any, _ wssession.PushSink) error {
				<-ctx.Done()
				return nil
			},
		},
	)
}

func parseUserID(raw []byte) string { return "user-1" } // 占位：解析首帧
```

向某用户的所有在线端**定向推送**（任意服务端代码处）：

```go
for _, conn := range hub.Conns("user-1") {
	_ = conn.Push(ctx, gin.H{"event": "notice", "text": "您有新的订单"})
}
```

> **多端策略由业务决定**：允许多端就不踢（只 `RegisterConn` 不 `Kick`）；限制端数就先 `Conns` 检查再决定。`Conns` 返回快照，必须在循环里调 `Kick`（它同步等出帧 flush）——不要在持有自己锁的临界区内调用。
> 只需要枚举不需要操作时，继续用 `Register`（无句柄，`Conns` 不含这类条目）。

> **握手前鉴权场景更简单**：若 userID 在调 `Serve` 前已知（中间件鉴权放进 ctx），直接 `_, release := hub.Register(userID); defer release()` 再 `Serve(...)` 即可，无需闭包。
>
> **出帧序列化**：`PushSink.Push` 的 payload 在**业务 goroutine 侧**用 `gtkitjson` 序列化（可并行），`writeLoop` 只做纯 IO——大 payload 不阻塞出帧管道。因此 `Push` 现在会在 payload **无法序列化时立即返回错误**（如含 channel 字段）。

### 扇出推送（一次序列化 + 非阻塞）

把同一条消息推给一批连接时，逐个 `Push` 有两个代价：payload 被**重复序列化** N 次，且任一慢客户端会把整轮遍历卡住最多 `QueueOfferTimeout`（默认 5s）。`Frame` + `TryPush` 消除这两点：

```go
// 一次序列化，跨连接复用
frame, err := wssession.NewFrame(gin.H{"event": "notice", "text": "系统维护通知"})
if err != nil {
	return err
}

// 消费者侧声明所需能力（Go 惯用做法）：sessionhub.Conn 保持单方法接口不变
type trySink interface {
	TryPush(ctx context.Context, payload any) error
}

for _, uid := range hub.Users() {
	for _, conn := range hub.Conns(uid) {
		if ts, ok := conn.(trySink); ok {
			_ = ts.TryPush(ctx, frame) // 队列满立即返回 ErrSlowConsumer，不等待
			continue
		}
		_ = conn.Push(ctx, frame)
	}
}
```

- `NewFrame(payload)` 只序列化一次；`Push` / `TryPush` 识别 `Frame` 后原样入队，不重复 `Marshal`。
- `TryPush` 队列满即返回 `ErrSlowConsumer` 且该帧不入队——慢连接自己失败，不拖累其余连接。
- `Frame` 构造后不可变，可被多个 goroutine 并发推送；连接级的 `MaxOutboundFrameBytes` 仍按目标连接校验。
- **`NewBinaryFrame(data)` 不复制 `data`**，且出帧是**异步**写出（入队后由写循环发送）。因此推送后**不要复用或修改**那段字节——典型陷阱是从 `sync.Pool` 取 buffer、`Push` 之后立刻归还/覆写，客户端会收到损坏数据。需要复用 buffer 就自己先 `bytes.Clone`。`NewFrame(payload)` 无此问题（序列化产生的是新分配的字节）。
- `PushSink` 与 `sessionhub.Conn` 仍是单方法接口（下游自定义实现 / test fake 不受影响），所以 `TryPush` 通过上面的类型断言获取。

> 扇出由调用方组合：连接枚举用可选子包 `sessionhub`（或业务自己的注册表），推送策略写在循环里——上面那段就是完整实现。

### 关键约束

- `ParseRequest` 必填；`Run` / `OnMessage` / `OnBinaryMessage` 三者至少一个，`OnConnect` 可选；
  缺必填字段时 `Serve` 返回 `ErrHandlersIncomplete`。
- **`ParseRequest` 返回的 `err.Error()` 会作为 `error(422)` 帧的 reason 原样下发给客户端**（按 256 字节截断）：只返回适合客户端看到的文案，不要把内部错误包装进去。
- **`Serve` 的返回值**：服务端主动错误关闭的根因——首帧超时（`ErrFirstFrameTimeout`）、`ParseRequest` 错误、token 维度连接 cap（`ErrConnCapExceeded`）、`Run` / `OnMessage` / `OnBinaryMessage` 返回的非预期错误或 panic、帧准入拒绝（`ErrInvalidFrame` / `ErrUnexpectedFrame`）——确定性地从 `Serve` 返回；`Kick`、客户端主动关闭、会话超时、上游取消返回 nil。
- `Run` 是 blocking 调用，跑在独立 processLoop；**不要**在 `Run` 内 spawn goroutine 后立即 return
  （否则会被当作业务已结束）。需异步处理就在 `Run` 内自己用 errgroup 编排后再 return。
- 单向模式下 `Run` 返回 nil 即视为业务结束：`wssession` 会 flush 完在途帧、下发 close(1000) 并主动关闭连接。
- **Handlers 闭包不要捕获可变状态**：同一个 `Handlers` 值复用于多次 `Serve`（如提升为包级变量）时，
  闭包捕获的可变状态会被该路由**所有连接（所有用户）共享**——既是数据竞争也是用户间串台。
  连接级状态经 `ParseRequest` 返回的 `req` 传递，或像本文示例一样在每次请求内现场构造 `Handlers`。
- `Options.OnEvent` 回调会被多个 goroutine 并发调用，实现必须并发安全且快速非阻塞。
- `sink.Push` 返回 `ErrSlowConsumer`（出站队列满 + 超时）时，业务应 `return` 让 `wssession` 收敛连接。
- `sink.Push` 返回 `ErrOutboundFrameTooLarge` 时，该业务帧没有入队；生产环境建议按协议设置 `MaxOutboundFrameBytes`。
- 双向模式的 `OnMessage` / `OnBinaryMessage` 必须监听 `ctx`；否则 `TurnCloseTimeout` 到期后会触发 `EventTurnStuck` 并关闭连接。
- **`Run` 也必须监听 `ctx`**：Go 无法强杀 goroutine，桥接层只能等它自己退出。不响应 `ctx` 的 `Run` 会让 `Serve` 一直不返回（该 HTTP handler 与 goroutine 随之滞留），单向与双向模式皆然；`TurnCloseTimeout` 只约束消息回调，**不约束 `Run`**。
- **首帧可以和后续帧连续发出**（不必等 `subscribed` 回执）：帧准入按"是否首帧"判定，与服务端何时处理完首帧无关。但首帧本身必须是文本帧。
- `Dispatch` 为 `DispatchConcurrent` 时 `MaxConcurrentMessages` 必须 > 0，未定义的 `Dispatch` 值一律被 `Serve` 拒绝（fail-closed，不静默回退到默认模式）。
- `sink.Push` 返回 `ErrInvalidFrame` 时说明推的是零值 `Frame`——`Frame` 必须经 `NewFrame` / `NewBinaryFrame` 创建。
- `Session.ExtendDeadline` 需先开启 `Options.SessionDeadlineExtendable`，否则返回 `ErrDeadlineNotExtendable`；开启后会话 ctx 到期表现为 `context.Canceled`（原因见 `context.Cause`）且 `ctx.Deadline()` 失效。

### 优雅停机

**陷阱**：`http.Server.Shutdown` 对被 hijack 的连接（WebSocket 正是）**既不关闭也不等待**，且 hijack 之后
`r.Context()` 不会因 Shutdown 取消——若按上文示例把 `c.Request.Context()` 直接当 parent 传给 `Serve`，
停机时所有 WS 会话会一直挂到 `MaxSessionDuration`（默认 30 分钟）。

**正确接法**：用进程级 shutdown ctx 作为 `Serve` 的 parent。停机时 cancel，所有会话经既有收敛路径
向客户端发 close `1001`（GoingAway，客户端据此重连到新实例）并释放：

```go
shutdownCtx, shutdown := context.WithCancel(context.Background())

r.GET("/ws", func(c *gin.Context) {
	// parent 同时尊重停机信号与单请求生命周期
	ctx, cancel := context.WithCancel(shutdownCtx)
	defer cancel()
	context.AfterFunc(c.Request.Context(), cancel) // 客户端断开也收敛

	_ = wssession.Serve(ctx, c.Writer, c.Request, opts, handlers)
})

// 停机流程：先 shutdown() 收敛 WS 会话，再 srv.Shutdown(ctx) 处理普通 HTTP 请求
```

**停机期的新连接**：`shutdownCtx` 取消后到进程真正退出之间仍可能有新请求进来（负载均衡摘流有延迟）。`Serve` 在 Upgrade **之前**检查 parent ctx，已取消时直接返回 HTTP 503 并不升级连接：

```json
{"code":503,"msg":"server shutting down","data":null}
```

客户端因此拿到一个干净的 503（可按普通 HTTP 错误退避重试），而不是"握手成功后立刻闪断"。这一步发生在连接 cap 之前，不占用配额；`Serve` 返回的 error 包装了 parent 的 `context.Canceled`，可用 `errors.Is` 判断。

> `Handlers` / `Options` 的合法性校验仍排在停机检查之前——配置错误和停机是两类问题，不该在滚动更新期间被 503 掩盖。

### 前端对接（WebSocket）

#### 单向订阅模式

按 `wssession` 的协议约定对接，步骤：

1. **建立连接**：`new WebSocket("wss://…/path")`，生产用 `wss`。浏览器原生 `WebSocket` 不能自定义 header——鉴权 token 走下一步的首帧（或 cookie）。
2. **连上后立即发首帧订阅**（`onopen` 里），文本 JSON，字段与服务端 `ParseRequest` 解析的一致。
3. **解析下行帧**（`onmessage`）：先 `JSON.parse`，按 `event` 分支——`subscribed`（订阅确认）/ `error`（带 `code` + `reason`）/ 其余为业务推送 payload。
4. **首帧后不要再发业务帧**：单向模式（`Handlers.OnMessage == nil`）协议约定"一连接一订阅"，订阅后再发任何帧会被服务端拒（`error` 422 + close）。
5. **心跳无需处理**：服务端定期发 WebSocket Ping 控制帧，浏览器自动回 Pong；前端不用写心跳代码。长时间无数据时连接靠服务端 Ping 保活（`PongWait` 默认 70s）。
6. **关闭与重连**：`onclose` 里区分正常关闭（code 1000）与异常，异常用指数退避重连，**重连后需重新发首帧订阅**。

```js
function connect() {
  const ws = new WebSocket("wss://example.com/orders/query/wsmsg");

  ws.onopen = () => {
    // 2) 首帧订阅（字段对应服务端 ParseRequest）
    ws.send(JSON.stringify({ action: "subscribe", token: "abc123" }));
  };

  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    switch (msg.event) {
      case "subscribed":
        console.log("订阅成功", msg.timestamp);
        break;
      case "error":
        // code: 408 首帧超时 / 415 非文本帧 / 422 解析失败 / 429 超限 / 500 内部错误
        console.error(`服务端错误 ${msg.code}: ${msg.reason}`);
        break;
      default:
        render(msg); // 业务推送 payload
    }
  };

  // 6) 异常关闭 → 指数退避重连（重连后 onopen 会重新发首帧）
  ws.onclose = (e) => {
    if (e.code === 1000) return; // 正常关闭，不重连
    setTimeout(connect, backoff());
  };
  ws.onerror = () => ws.close();
  return ws;
}
```

> **单向模式约束**：`Handlers.OnMessage == nil` 时，订阅成功后不要用 `ws.send` 继续发业务请求帧；多发会触发服务端 `error(422) + close`。

#### 双向消息模式

`Handlers.OnMessage != nil` 时，首帧仍是订阅 / 鉴权帧；收到 `subscribed` 后可以继续发送业务消息。每条业务消息触发一轮 `OnMessage`，新消息会打断上一轮。客户端应把 `error(429)` 当作限速提示；收到 `error(500)` 或 close 后按业务策略重连并重新发送首帧。
