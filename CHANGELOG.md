# Changelog

本项目遵循 [Keep a Changelog 1.1.0](https://keepachangelog.com/zh-CN/1.1.0/)。

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## [1.1.0] - 2026-09-03

### Added

- 新增 `ErrConnCapExceeded` 哨兵：连接被 IP 或 token 维度连接 cap 拒绝时，`Serve` 返回包装了它的错误，可用 `errors.Is` 判断

### Changed

- `Serve` 的返回值改为确定性：服务端主动错误关闭的根因（首帧超时 `ErrFirstFrameTimeout`、`ParseRequest` 错误、token cap、`Run` / `OnMessage` / `OnBinaryMessage` 返回的非预期错误或 panic、帧准入拒绝）一律从 `Serve` 返回。此前单向模式下它与读循环因主动关闭而返回的预期 close 错误竞争，偶发返回 nil；双向模式下消息回调的业务错误与后台 `Run` 的 panic 从不返回，服务端侧没有任何信号；首帧超时此前实际返回 nil
- 后台 `Run`（双向模式）发生 panic 时，现在与消息回调 panic 一致：下发 `error(500)` 帧并完成 close 握手，此前只静默收敛连接
- 握手阶段默认设置 10s `HandshakeTimeout`（此前无超时），仍可经 `Options.ConfigureUpgrader` 覆盖
- `Serve` 对 Upgrade 失败的错误加 `wssession: upgrade:` 前缀包装，`errors.Is` / `errors.As` 仍可穿透到 gorilla 原始错误
- `sessionhub.Registry.Users` 在没有活跃连接时返回 nil（此前为空切片），与 `List` / `Conns` 的空值约定一致

### Fixed

- `Push` / `TryPush` 传入 `*Frame` 指针时不再被当作普通 payload 序列化成 `{}`，而是与 `Frame` 同样原样入队；nil `*Frame` 返回 `ErrInvalidFrame`
- error 帧与 `EventClientClose` 的 reason 按 256 字节截断时回退到 UTF-8 字符边界，不再把多字节字符切成非法字节

### Security

- `TrustedProxyCount` 大于 `X-Forwarded-For` 条目数时（请求没有经过完整的可信链）改为退回 `RemoteAddr`，此前会取客户端可控的最左端条目，使 IP 维度连接 cap 可被伪造的 XFF 绕过

## [1.0.0] - 2026-09-03

### Added

- 首个版本：通用、与业务无关的 WebSocket 长连接底座，import 路径 `github.com/gtkit/wssession`
- 核心四件套 `Serve` / `Handlers` / `Options` / `PushSink`：业务逻辑经 `Handlers{ParseRequest, Run, OnConnect, OnMessage, OnBinaryMessage}` 函数式注入，包本身不含领域语义；错误统一经 `Serve` 返回值上抛，由调用方决定如何记录
- 单向订阅模式：首帧鉴权解析后由 `Run` 持续推送，`Run` 返回后 flush 在途帧并下发 close 1000 收敛连接
- 双向消息模式：单连接内多轮双向消息，`Options.Dispatch` 提供三种入站调度——`DispatchInterrupt`（新消息打断上一轮，任一时刻至多一个活跃轮次）、`DispatchSequential`（逐条处理完再取下一条）、`DispatchConcurrent`（并发处理，上限 `MaxConcurrentMessages`）
- 二进制帧支持：提供 `Handlers.OnBinaryMessage` 即接受订阅后的入站二进制帧，`NewBinaryFrame` 用于推送二进制帧，调度、限速与错误处置与文本帧一致
- 出站接口：`Session.Push`（阻塞入队，受 `QueueOfferTimeout` 约束）、`Session.TryPush`（非阻塞，队列满立即返回 `ErrSlowConsumer`）、`Frame` / `NewFrame`（一次序列化跨连接复用，扇出不重复序列化）
- 连接治理：心跳保活、慢消费者反压、IP 与自定义 key 双维度连接 cap、Origin 白名单、首帧超时、`ReadLimit` 入站帧上限、`MaxOutboundFrameBytes` 出站帧上限、`MaxSessionDuration` 会话时长封顶、`InboundRatePerSecond` / `InboundRateBurst` 单连接入站限速（超限丢弃并下发 `error(429)`，不关连接）、loop goroutine panic 恢复并转为 error 上抛
- 会话续期：`Options.SessionDeadlineExtendable` 开启后可用 `Session.ExtendDeadline` 把存活上限重设为 `now+d`，用于 JWT 等凭证刷新时不断连接；默认关闭，未开启调用返回 `ErrDeadlineNotExtendable`
- 关闭语义：服务端主动关闭完成 WebSocket close 握手，业务码映射到 close code（`408/409/415/422/429` → 1008，`500` → 1011，会话超时与上游取消 → 1001），1006 保留为真实网络异常信号；`Session.Kick` 下发 `error(409)` + close 1008 踢下线（幂等）；停机期（parent context 已取消）在 Upgrade 之前返回 HTTP 503，不占用连接 cap 配额
- 可观测：`Options.OnEvent` 上报 panic、慢消费者、连接 cap 拒绝、入站限速、轮次被打断、轮次失约、客户端 close code、1006 异常断开等事件（回调会被多 goroutine 并发调用，实现须并发安全）；`ConnCapSnapshot` 返回活跃 cap key 快照；`Session.Value` / `Request` / `ClientIP` / `IsClosed` 四个只读查询
- 握手可配：`Options.ConfigureUpgrader` 与 `Options.ResponseHeader` 支持 `Sec-WebSocket-Protocol` 子协议协商、permessage-deflate 压缩、握手超时、缓冲区大小与 101 响应头；升级握手接入共享写缓冲池降低每连接常驻内存
- 客户端 IP 识别：默认取自 `RemoteAddr`，仅在显式配置 `Options.TrustedProxyCount > 0` 时按可信代理跳数从 `X-Forwarded-For` 由右向左解析，避免伪造 XFF 绕过 IP 维度连接 cap
- 可选子包 `sessionhub`：按 userID 管理同一用户的多个并发连接（多设备 / 多标签页），`Register` / `List` / `Count` / `Users` / `Total` 枚举元数据，`Conn` 接口（`*wssession.Session` 结构性满足）+ `RegisterConn` + `Conns` 支持定向推送与单点登录踢旧连接；注册 / 注销 / 枚举 / 句柄操作并发安全
- 配置校验：`Options.Validate` 对非法组合 fail-closed 报错（如 `DispatchConcurrent` 未设 `MaxConcurrentMessages`、`Dispatch` 为未定义值）
