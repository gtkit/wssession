package wssession

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// ExampleFrame 演示扇出推送:payload 只序列化一次,多个连接复用同一帧,
// 并用 TryPush 避免整轮遍历被单个慢客户端拖住。
func ExampleFrame() {
	// conns 通常来自业务自己的注册表(如 sessionhub 的 Conns);此处用空切片示意类型。
	conns := make([]*Session, 0)

	frame, err := NewFrame(map[string]any{"event": "notice", "text": "系统维护通知"})
	if err != nil {
		fmt.Println("marshal:", err)
		return
	}
	for _, c := range conns {
		if err := c.TryPush(context.Background(), frame); errors.Is(err, ErrSlowConsumer) {
			fmt.Println("slow consumer, frame dropped")
		}
	}
	fmt.Printf("fan-out to %d conns\n", len(conns))
	// Output: fan-out to 0 conns
}

// ExampleOptions_Dispatch 演示入站调度模式的选择与 fail-closed 校验。
func ExampleOptions_Dispatch() {
	// 顺序模式:每条消息完整处理完再取下一条(信令 / 协作编辑 / 命令通道)。
	sequential := Options{Dispatch: DispatchSequential}
	// 并发模式:最多 8 轮同时在飞;上限必填,缺失即配置错误。
	concurrent := Options{Dispatch: DispatchConcurrent, MaxConcurrentMessages: 8}
	missingLimit := Options{Dispatch: DispatchConcurrent}

	fmt.Println(sequential.Validate())
	fmt.Println(concurrent.Validate())
	fmt.Println(missingLimit.Validate())
	// Output:
	// <nil>
	// <nil>
	// wssession: MaxConcurrentMessages must be > 0 when Dispatch is DispatchConcurrent
}

// ExampleHandlers_OnBinaryMessage 演示二进制帧的收发:提供 OnBinaryMessage 即接受
// 订阅后的二进制入站帧,回帧用 NewBinaryFrame 经既有 Push 写出。
func ExampleHandlers_OnBinaryMessage() {
	handlers := Handlers{
		// 首帧仍是文本鉴权 / 订阅帧
		ParseRequest: func(context.Context, []byte) (string, any, error) {
			return "token-placeholder", nil, nil
		},
		OnBinaryMessage: func(ctx context.Context, raw []byte, sink PushSink) error {
			// 例:把上行音频分片转码后回推(必须监听 ctx 并及时返回)
			return sink.Push(ctx, NewBinaryFrame(raw))
		},
	}

	http.HandleFunc("/audio", func(w http.ResponseWriter, r *http.Request) {
		_ = Serve(r.Context(), w, r, Options{}, handlers)
	})
}

// ExampleSession_ExtendDeadline 演示凭证刷新:JWT 有效期短于连接期望存活时间时,
// 校验客户端送来的新 token 后续期,连接不断、只换凭证。
func ExampleSession_ExtendDeadline() {
	const tokenTTL = 15 * time.Minute

	http.HandleFunc("/chat", func(w http.ResponseWriter, r *http.Request) {
		var sess *Session

		_ = Serve(r.Context(), w, r, Options{
			MaxSessionDuration:        tokenTTL, // 与 token 有效期对齐
			SessionDeadlineExtendable: true,     // 不开启则 ExtendDeadline 返回 ErrDeadlineNotExtendable
		}, Handlers{
			OnConnect: func(_ context.Context, s *Session) error {
				sess = s
				return nil
			},
			ParseRequest: func(context.Context, []byte) (string, any, error) {
				return "token-placeholder", nil, nil
			},
			OnMessage: func(ctx context.Context, raw []byte, sink PushSink) error {
				// 刷新帧的格式由业务定义,本库不介入
				if string(raw) == "refresh" {
					// 真实实现:先校验新 token,通过后才续期
					if err := sess.ExtendDeadline(tokenTTL); err != nil {
						return err
					}
					return sink.Push(ctx, map[string]any{"event": "refreshed"})
				}
				return sink.Push(ctx, map[string]any{"echo": string(raw)})
			},
		})
	})
}

// ExampleSession_Value 演示双向模式下经 Session.Value 取到 ParseRequest 返回的业务对象,
// 避免让 Handlers 闭包捕获可变状态。
func ExampleSession_Value() {
	type user struct{ ID string }

	http.HandleFunc("/chat", func(w http.ResponseWriter, r *http.Request) {
		// sess 由 OnConnect 注入;每个请求各自一份,闭包只捕获 per-request 局部量。
		var sess *Session

		_ = Serve(r.Context(), w, r, Options{}, Handlers{
			OnConnect: func(_ context.Context, s *Session) error {
				sess = s
				return nil
			},
			ParseRequest: func(context.Context, []byte) (string, any, error) {
				u := &user{ID: "u-1"} // 例:从首帧解析出的会话对象
				return u.ID, u, nil
			},
			OnMessage: func(ctx context.Context, raw []byte, sink PushSink) error {
				u, _ := sess.Value().(*user) // 每轮都能取到
				return sink.Push(ctx, map[string]any{"user": u.ID, "echo": string(raw)})
			},
		})
	})
}
