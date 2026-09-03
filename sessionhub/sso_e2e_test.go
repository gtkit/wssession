package sessionhub_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/gtkit/wssession"
	"github.com/gtkit/wssession/sessionhub"
)

// TestSingleSignOnKickEndToEnd 用两条真实 WS 连接验证单点登录踢旧模式:
// 后登录者经 hub.Conns 踢出前者(error 409 + close 1008),自己正常服务,
// 同时验证 *wssession.Session 结构性满足 sessionhub.Conn。
func TestSingleSignOnKickEndToEnd(t *testing.T) {
	t.Parallel()
	hub := sessionhub.New()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		var sess *wssession.Session
		var release func()
		handlers := wssession.Handlers{
			OnConnect: func(_ context.Context, s *wssession.Session) error {
				sess = s
				return nil
			},
			ParseRequest: func(ctx context.Context, raw []byte) (string, any, error) {
				uid := string(raw)
				// 单点登录:踢掉同 userID 的旧连接,再注册自己
				for _, old := range hub.Conns(uid) {
					old.Kick(ctx, "logged in elsewhere")
				}
				_, release = hub.RegisterConn(uid, sess)
				return uid, uid, nil
			},
			Run: func(ctx context.Context, _ any, _ wssession.PushSink) error {
				<-ctx.Done()
				return nil
			},
		}
		_ = wssession.Serve(r.Context(), w, r, wssession.Options{}, handlers)
		if release != nil {
			release()
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"

	subscribe := func(t *testing.T) *websocket.Conn {
		t.Helper()
		conn, _, err := (&websocket.Dialer{HandshakeTimeout: 3 * time.Second}).Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		_ = conn.WriteMessage(websocket.TextMessage, []byte("user-1"))
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		var sub map[string]any
		if err := conn.ReadJSON(&sub); err != nil || sub["event"] != "subscribed" {
			t.Fatalf("subscribe frame = %v, err = %v", sub, err)
		}
		return conn
	}

	conn1 := subscribe(t)
	conn2 := subscribe(t) // 触发对 conn1 的踢出

	// conn1 收到 error(409) + close 1008
	_ = conn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	var kicked map[string]any
	if err := conn1.ReadJSON(&kicked); err != nil {
		t.Fatalf("read kicked frame: %v", err)
	}
	if code, _ := kicked["code"].(float64); kicked["event"] != "error" || int(code) != 409 {
		t.Fatalf("kicked frame = %v, want error(409)", kicked)
	}
	_, _, err := conn1.ReadMessage()
	if !websocket.IsCloseError(err, websocket.ClosePolicyViolation) {
		t.Fatalf("err = %v, want close 1008", err)
	}

	// 旧连接收敛注销后只剩新连接;新连接经定向推送可达
	deadline := time.Now().Add(3 * time.Second)
	for hub.Count("user-1") != 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := hub.Count("user-1"); n != 1 {
		t.Fatalf("Count = %d, want 1", n)
	}
	for _, c := range hub.Conns("user-1") {
		if err := c.Push(t.Context(), map[string]string{"notice": "hi"}); err != nil {
			t.Fatalf("directed push: %v", err)
		}
	}
	_ = conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	var pushed map[string]any
	if err := conn2.ReadJSON(&pushed); err != nil || pushed["notice"] != "hi" {
		t.Fatalf("pushed frame = %v, err = %v", pushed, err)
	}
}
