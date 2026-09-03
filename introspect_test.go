package wssession

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// dialWithHeader 用自定义 HTTP 头建连。
func dialWithHeader(t *testing.T, url string, header http.Header) *websocket.Conn {
	t.Helper()
	dialer := &websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	conn, _, err := dialer.Dial(url, header)
	if err != nil {
		t.Fatalf("dial: %v (url=%s)", err, url)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestSessionValueReturnsParseRequestReq:双向模式的消息回调经 Session.Value()
// 拿到 ParseRequest 返回的业务请求对象,无需闭包捕获可变状态。
func TestSessionValueReturnsParseRequestReq(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	var sess atomic.Pointer[Session]
	type sessionObj struct{ UserID string }
	want := &sessionObj{UserID: "u-42"}

	h := Handlers{
		OnConnect: func(_ context.Context, s *Session) error {
			sess.Store(s)
			return nil
		},
		ParseRequest: func(_ context.Context, _ []byte) (string, any, error) {
			return "tok", want, nil
		},
		OnMessage: func(ctx context.Context, _ []byte, sink PushSink) error {
			got, _ := sess.Load().Value().(*sessionObj)
			return sink.Push(ctx, map[string]any{"same": got == want})
		},
	}
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`hello`))

	if got := readJSONFrame(t, conn, 2*time.Second); got["same"] != true {
		t.Fatalf("Value() 未返回 ParseRequest 的 req: %v", got)
	}
}

// TestSessionValueNilBeforeFirstFrame:首帧解析成功前 Value() 为 nil。
func TestSessionValueNilBeforeFirstFrame(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	valueAtConnect := make(chan any, 1)

	h := passthroughHandlers(nil)
	h.OnConnect = func(_ context.Context, s *Session) error {
		valueAtConnect <- s.Value()
		return nil
	}
	srv := newTestSession(t, path, Options{}, h)
	_, _ = dial(t, wsURL(srv.URL, path))

	select {
	case v := <-valueAtConnect:
		if v != nil {
			t.Fatalf("OnConnect 时 Value() = %v, want nil", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("2s 内 OnConnect 未被调用")
	}
}

// TestSessionValueNilReqDoesNotPanic:ParseRequest 返回 nil req 时 Value() 返回 nil。
func TestSessionValueNilReqDoesNotPanic(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	var sess atomic.Pointer[Session]

	h := Handlers{
		OnConnect: func(_ context.Context, s *Session) error {
			sess.Store(s)
			return nil
		},
		ParseRequest: func(_ context.Context, _ []byte) (string, any, error) {
			return "tok", nil, nil
		},
		OnMessage: func(ctx context.Context, _ []byte, sink PushSink) error {
			return sink.Push(ctx, map[string]any{"nil": sess.Load().Value() == nil})
		},
	}
	srv := newTestSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"sub":1}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`hello`))

	if got := readJSONFrame(t, conn, 2*time.Second); got["nil"] != true {
		t.Fatalf("nil req 时 Value() 应为 nil: %v", got)
	}
}

// TestSessionRequestExposesHandshakeMetadata:Request() 可读到握手期 URL 与 Header。
func TestSessionRequestExposesHandshakeMetadata(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	type meta struct {
		path   string
		header string
	}
	got := make(chan meta, 1)

	h := passthroughHandlers(nil)
	h.OnConnect = func(_ context.Context, s *Session) error {
		r := s.Request()
		got <- meta{path: r.URL.Path, header: r.Header.Get("X-Test")}
		return nil
	}
	srv := newTestSession(t, path, Options{}, h)
	dialWithHeader(t, wsURL(srv.URL, path), http.Header{"X-Test": []string{"1"}})

	select {
	case m := <-got:
		if m.path != path {
			t.Fatalf("Request().URL.Path = %q, want %q", m.path, path)
		}
		if m.header != "1" {
			t.Fatalf("Request().Header X-Test = %q, want \"1\"", m.header)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("2s 内 OnConnect 未被调用")
	}
}

// TestSessionClientIPMatchesCapKey:ClientIP() 与 IP cap key 使用同一口径。
func TestSessionClientIPMatchesCapKey(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	const wantIP = "198.51.100.1"
	got := make(chan string, 1)

	h := passthroughHandlers(nil)
	h.OnConnect = func(_ context.Context, s *Session) error {
		got <- s.ClientIP()
		return nil
	}
	opts := Options{
		TrustedProxyCount: 1,
		ConnCapEnabled:    true,
		ConnCapIPMax:      2,
		ConnCapKeyMax:     2,
	}
	srv := newTestSession(t, path, opts, h)
	dialWithHeader(t, wsURL(srv.URL, path), http.Header{
		"X-Forwarded-For": []string{"203.0.113.7, " + wantIP},
	})

	select {
	case ip := <-got:
		if ip != wantIP {
			t.Fatalf("ClientIP() = %q, want %q", ip, wantIP)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("2s 内 OnConnect 未被调用")
	}

	wantKey := "ip:" + wantIP + ":" + path
	if _, ok := ConnCapSnapshot()[wantKey]; !ok {
		t.Fatalf("cap 快照缺少 key %q,ClientIP 与 cap key 口径不一致", wantKey)
	}
}

// TestSessionIsClosedFlipsAfterClose:连接收敛后 IsClosed() 翻转为 true。
func TestSessionIsClosedFlipsAfterClose(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	var sess atomic.Pointer[Session]
	closedAtConnect := make(chan bool, 1)
	served := make(chan struct{})
	var once sync.Once

	h := passthroughHandlers(nil)
	h.OnConnect = func(_ context.Context, s *Session) error {
		sess.Store(s)
		closedAtConnect <- s.IsClosed()
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		defer once.Do(func() { close(served) })
		_ = Serve(r.Context(), w, r, Options{}, h)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	conn, _ := dial(t, wsURL(srv.URL, path))
	if got := <-closedAtConnect; got {
		t.Fatal("连接建立时 IsClosed() 应为 false")
	}

	_ = conn.Close() // 客户端断开 → 服务端收敛
	select {
	case <-served:
	case <-time.After(3 * time.Second):
		t.Fatal("3s 内 Serve 未返回")
	}
	if !sess.Load().IsClosed() {
		t.Fatal("连接收敛后 IsClosed() 应为 true")
	}
}

// TestSessionClientIPWithoutTrustedProxy:默认不信任 X-Forwarded-For,取传输层地址。
func TestSessionClientIPWithoutTrustedProxy(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	got := make(chan string, 1)

	h := passthroughHandlers(nil)
	h.OnConnect = func(_ context.Context, s *Session) error {
		got <- s.ClientIP()
		return nil
	}
	srv := newTestSession(t, path, Options{}, h)
	dialWithHeader(t, wsURL(srv.URL, path), http.Header{
		"X-Forwarded-For": []string{"203.0.113.7"},
	})

	select {
	case ip := <-got:
		if strings.Contains(ip, "203.0.113.7") {
			t.Fatalf("ClientIP() = %q,默认不应信任 X-Forwarded-For", ip)
		}
		if ip == "" {
			t.Fatal("ClientIP() 为空")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("2s 内 OnConnect 未被调用")
	}
}
