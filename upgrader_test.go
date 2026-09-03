package wssession

import (
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestConfigureUpgraderNegotiatesSubprotocol:回调内设置 Subprotocols 后,
// 握手完成子协议协商。
func TestConfigureUpgraderNegotiatesSubprotocol(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	opts := Options{
		ConfigureUpgrader: func(u *websocket.Upgrader) {
			u.Subprotocols = []string{"chat.v1"}
		},
	}
	srv := newTestSession(t, path, opts, passthroughHandlers(nil))

	dialer := &websocket.Dialer{
		HandshakeTimeout: 3 * time.Second,
		Subprotocols:     []string{"chat.v1"},
	}
	conn, resp, err := dialer.Dial(wsURL(srv.URL, path), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if got := conn.Subprotocol(); got != "chat.v1" {
		t.Fatalf("conn.Subprotocol() = %q, want chat.v1", got)
	}
	if got := resp.Header.Get("Sec-Websocket-Protocol"); got != "chat.v1" {
		t.Fatalf("响应头 Sec-Websocket-Protocol = %q, want chat.v1", got)
	}
}

// TestConfigureUpgraderEnableCompression:开启 permessage-deflate 后连接可正常收发。
func TestConfigureUpgraderEnableCompression(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	opts := Options{
		ConfigureUpgrader: func(u *websocket.Upgrader) {
			u.EnableCompression = true
		},
	}
	srv := newTestSession(t, path, opts, passthroughHandlers(nil))

	dialer := &websocket.Dialer{HandshakeTimeout: 3 * time.Second, EnableCompression: true}
	conn, _, err := dialer.Dial(wsURL(srv.URL, path), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"t"}`))
	if got := readJSONFrame(t, conn, 2*time.Second); got["event"] != "subscribed" {
		t.Fatalf("首帧 = %v, want subscribed", got)
	}
}

// TestConfigureUpgraderCanOverrideCheckOrigin:回调可覆盖 CheckOrigin,
// 覆盖后 AllowedOrigins 白名单不再生效(安全责任转移给调用方)。
func TestConfigureUpgraderCanOverrideCheckOrigin(t *testing.T) {
	t.Parallel()
	evilOrigin := http.Header{"Origin": []string{"https://evil.example"}}

	// 基线:白名单外的 Origin 被拒。
	rejectPath := uniquePath(t) + "/reject"
	rejectSrv := newTestSession(t, rejectPath,
		Options{AllowedOrigins: []string{"https://allowed.example"}}, passthroughHandlers(nil))
	dialer := &websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	if conn, _, err := dialer.Dial(wsURL(rejectSrv.URL, rejectPath), evilOrigin); err == nil {
		_ = conn.Close()
		t.Fatal("白名单外 Origin 应被拒")
	}

	// 覆盖 CheckOrigin 后放行。
	allowPath := uniquePath(t) + "/allow"
	allowSrv := newTestSession(t, allowPath, Options{
		AllowedOrigins: []string{"https://allowed.example"},
		ConfigureUpgrader: func(u *websocket.Upgrader) {
			u.CheckOrigin = func(*http.Request) bool { return true }
		},
	}, passthroughHandlers(nil))

	conn, _, err := dialer.Dial(wsURL(allowSrv.URL, allowPath), evilOrigin)
	if err != nil {
		t.Fatalf("覆盖 CheckOrigin 后仍被拒: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
}

// TestResponseHeaderSentOnHandshake:ResponseHeader 出现在 101 响应上。
func TestResponseHeaderSentOnHandshake(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	opts := Options{ResponseHeader: http.Header{"X-Conn-Id": []string{"abc"}}}
	srv := newTestSession(t, path, opts, passthroughHandlers(nil))

	_, resp := dial(t, wsURL(srv.URL, path))
	if got := resp.Header.Get("X-Conn-Id"); got != "abc" {
		t.Fatalf("响应头 X-Conn-Id = %q, want abc", got)
	}
}

// TestConfigureUpgraderNilKeepsDefaultOriginCheck:未配置逃生阀时,
// 桥接层默认的 same-origin 校验照旧生效。
func TestConfigureUpgraderNilKeepsDefaultOriginCheck(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	srv := newTestSession(t, path, Options{}, passthroughHandlers(nil))

	dialer := &websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	conn, _, err := dialer.Dial(wsURL(srv.URL, path),
		http.Header{"Origin": []string{"https://evil.example"}})
	if err == nil {
		_ = conn.Close()
		t.Fatal("默认 same-origin 校验应拒绝跨域 Origin")
	}
}
