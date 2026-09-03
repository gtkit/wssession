package wssession

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	gtkitjson "github.com/gtkit/json/v2"
)

// upgradeRequest 构造一个合法的 WebSocket 握手请求(仅用于不会真正 Upgrade 的路径)。
func upgradeRequest(path string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Sec-WebSocket-Version", "13")
	r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	r.RemoteAddr = "203.0.113.9:34567"
	return r
}

// TestServeShutdownRejectsWith503:parent ctx 已取消(停机)时不 Upgrade,返回 HTTP 503。
func TestServeShutdownRejectsWith503(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	rec := httptest.NewRecorder()
	err := Serve(ctx, rec, upgradeRequest(uniquePath(t)), Options{}, passthroughHandlers(nil))

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve error = %v, want 包装 context.Canceled", err)
	}
	if !strings.Contains(err.Error(), ReasonServerShuttingDown) {
		t.Fatalf("Serve error = %v, want 含 %q", err, ReasonServerShuttingDown)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}

	var body struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := gtkitjson.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("响应体不是合法 JSON: %v (%s)", err, rec.Body.String())
	}
	if body.Code != http.StatusServiceUnavailable || body.Msg != ReasonServerShuttingDown {
		t.Fatalf("body = %+v, want code=503 msg=%q", body, ReasonServerShuttingDown)
	}
}

// TestServeShutdownDoesNotConsumeIPCap:停机拒连发生在 IP cap 之前,不占用配额。
func TestServeShutdownDoesNotConsumeIPCap(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	opts := Options{ConnCapEnabled: true, ConnCapIPMax: 1, ConnCapKeyMax: 1}
	_ = Serve(ctx, httptest.NewRecorder(), upgradeRequest(path), opts, passthroughHandlers(nil))

	for key := range ConnCapSnapshot() {
		if strings.HasSuffix(key, ":"+path) {
			t.Fatalf("停机拒连不应占用 cap 配额,却出现 key %q", key)
		}
	}
}

// TestServeValidatesHandlersBeforeShutdownCheck:配置错误优先于停机检查,
// 否则滚动更新时会把"handler 写错了"伪装成"正在停机"。
func TestServeValidatesHandlersBeforeShutdownCheck(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	rec := httptest.NewRecorder()
	err := Serve(ctx, rec, upgradeRequest(uniquePath(t)), Options{}, Handlers{})

	if !errors.Is(err, ErrHandlersIncomplete) {
		t.Fatalf("Serve error = %v, want ErrHandlersIncomplete", err)
	}
	if rec.Code == http.StatusServiceUnavailable {
		t.Fatal("配置错误不应返回 503 停机响应")
	}
}

// TestServeValidatesOptionsBeforeShutdownCheck:Options 非法同样优先于停机检查。
func TestServeValidatesOptionsBeforeShutdownCheck(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	opts := Options{Dispatch: DispatchConcurrent} // 缺 MaxConcurrentMessages
	err := Serve(ctx, httptest.NewRecorder(), upgradeRequest(uniquePath(t)), opts, passthroughHandlers(nil))

	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("Serve error = %v, want Options 校验错误", err)
	}
}
