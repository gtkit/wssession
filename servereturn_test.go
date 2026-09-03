package wssession

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// newServeErrSession 与 newTestSession 相同,但把每次 Serve 的返回值送进 errs。
func newServeErrSession(t *testing.T, path string, opts Options, handlers Handlers) (*httptest.Server, <-chan error) {
	t.Helper()
	errs := make(chan error, 4)
	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		errs <- Serve(r.Context(), w, r, opts, handlers)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, errs
}

// awaitServeErr 等第一个 Serve 返回值,3s 内没有即失败。
func awaitServeErr(t *testing.T, errs <-chan error) error {
	t.Helper()
	select {
	case err := <-errs:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("3s 内 Serve 未返回")
		return nil
	}
}

// TestServeReturnsRunErrorDeterministically:单向 Run 返回业务错误时,Serve 确定性地
// 返回该错误。此前它与 readLoop 因主动 Close 而返回的 net.ErrClosed 竞争 errgroup
// 的首个 error,后者胜出时 Serve 返回 nil。
func TestServeReturnsRunErrorDeterministically(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	srv, errs := newServeErrSession(t, path, Options{}, passthroughHandlers(func(context.Context, PushSink) error {
		return errBusinessFailure
	}))

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"x"}`))

	if err := awaitServeErr(t, errs); !errors.Is(err, errBusinessFailure) {
		t.Fatalf("Serve error = %v, want 包装 errBusinessFailure", err)
	}
}

// TestServeReturnsOnMessageError:双向模式下消息回调返回的业务错误同样从 Serve 返回。
// 此前该路径只 cancel 连接,Serve 恒返回 nil,服务端侧没有任何信号。
func TestServeReturnsOnMessageError(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	srv, errs := newServeErrSession(t, path, Options{}, duplexHandlers(func(context.Context, []byte, PushSink) error {
		return errBusinessFailure
	}))

	conn, _ := dial(t, wsURL(srv.URL, path))
	subscribe(t, conn)
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"q":1}`))

	if err := awaitServeErr(t, errs); !errors.Is(err, errBusinessFailure) {
		t.Fatalf("Serve error = %v, want 包装 errBusinessFailure", err)
	}
}

// TestServeReturnsParseRequestError:ParseRequest 的错误作为根因从 Serve 返回。
func TestServeReturnsParseRequestError(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	srv, errs := newServeErrSession(t, path, Options{}, passthroughHandlers(nil))

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":""}`))

	err := awaitServeErr(t, errs)
	if err == nil || !strings.Contains(err.Error(), "token required") {
		t.Fatalf("Serve error = %v, want ParseRequest 的 token required", err)
	}
}

// TestServeReturnsFirstFrameTimeout:首帧超时作为 ErrFirstFrameTimeout 从 Serve 返回。
func TestServeReturnsFirstFrameTimeout(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	srv, errs := newServeErrSession(t, path, Options{FirstFrameTimeout: 100 * time.Millisecond}, passthroughHandlers(nil))

	_, _ = dial(t, wsURL(srv.URL, path)) // 连上不发首帧

	if err := awaitServeErr(t, errs); !errors.Is(err, ErrFirstFrameTimeout) {
		t.Fatalf("Serve error = %v, want ErrFirstFrameTimeout", err)
	}
}

// TestServeReturnsConnCapSentinel:token 维度 cap 拒绝时 Serve 返回包装 ErrConnCapExceeded 的错误。
func TestServeReturnsConnCapSentinel(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	opts := Options{ConnCapEnabled: true, ConnCapIPMax: 99, ConnCapKeyMax: 1}
	srv, errs := newServeErrSession(t, path, opts, passthroughHandlers(func(ctx context.Context, _ PushSink) error {
		<-ctx.Done()
		return nil
	}))

	conn1, _ := dial(t, wsURL(srv.URL, path))
	_ = conn1.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"same"}`))
	_ = readJSONFrame(t, conn1, 2*time.Second) // subscribed,conn1 一直占着 cap

	conn2, _ := dial(t, wsURL(srv.URL, path))
	_ = conn2.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"same"}`))
	_ = readJSONFrame(t, conn2, 2*time.Second) // error(429)

	// conn1 的 Run 阻塞到测试结束,先返回的必是 conn2 的 Serve。
	if err := awaitServeErr(t, errs); !errors.Is(err, ErrConnCapExceeded) {
		t.Fatalf("Serve error = %v, want 包装 ErrConnCapExceeded", err)
	}
}

// TestServeReturnsNilOnKick:Kick 是预期关闭,Serve 返回 nil。
func TestServeReturnsNilOnKick(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	sessCh := make(chan *Session, 1)
	h := passthroughHandlers(func(ctx context.Context, _ PushSink) error {
		<-ctx.Done()
		return nil
	})
	h.OnConnect = func(_ context.Context, s *Session) error {
		sessCh <- s
		return nil
	}
	srv, errs := newServeErrSession(t, path, Options{}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"x"}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	(<-sessCh).Kick(t.Context(), "bye")
	if err := awaitServeErr(t, errs); err != nil {
		t.Fatalf("Serve error = %v, want nil(Kick 不作为错误上抛)", err)
	}
}
