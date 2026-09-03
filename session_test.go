package wssession

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	gtkitjson "github.com/gtkit/json/v2"
)

// ============================================================================
// Test helpers
// ============================================================================

// uniquePath 给每个测试一个独占 path,避免 connCap 计数器串扰。
func uniquePath(t *testing.T) string {
	t.Helper()
	return "/test/" + strings.ReplaceAll(t.Name(), "/", "_")
}

// newTestSession 启一个 httptest server,把 wssession.Serve 挂在 path 上。
func newTestSession(t *testing.T, path string, opts Options, handlers Handlers) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		_ = Serve(r.Context(), w, r, opts, handlers)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// wsURL 把 http://...  转 ws://... + 拼 path.
func wsURL(srvURL, path string) string {
	return "ws" + strings.TrimPrefix(srvURL, "http") + path
}

// dial 用 gorilla 客户端建连,返回 conn + HTTP 响应。
func dial(t *testing.T, url string) (*websocket.Conn, *http.Response) {
	t.Helper()
	dialer := &websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	conn, resp, err := dialer.Dial(url, nil)
	if err != nil {
		var code int
		if resp != nil {
			code = resp.StatusCode
		}
		t.Fatalf("dial: %v (http=%d url=%s)", err, code, url)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, resp
}

// dialExpectingFailure 期望握手失败,返回 HTTP 响应。
func dialExpectingFailure(t *testing.T, url string) *http.Response {
	t.Helper()
	dialer := &websocket.Dialer{HandshakeTimeout: 3 * time.Second}
	conn, resp, err := dialer.Dial(url, nil)
	if err == nil {
		_ = conn.Close()
		t.Fatal("expected dial fail, got success")
	}
	if resp == nil {
		t.Fatalf("expected HTTP response on dial fail, err=%v", err)
	}
	return resp
}

// readJSONFrame 读一帧 JSON 文本帧 → map[string]any。
func readJSONFrame(t *testing.T, conn *websocket.Conn, timeout time.Duration) map[string]any {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	var payload map[string]any
	if err := conn.ReadJSON(&payload); err != nil {
		t.Fatalf("ReadJSON: %v", err)
	}
	return payload
}

// passthroughHandlers 简单回放:ParseRequest 直接把 token 字段当 key,Run 阻塞到 ctx 取消。
func passthroughHandlers(runHook func(ctx context.Context, sink PushSink) error) Handlers {
	return Handlers{
		ParseRequest: func(_ context.Context, raw []byte) (string, any, error) {
			var msg struct {
				Action string `json:"action"`
				Token  string `json:"token"`
			}
			if err := gtkitjson.Unmarshal(raw, &msg); err != nil {
				return "", nil, fmt.Errorf("bad json: %w", err)
			}
			if msg.Action != "subscribe" {
				return "", nil, fmt.Errorf("action must be subscribe, got %q", msg.Action)
			}
			if msg.Token == "" {
				return "", nil, fmt.Errorf("token required")
			}
			return msg.Token, msg.Token, nil
		},
		Run: func(ctx context.Context, req any, sink PushSink) error {
			if runHook != nil {
				return runHook(ctx, sink)
			}
			<-ctx.Done()
			return nil
		},
	}
}

// ============================================================================
// 6.2 首帧超时
// ============================================================================

func TestFirstFrameTimeout(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	opts := Options{FirstFrameTimeout: 200 * time.Millisecond}
	srv := newTestSession(t, path, opts, passthroughHandlers(nil))

	conn, _ := dial(t, wsURL(srv.URL, path))
	// 不发任何帧,等服务端下发 error 帧
	msg := readJSONFrame(t, conn, 2*time.Second)
	if msg["event"] != "error" {
		t.Fatalf("event = %v, want error", msg["event"])
	}
	if code, _ := msg["code"].(float64); int(code) != CodeFirstFrameTimeout {
		t.Fatalf("code = %v, want %d", msg["code"], CodeFirstFrameTimeout)
	}
}

// ============================================================================
// 6.3 首帧及时到达 → 收到 subscribed + Run 阻塞
// ============================================================================

func TestFirstFrameNormalSubscribe(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	runCalled := make(chan struct{})
	handlers := passthroughHandlers(func(ctx context.Context, sink PushSink) error {
		close(runCalled)
		<-ctx.Done()
		return nil
	})
	srv := newTestSession(t, path, Options{}, handlers)

	conn, _ := dial(t, wsURL(srv.URL, path))
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"abc"}`)); err != nil {
		t.Fatalf("write: %v", err)
	}

	msg := readJSONFrame(t, conn, 2*time.Second)
	if msg["event"] != "subscribed" {
		t.Fatalf("event = %v, want subscribed", msg["event"])
	}
	if _, ok := msg["timestamp"].(string); !ok {
		t.Fatalf("subscribed frame missing timestamp: %+v", msg)
	}

	select {
	case <-runCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("Run was not invoked")
	}
}

// ============================================================================
// 6.4 帧大小超 ReadLimit
// ============================================================================

func TestReadLimitExceeded(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	opts := Options{ReadLimit: 100}
	srv := newTestSession(t, path, opts, passthroughHandlers(nil))

	conn, _ := dial(t, wsURL(srv.URL, path))
	// 发 200 字节文本帧,超过 ReadLimit=100
	big := strings.Repeat("a", 200)
	_ = conn.WriteMessage(websocket.TextMessage, []byte(big))

	// 服务端应 close 连接(gorilla 客户端读会返回错误)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := conn.ReadMessage()
	if err == nil {
		t.Fatal("expected read error after server closes oversized frame")
	}
}

// ============================================================================
// 6.5 BinaryMessage 被拒
// ============================================================================

func TestBinaryFrameRejected(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	srv := newTestSession(t, path, Options{}, passthroughHandlers(nil))

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.BinaryMessage, []byte("binary content"))

	msg := readJSONFrame(t, conn, 2*time.Second)
	if msg["event"] != "error" {
		t.Fatalf("event = %v, want error", msg["event"])
	}
	if code, _ := msg["code"].(float64); int(code) != CodeInvalidFrameType {
		t.Fatalf("code = %v, want %d", msg["code"], CodeInvalidFrameType)
	}
}

// ============================================================================
// 6.6 ParseRequest 返回 error
// ============================================================================

func TestParseRequestError(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	srv := newTestSession(t, path, Options{}, passthroughHandlers(nil))

	conn, _ := dial(t, wsURL(srv.URL, path))
	// 发空 token → ParseRequest 返回 "token required"
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":""}`))

	msg := readJSONFrame(t, conn, 2*time.Second)
	if msg["event"] != "error" {
		t.Fatalf("event = %v, want error", msg["event"])
	}
	if code, _ := msg["code"].(float64); int(code) != CodeInvalidParam {
		t.Fatalf("code = %v, want %d", msg["code"], CodeInvalidParam)
	}
}

// ============================================================================
// 6.7 Run 返回 error
// ============================================================================

func TestRunError(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	handlers := passthroughHandlers(func(ctx context.Context, sink PushSink) error {
		return errors.New("business failure: dsn=mysql://secret@db")
	})
	srv := newTestSession(t, path, Options{}, handlers)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"x"}`))

	// 第 1 帧 subscribed
	first := readJSONFrame(t, conn, 2*time.Second)
	if first["event"] != "subscribed" {
		t.Fatalf("first = %v, want subscribed", first["event"])
	}
	// 第 2 帧 error (500)
	second := readJSONFrame(t, conn, 2*time.Second)
	if second["event"] != "error" {
		t.Fatalf("second = %v, want error", second["event"])
	}
	if code, _ := second["code"].(float64); int(code) != CodeInternal {
		t.Fatalf("code = %v, want %d", second["code"], CodeInternal)
	}
	if reason, _ := second["reason"].(string); reason != ReasonInternalError {
		t.Fatalf("reason = %q, want %q", reason, ReasonInternalError)
	}
}

// ============================================================================
// 6.8 已订阅后再发业务帧 → close
// ============================================================================

func TestSubscribedFrameAfterSubscribe(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	handlers := passthroughHandlers(func(ctx context.Context, sink PushSink) error {
		<-ctx.Done()
		return nil
	})
	srv := newTestSession(t, path, Options{}, handlers)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"x"}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed 帧

	// 再发一帧业务消息
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"foo"}`))

	// 服务端应下发 error(422) 后 close
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		mt, data, err := conn.ReadMessage()
		if err != nil {
			return // close 帧导致 read err,符合预期
		}
		if mt != websocket.TextMessage {
			continue
		}
		var m map[string]any
		_ = gtkitjson.Unmarshal(data, &m)
		if m["event"] == "error" {
			return // 收到 error 后等下次 read 返回 close 错误也行
		}
	}
}

// ============================================================================
// 6.9 慢消费者 → ErrSlowConsumer
//
// 用 unit-level 测试直接测 Session.queue:end-to-end 测试因 TCP send buffer
// 默认很大(64KB+)难以稳定填满,无 buffer channel 直接测语义最可靠。
// ============================================================================

func TestQueueSlowConsumer(t *testing.T) {
	t.Parallel()
	s := &Session{
		// 无缓冲 outbox,永不消费 → 第一次 send 就会进 timer
		outbox:  make(chan outboundMessage),
		options: Options{QueueOfferTimeout: 150 * time.Millisecond},
	}

	start := time.Now()
	err := s.queue(t.Context(), outboundMessage{
		messageType: websocket.TextMessage,
		data:        []byte(`{"x":1}`),
	})
	elapsed := time.Since(start)

	if !errors.Is(err, ErrSlowConsumer) {
		t.Fatalf("err = %v, want ErrSlowConsumer", err)
	}
	if elapsed < 100*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("elapsed = %v, want ~150ms", elapsed)
	}
}

func TestQueueCtxCancelDuringWait(t *testing.T) {
	t.Parallel()
	s := &Session{
		outbox:  make(chan outboundMessage),
		options: Options{QueueOfferTimeout: 5 * time.Second},
	}

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := s.queue(ctx, outboundMessage{messageType: websocket.TextMessage, data: []byte(`"x"`)})
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("elapsed = %v, ctx cancel should return immediately", elapsed)
	}
}

func TestCloseWithErrorUsesShortQueueTimeout(t *testing.T) {
	t.Parallel()
	s := &Session{
		outbox: make(chan outboundMessage),
		options: Options{
			QueueOfferTimeout: 5 * time.Second,
		},
	}

	start := time.Now()
	s.closeWithError(t.Context(), CodeTooManyConn, "slow consumer")
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Fatalf("closeWithError elapsed = %v, want short timeout under 1s", elapsed)
	}
}

func TestCloseWithErrorTruncatesLongReason(t *testing.T) {
	t.Parallel()
	s := &Session{
		outbox: make(chan outboundMessage, 1),
		options: Options{
			QueueOfferTimeout: time.Second,
		},
	}

	captured := make(chan errorFrame, 1)
	go func() {
		msg := <-s.outbox // error 帧(done 为 nil)
		var f errorFrame
		_ = gtkitjson.Unmarshal(msg.data, &f)
		captured <- f
		closeMsg := <-s.outbox // close 握手帧,done 挂在这一帧上
		close(closeMsg.done)
	}()

	s.closeWithError(t.Context(), CodeInvalidParam, strings.Repeat("x", maxErrorReasonLen+32))

	select {
	case frame := <-captured:
		if len(frame.Reason) != maxErrorReasonLen {
			t.Fatalf("reason len = %d, want %d", len(frame.Reason), maxErrorReasonLen)
		}
	case <-time.After(time.Second):
		t.Fatal("error frame was not queued")
	}
}

// ============================================================================
// 6.10 panic recovery
// ============================================================================

func TestPanicRecovery(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	handlers := passthroughHandlers(func(ctx context.Context, sink PushSink) error {
		panic("intentional panic for recovery test")
	})
	srv := newTestSession(t, path, Options{}, handlers)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"x"}`))

	// 不应该让进程 crash;服务端会 close 连接(panic 被 processLoop defer recover catch)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for range 5 {
		_, _, err := conn.ReadMessage()
		if err != nil {
			return // close 帧导致 read err,符合预期
		}
	}
}

// ============================================================================
// 6.11 OnConnect hook 被调用
// ============================================================================

func TestOnConnectCalled(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)

	var onConnectCalled atomic.Bool
	handlers := passthroughHandlers(nil)
	handlers.OnConnect = func(_ context.Context, sess *Session) error {
		onConnectCalled.Store(true)
		if sess == nil {
			return errors.New("sess is nil")
		}
		return nil
	}

	srv := newTestSession(t, path, Options{}, handlers)
	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"x"}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	if !onConnectCalled.Load() {
		t.Fatal("OnConnect was not called")
	}
}

func TestOnConnectErrorClosesConn(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)

	handlers := passthroughHandlers(nil)
	handlers.OnConnect = func(_ context.Context, sess *Session) error {
		return errors.New("connect rejected")
	}

	srv := newTestSession(t, path, Options{}, handlers)
	conn, _ := dial(t, wsURL(srv.URL, path))

	// 服务端应在收到 subscribe 之前直接 close 连接
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := conn.ReadMessage()
	if err == nil {
		t.Fatal("expected conn close after OnConnect error")
	}
}

// ============================================================================
// 6.12 readLoop 不被慢 ParseRequest 阻塞(间接验证:客户端 close 后服务端 ctx 应及时取消)
// ============================================================================

func TestReadLoopNotBlockedByParseRequest(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)

	parseStarted := make(chan struct{})
	parseDone := make(chan struct{})
	var ctxAtParse atomic.Pointer[context.Context]

	handlers := Handlers{
		ParseRequest: func(ctx context.Context, raw []byte) (string, any, error) {
			ctxAtParse.Store(&ctx)
			close(parseStarted)
			<-parseDone
			return "", nil, errors.New("parse done")
		},
		Run: func(_ context.Context, _ any, _ PushSink) error { return nil },
	}

	srv := newTestSession(t, path, Options{}, handlers)
	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{}`))

	<-parseStarted

	// ParseRequest 仍 block 中;readLoop 应该独立 goroutine,客户端 close 后应及时识别
	_ = conn.Close()

	// 验证:ctx 在客户端 close 后 2s 内取消
	deadline := time.After(2 * time.Second)
	check := time.NewTicker(50 * time.Millisecond)
	defer check.Stop()
	for {
		select {
		case <-deadline:
			close(parseDone) // 解锁让测试退出
			t.Fatal("ctx did not cancel within 2s after client close (readLoop blocked?)")
		case <-check.C:
			ctxPtr := ctxAtParse.Load()
			if ctxPtr != nil {
				select {
				case <-(*ctxPtr).Done():
					close(parseDone)
					return
				default:
				}
			}
		}
	}
}

// ============================================================================
// 6.13 MaxSessionDuration 绝对超时
// ============================================================================

func TestMaxSessionDuration(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)

	runReturned := make(chan struct{})
	handlers := passthroughHandlers(func(ctx context.Context, sink PushSink) error {
		<-ctx.Done()
		close(runReturned)
		return nil
	})
	opts := Options{MaxSessionDuration: 500 * time.Millisecond}
	srv := newTestSession(t, path, opts, handlers)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"x"}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	select {
	case <-runReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s after MaxSessionDuration (500ms)")
	}
}

// ============================================================================
// IP cap 在 Upgrade 前拒(HTTP 429,不进入 WS 协议层)
// ============================================================================

func TestConnCapIP_BeforeUpgrade(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)

	opts := Options{
		ConnCapEnabled: true,
		ConnCapIPMax:   1,
		ConnCapKeyMax:  10,
	}
	handlers := passthroughHandlers(func(ctx context.Context, sink PushSink) error {
		<-ctx.Done()
		return nil
	})
	srv := newTestSession(t, path, opts, handlers)

	// 第 1 条连接占住 IP cap
	conn1, _ := dial(t, wsURL(srv.URL, path))
	_ = conn1.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"x"}`))
	_ = readJSONFrame(t, conn1, 2*time.Second) // subscribed

	// 第 2 条应被 HTTP 429 拒,不进入 WS
	resp := dialExpectingFailure(t, wsURL(srv.URL, path))
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("conn2 status = %d, want 429", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}

// ============================================================================
// Token cap 在 ParseRequest 后拒(下发 error 帧 + close)
// ============================================================================

func TestConnCapToken_RejectAfterParseRequest(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)

	opts := Options{
		ConnCapEnabled: true,
		ConnCapIPMax:   99,
		ConnCapKeyMax:  1,
	}
	handlers := passthroughHandlers(func(ctx context.Context, sink PushSink) error {
		<-ctx.Done()
		return nil
	})
	srv := newTestSession(t, path, opts, handlers)

	// 第 1 条连接占住 token cap
	conn1, _ := dial(t, wsURL(srv.URL, path))
	_ = conn1.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"shared-tok"}`))
	_ = readJSONFrame(t, conn1, 2*time.Second) // subscribed

	// 第 2 条 Upgrade 成功 + 发 subscribe → 收到 error(429)
	conn2, _ := dial(t, wsURL(srv.URL, path))
	_ = conn2.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"shared-tok"}`))
	msg := readJSONFrame(t, conn2, 2*time.Second)
	if msg["event"] != "error" {
		t.Fatalf("event = %v, want error", msg["event"])
	}
	if code, _ := msg["code"].(float64); int(code) != CodeTooManyConn {
		t.Fatalf("code = %v, want %d", msg["code"], CodeTooManyConn)
	}
}

// ============================================================================
// Origin 校验
// ============================================================================

func TestOriginCheckerRejectsEvil(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	opts := Options{AllowedOrigins: []string{"https://allowed.example.com"}}
	srv := newTestSession(t, path, opts, passthroughHandlers(nil))

	header := http.Header{}
	header.Set("Origin", "https://evil.example.com")
	dialer := &websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	_, resp, err := dialer.Dial(wsURL(srv.URL, path), header)
	if err == nil {
		t.Fatal("expected dial fail on bad origin")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		var code int
		if resp != nil {
			code = resp.StatusCode
		}
		t.Fatalf("status = %d, want 403", code)
	}
}

// ============================================================================
// Handlers 校验:ParseRequest 或 Run 为 nil 时 Serve 立即拒
// ============================================================================

func TestServeRejectsIncompleteHandlers(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	// ParseRequest 缺
	srv := newTestSession(t, path, Options{}, Handlers{
		Run: func(_ context.Context, _ any, _ PushSink) error { return nil },
	})

	// Upgrade 应失败(Serve 在 validate 阶段直接返回 error,gorilla 内部 Upgrade 没机会写 HTTP 101)
	dialer := &websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	conn, _, err := dialer.Dial(wsURL(srv.URL, path), nil)
	if err == nil {
		_ = conn.Close()
		t.Fatal("expected dial fail with incomplete handlers")
	}
}

// ============================================================================
// 并发多连接 enter/leave 计数归零
// ============================================================================

func TestConcurrentSessionsCounterBalances(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	opts := Options{
		ConnCapEnabled: true,
		ConnCapIPMax:   100,
		ConnCapKeyMax:  100,
	}
	handlers := passthroughHandlers(func(ctx context.Context, sink PushSink) error {
		<-ctx.Done()
		return nil
	})
	srv := newTestSession(t, path, opts, handlers)

	const N = 20
	var wg sync.WaitGroup
	for i := range N {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, _ := dial(t, wsURL(srv.URL, path))
			payload := fmt.Sprintf(`{"action":"subscribe","token":"tok-%d"}`, i)
			_ = conn.WriteMessage(websocket.TextMessage, []byte(payload))
			_ = readJSONFrame(t, conn, 2*time.Second)
			_ = conn.Close()
		}(i)
	}
	wg.Wait()

	// 等服务端 cleanup
	time.Sleep(200 * time.Millisecond)

	// 此 path 上每个 token key 都应归零(归零即被删除,count 返回 0)。
	for i := range N {
		key := fmt.Sprintf("token:tok-%d:%s", i, path)
		if n := connCounters.count(key); n != 0 {
			t.Errorf("counter %s = %d, want 0", key, n)
		}
	}
}
