package wssession

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// deadlineHandlers 构造一个 Run 阻塞到会话结束的 Handlers,并把会话 ctx 的
// 结束原因回传给测试。
func deadlineHandlers(sess *atomic.Pointer[Session], ended chan<- error) Handlers {
	h := passthroughHandlers(func(ctx context.Context, _ PushSink) error {
		<-ctx.Done()
		ended <- context.Cause(ctx)
		return ctx.Err()
	})
	h.OnConnect = func(_ context.Context, s *Session) error {
		sess.Store(s)
		return nil
	}
	return h
}

// TestExtendDeadlineKeepsSessionAlive:续期后会话活过原 MaxSessionDuration。
func TestExtendDeadlineKeepsSessionAlive(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	var sess atomic.Pointer[Session]
	ended := make(chan error, 1)

	opts := Options{
		MaxSessionDuration:        200 * time.Millisecond,
		SessionDeadlineExtendable: true,
	}
	srv := newTestSession(t, path, opts, deadlineHandlers(&sess, ended))

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"t"}`))
	_ = readJSONFrame(t, conn, 2*time.Second) // subscribed

	// 在原 200ms 截止点之前续期到 2s
	time.Sleep(80 * time.Millisecond)
	if err := sess.Load().ExtendDeadline(2 * time.Second); err != nil {
		t.Fatalf("ExtendDeadline error = %v", err)
	}

	// 越过原截止点后连接仍应存活:此时不应收到会话结束
	select {
	case cause := <-ended:
		t.Fatalf("续期后会话仍在原截止点结束: cause=%v", cause)
	case <-time.After(400 * time.Millisecond):
	}

	// 连接确实还能收发
	if err := sess.Load().Push(t.Context(), map[string]any{"alive": true}); err != nil {
		t.Fatalf("续期后 Push error = %v", err)
	}
	if got := readJSONFrame(t, conn, 2*time.Second); got["alive"] != true {
		t.Fatalf("续期后收到 %v, want alive=true", got)
	}
}

// TestSessionExpiresWithoutExtension:开启开关但不续期时,仍按原上限收敛,
// 且到期原因可经 context.Cause 判别。
func TestSessionExpiresWithoutExtension(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	var sess atomic.Pointer[Session]
	ended := make(chan error, 1)

	opts := Options{
		MaxSessionDuration:        150 * time.Millisecond,
		SessionDeadlineExtendable: true,
	}
	srv := newTestSession(t, path, opts, deadlineHandlers(&sess, ended))

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"t"}`))
	_ = readJSONFrame(t, conn, 2*time.Second)

	select {
	case cause := <-ended:
		if !errors.Is(cause, context.DeadlineExceeded) {
			t.Fatalf("context.Cause = %v, want context.DeadlineExceeded", cause)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("3s 内会话未按 MaxSessionDuration 到期")
	}
}

// TestDefaultDeadlineKeepsDeadlineExceededSemantics:未开启开关时会话 ctx 保持
// 固定 deadline 语义(ctx.Err() 为 DeadlineExceeded、ctx.Deadline() 可读)。
func TestDefaultDeadlineKeepsDeadlineExceededSemantics(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	type ctxInfo struct {
		err         error
		hasDeadline bool
	}
	info := make(chan ctxInfo, 1)

	h := passthroughHandlers(func(ctx context.Context, _ PushSink) error {
		_, hasDeadline := ctx.Deadline()
		<-ctx.Done()
		info <- ctxInfo{err: ctx.Err(), hasDeadline: hasDeadline}
		return ctx.Err()
	})
	srv := newTestSession(t, path, Options{MaxSessionDuration: 150 * time.Millisecond}, h)

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"t"}`))
	_ = readJSONFrame(t, conn, 2*time.Second)

	select {
	case got := <-info:
		if !errors.Is(got.err, context.DeadlineExceeded) {
			t.Fatalf("ctx.Err() = %v, want context.DeadlineExceeded", got.err)
		}
		if !got.hasDeadline {
			t.Fatal("默认模式下 ctx.Deadline() 应可读")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("3s 内会话未到期")
	}
}

// TestExtendDeadlineRejectedWithoutOptIn:未开启开关时调用被显式拒绝,
// 且会话仍在原上限到期。
func TestExtendDeadlineRejectedWithoutOptIn(t *testing.T) {
	t.Parallel()
	path := uniquePath(t)
	var sess atomic.Pointer[Session]
	ended := make(chan error, 1)

	opts := Options{MaxSessionDuration: 200 * time.Millisecond} // 未开启续期
	srv := newTestSession(t, path, opts, deadlineHandlers(&sess, ended))

	conn, _ := dial(t, wsURL(srv.URL, path))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"action":"subscribe","token":"t"}`))
	_ = readJSONFrame(t, conn, 2*time.Second)

	err := sess.Load().ExtendDeadline(time.Hour)
	if !errors.Is(err, ErrDeadlineNotExtendable) {
		t.Fatalf("ExtendDeadline error = %v, want ErrDeadlineNotExtendable", err)
	}

	// 被拒的续期不得改变截止时间:会话仍按 200ms 到期
	select {
	case <-ended:
	case <-time.After(3 * time.Second):
		t.Fatal("被拒的续期不应延长会话,但 3s 内会话未到期")
	}
}

// TestExtendDeadlineRejectsNonPositive:非法时长被拒。
func TestExtendDeadlineRejectsNonPositive(t *testing.T) {
	t.Parallel()
	s := &Session{}
	for _, d := range []time.Duration{0, -time.Second} {
		if err := s.ExtendDeadline(d); err == nil {
			t.Fatalf("ExtendDeadline(%v) 应返回 error", d)
		} else if errors.Is(err, ErrDeadlineNotExtendable) {
			t.Fatalf("ExtendDeadline(%v) 应先校验时长,而非返回 ErrDeadlineNotExtendable", d)
		}
	}
}

// TestExtendDeadlineOnClosedSession:已收敛的连接不再被续期复活。
func TestExtendDeadlineOnClosedSession(t *testing.T) {
	t.Parallel()
	s := &Session{
		options: Options{SessionDeadlineExtendable: true, MaxSessionDuration: time.Hour},
	}
	ctx, cancel := s.newSessionContext(context.Background())
	defer cancel()
	_ = ctx

	if err := s.ExtendDeadline(time.Minute); err != nil {
		t.Fatalf("活跃会话 ExtendDeadline error = %v", err)
	}

	_ = s.Close() // wsConn 为 nil,只翻转状态
	err := s.ExtendDeadline(time.Minute)
	if err == nil {
		t.Fatal("已收敛连接上的 ExtendDeadline 应返回 error")
	}
	if errors.Is(err, ErrDeadlineNotExtendable) {
		t.Fatalf("error = %v, want 已关闭错误而非 ErrDeadlineNotExtendable", err)
	}
}

// TestExtendDeadlineConcurrent:并发续期无数据竞态(-race 下验证)。
func TestExtendDeadlineConcurrent(t *testing.T) {
	t.Parallel()
	s := &Session{
		options: Options{SessionDeadlineExtendable: true, MaxSessionDuration: time.Hour},
	}
	_, cancel := s.newSessionContext(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 20 {
				if err := s.ExtendDeadline(time.Hour); err != nil {
					t.Errorf("ExtendDeadline error = %v", err)
					return
				}
			}
		})
	}
	wg.Wait()
}

// TestExtendDeadlineRejectedAfterParentCancel:会话 ctx 已取消(停机 / 上游取消)后,
// 续期必须显式失败——此前只看 closed,而 Close 由 ctx watcher 在之后才调用,
// 中间窗口里续期会误报成功,业务会把"会话继续"回给客户端。
func TestExtendDeadlineRejectedAfterParentCancel(t *testing.T) {
	t.Parallel()
	s := &Session{
		options: Options{SessionDeadlineExtendable: true, MaxSessionDuration: time.Hour},
	}
	parent, cancelParent := context.WithCancel(context.Background())
	_, cancel := s.newSessionContext(parent)
	defer cancel()

	if err := s.ExtendDeadline(time.Minute); err != nil {
		t.Fatalf("活跃会话续期 error = %v", err)
	}

	cancelParent() // 模拟停机 / 上游取消:此刻 Close 还没跑,closed 仍为 false
	if s.IsClosed() {
		t.Skip("Close 已先跑完,本用例要测的窗口不存在")
	}
	err := s.ExtendDeadline(time.Minute)
	if err == nil {
		t.Fatal("会话 ctx 已取消后续期应返回 error,不能误报成功")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want 包装 context.Canceled", err)
	}
}
