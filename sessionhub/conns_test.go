package sessionhub

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// fakeConn 是 Conn 的测试替身,记录调用。
type fakeConn struct {
	mu     sync.Mutex
	pushed []any
	kicked []string
}

func (f *fakeConn) Push(_ context.Context, payload any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pushed = append(f.pushed, payload)
	return nil
}

func (f *fakeConn) Kick(_ context.Context, reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.kicked = append(f.kicked, reason)
}

func TestRegisterConnAndConns(t *testing.T) {
	t.Parallel()
	hub := New()

	c1, c2 := &fakeConn{}, &fakeConn{}
	_, rel1 := hub.RegisterConn("u1", c1)
	_, rel2 := hub.RegisterConn("u1", c2)
	_, rel3 := hub.Register("u1") // 无句柄:计数计入,Conns 不含
	defer rel1()
	defer rel2()
	defer rel3()

	if n := hub.Count("u1"); n != 3 {
		t.Fatalf("Count = %d, want 3", n)
	}
	conns := hub.Conns("u1")
	if len(conns) != 2 {
		t.Fatalf("Conns len = %d, want 2(无句柄条目不枚举)", len(conns))
	}

	// 定向推送:两个句柄各收到一帧
	for _, c := range conns {
		if err := c.Push(t.Context(), "hello"); err != nil {
			t.Fatalf("Push error = %v", err)
		}
	}
	if len(c1.pushed) != 1 || len(c2.pushed) != 1 {
		t.Fatalf("pushed = %d/%d, want 1/1", len(c1.pushed), len(c2.pushed))
	}
}

func TestReleaseRemovesHandle(t *testing.T) {
	t.Parallel()
	hub := New()

	c1 := &fakeConn{}
	_, rel := hub.RegisterConn("u1", c1)

	if got := hub.Conns("u1"); len(got) != 1 {
		t.Fatalf("Conns len = %d, want 1", len(got))
	}
	rel()
	if got := hub.Conns("u1"); got != nil {
		t.Fatalf("Conns after release = %v, want nil", got)
	}
	if n := hub.Count("u1"); n != 0 {
		t.Fatalf("Count after release = %d, want 0", n)
	}
}

func TestConnsNilWhenOnlyMetadataEntries(t *testing.T) {
	t.Parallel()
	hub := New()
	_, rel := hub.Register("u1")
	defer rel()

	if got := hub.Conns("u1"); got != nil {
		t.Fatalf("Conns = %v, want nil(仅元数据条目)", got)
	}
}

// printConn 供 Example 演示,Kick 打印动作。
type printConn struct{ id string }

func (p *printConn) Push(context.Context, any) error { return nil }
func (p *printConn) Kick(_ context.Context, reason string) {
	fmt.Printf("kick %s: %s\n", p.id, reason)
}

// ExampleRegistry_RegisterConn 演示单点登录"新登录踢旧连接"模式:
// 先枚举并踢出同 userID 的旧连接,再注册自己;被踢连接结束时
// 由其自身的 defer release 注销(此处手动调用模拟)。
func ExampleRegistry_RegisterConn() {
	hub := New()

	// 旧设备在线
	_, oldRelease := hub.RegisterConn("user-1", &printConn{id: "old"})

	// 新设备登录:踢掉旧连接(锁外遍历快照),再注册自己
	for _, c := range hub.Conns("user-1") {
		c.Kick(context.Background(), "logged in elsewhere")
	}
	_, release := hub.RegisterConn("user-1", &printConn{id: "new"})
	defer release()

	oldRelease() // 被踢连接收敛后,其 defer release 生效
	fmt.Println("online:", hub.Count("user-1"))
	// Output:
	// kick old: logged in elsewhere
	// online: 1
}
