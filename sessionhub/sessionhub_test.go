package sessionhub

import (
	"sync"
	"testing"
)

func TestRegisterUniqueConnIDs(t *testing.T) {
	t.Parallel()
	r := New()
	id1, rel1 := r.Register("u1")
	id2, rel2 := r.Register("u1")
	if id1 == id2 {
		t.Fatalf("connID 应唯一,得到相同: %s", id1)
	}
	if n := r.Count("u1"); n != 2 {
		t.Fatalf("Count(u1) = %d, want 2", n)
	}
	rel1()
	rel2()
}

func TestReleaseIdempotent(t *testing.T) {
	t.Parallel()
	r := New()
	_, release := r.Register("u1")
	if r.Count("u1") != 1 {
		t.Fatal("注册后应为 1")
	}
	release()
	release() // 重复调用
	release()
	if n := r.Count("u1"); n != 0 {
		t.Fatalf("release 后 Count = %d, want 0", n)
	}
	if got := r.List("u1"); got != nil {
		t.Fatalf("release 后 List 应为 nil,得 %v", got)
	}
}

func TestEnumerate(t *testing.T) {
	t.Parallel()
	r := New()
	_, ra := r.Register("alice")
	_, rb := r.Register("alice")
	_, rc := r.Register("bob")
	defer func() { ra(); rb(); rc() }()

	if n := r.Count("alice"); n != 2 {
		t.Fatalf("alice Count = %d, want 2", n)
	}
	list := r.List("alice")
	if len(list) != 2 {
		t.Fatalf("alice List len = %d, want 2", len(list))
	}
	for _, e := range list {
		if e.UserID != "alice" || e.ConnID == "" || e.ConnectedAt.IsZero() {
			t.Fatalf("Entry 字段不完整: %+v", e)
		}
	}
	if r.Total() != 3 {
		t.Fatalf("Total = %d, want 3", r.Total())
	}
	if len(r.Users()) != 2 {
		t.Fatalf("Users len = %d, want 2", len(r.Users()))
	}
}

func TestListReturnsCopy(t *testing.T) {
	t.Parallel()
	r := New()
	_, rel := r.Register("u1")
	defer rel()
	list := r.List("u1")
	list[0].UserID = "tampered" // 改返回的副本
	if again := r.List("u1"); again[0].UserID != "u1" {
		t.Fatal("List 返回的应是独立副本,内部被篡改了")
	}
}

func TestConcurrentRegisterRelease(t *testing.T) {
	t.Parallel()
	r := New()
	const N = 500
	var wg sync.WaitGroup
	for range N {
		wg.Go(func() {
			_, release := r.Register("u1")
			release()
		})
	}
	wg.Wait()
	if n := r.Count("u1"); n != 0 {
		t.Fatalf("并发注册注销后 Count = %d, want 0(无泄漏)", n)
	}
	if r.Total() != 0 {
		t.Fatalf("Total = %d, want 0", r.Total())
	}
}
