package wssession

import (
	"strings"
	"sync"
	"testing"
)

// uniqueKey 给每个测试一个独占 key,避免 connCounters 全局 map 串扰。
func uniqueKey(t *testing.T) string {
	t.Helper()
	return "test:" + strings.ReplaceAll(t.Name(), "/", "_")
}

func TestTryAcquire_Bounds(t *testing.T) {
	key := uniqueKey(t)

	// 0 → 1(允许)
	cur, ok := tryAcquire(key, 2)
	if !ok || cur != 1 {
		t.Fatalf("acq#1: cur=%d ok=%v, want 1+true", cur, ok)
	}

	// 1 → 2(允许)
	cur, ok = tryAcquire(key, 2)
	if !ok || cur != 2 {
		t.Fatalf("acq#2: cur=%d ok=%v, want 2+true", cur, ok)
	}

	// 2 → 3(超 max,拒;cur 显示 max+1 是回滚前的瞬时值)
	cur, ok = tryAcquire(key, 2)
	if ok {
		t.Fatalf("acq#3: ok=true, want false (over max)")
	}
	if cur != 3 {
		t.Fatalf("acq#3: cur=%d, want 3 (max+1 pre-rollback)", cur)
	}

	// 验证拒之后计数未净增,实际仍是 2
	if n := connCounters.count(key); n != 2 {
		t.Fatalf("after rejected acq: counter=%d, want 2 (failed attempt did not net-increase)", n)
	}

	// cleanup
	release(key)
	release(key)
}

func TestRelease_ThenAcquireSucceeds(t *testing.T) {
	key := uniqueKey(t)

	// 占满 max=1
	_, _ = tryAcquire(key, 1)
	if _, ok := tryAcquire(key, 1); ok {
		t.Fatal("expected 2nd acquire to fail")
	}

	// release 后立即 acquire 应成功
	release(key)
	if _, ok := tryAcquire(key, 1); !ok {
		t.Fatal("after release, next acquire should succeed")
	}

	release(key)
}

func TestTryAcquire_DifferentKeysIndependent(t *testing.T) {
	keyA := uniqueKey(t) + ":A"
	keyB := uniqueKey(t) + ":B"

	// A 占满 max=1
	_, _ = tryAcquire(keyA, 1)
	if _, ok := tryAcquire(keyA, 1); ok {
		t.Fatal("A 2nd should fail")
	}

	// B 应不受 A 影响
	if _, ok := tryAcquire(keyB, 1); !ok {
		t.Fatal("B 1st should succeed (independent of A)")
	}

	release(keyA)
	release(keyB)
}

func TestTryAcquire_ConcurrentEnterLeaveBalances(t *testing.T) {
	key := uniqueKey(t)
	const N = 1000
	var wg sync.WaitGroup

	for range N {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := tryAcquire(key, 100); ok {
				release(key)
			}
		}()
	}
	wg.Wait()

	if n := connCounters.count(key); n != 0 {
		t.Fatalf("after %d concurrent enter/leave: counter=%d, want 0 (no leak)", N, n)
	}
}
