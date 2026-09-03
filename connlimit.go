package wssession

import (
	"maps"
	"sync"
)

// keyedCounter 是按 key 计数的活跃连接表,计数归零时**删除 key 条目**,
// 保证长期运行时占用与当前活跃 key 数成正比,而非历史出现过的 key 总数。
//
// 用分片 mutex + map 实现:每片自带锁守护一个 map,"自增/自减/归零删除"
// 在同一临界区内原子完成。这避免了 sync.Map 上"减到 0 再 Delete"与并发
// LoadOrStore 之间的 TOCTOU(漏删导致内存泄漏 / 误删导致计数丢失)。
//
// 分片仅为降低锁竞争——cap 路径每条连接至多调用两次(Upgrade 前 + 首帧后),
// 竞争本不高,分片是低成本保险,故分片数固定不暴露为配置项。
type keyedCounter struct {
	shards [connCounterShards]struct {
		mu     sync.Mutex
		counts map[string]int64
	}
}

const connCounterShards = 32

func newKeyedCounter() *keyedCounter {
	kc := &keyedCounter{}
	for i := range kc.shards {
		kc.shards[i].counts = make(map[string]int64)
	}
	return kc
}

// shardIndex 用 FNV-1a 对 key 散列定位分片(手写避免 hash/fnv 的堆分配)。
func shardIndex(key string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := range len(key) {
		h ^= uint32(key[i])
		h *= prime32
	}
	return h % connCounterShards
}

// acquire 原子自增 key 计数;超过 maxConcurrent 则不计入并返回 ok=false。
//
// 返回的 current 为"若放行后"的瞬时计数:放行时 = 实际占用数(≤max),
// 被拒时 = max+1(未真正写入 map)。仅用于日志。
func (kc *keyedCounter) acquire(key string, maxConcurrent int) (current int64, ok bool) {
	s := &kc.shards[shardIndex(key)]
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.counts[key] + 1
	if n > int64(maxConcurrent) {
		// 不写入 map:失败的尝试不产生条目、不净增计数。
		return n, false
	}
	s.counts[key] = n
	return n, true
}

// release 原子自减 key 计数,归零时删除条目。
//
// 应一一对应 acquire 的**成功**路径;acquire 失败不应再调 release。
func (kc *keyedCounter) release(key string) {
	s := &kc.shards[shardIndex(key)]
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.counts[key] - 1
	if n <= 0 {
		delete(s.counts, key)
		return
	}
	s.counts[key] = n
}

// count 返回 key 当前计数(条目不存在时为 0)。仅供包内测试断言使用。
func (kc *keyedCounter) count(key string) int64 {
	s := &kc.shards[shardIndex(key)]
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[key]
}

// snapshot 返回所有活跃 key 及其计数的独立副本。
func (kc *keyedCounter) snapshot() map[string]int64 {
	out := make(map[string]int64)
	for i := range kc.shards {
		s := &kc.shards[i]
		s.mu.Lock()
		maps.Copy(out, s.counts)
		s.mu.Unlock()
	}
	return out
}

// connCounters 是 wssession 包级独立的连接计数器注册表。
//
// 与其它子系统(如 SSE 路径)的 connCap **不共享**——保证各路径连接 cap 隔离。
//
// key 形态:
//   - IP 维度:"ip:" + client_ip + ":" + path
//   - token 维度:"token:" + key + ":" + path(key 由 Handlers.ParseRequest 返回)
var connCounters = newKeyedCounter()

// tryAcquire 原子自增 key 对应的活跃连接计数(见 keyedCounter.acquire)。
func tryAcquire(key string, maxConcurrent int) (current int64, ok bool) {
	return connCounters.acquire(key, maxConcurrent)
}

// release 原子自减 key 对应的活跃连接计数(见 keyedCounter.release)。
func release(key string) {
	connCounters.release(key)
}

// ConnCapSnapshot 返回调用时刻所有活跃 cap key 及其连接数的快照。
//
// 返回值是独立副本,调用方可自由读取/修改而不影响内部计数;
// 已归零删除的 key 不会出现。供 metrics / 运维查询使用。
//
// key 形态:"ip:<client_ip>:<path>" / "token:<key>:<path>"。
func ConnCapSnapshot() map[string]int64 {
	return connCounters.snapshot()
}
