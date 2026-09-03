// Package sessionhub 提供一个可选、轻量的连接注册表,用于**管理**同一 userID
// 的多个并发连接(多设备 / 多标签页):枚举元数据、定向推送、踢下线。
//
// 它独立于 wssession 核心包:不 import wssession,连接操作经 Conn 接口注入
// (*wssession.Session 结构性满足)。集成模式利用 wssession.Serve 是阻塞调用——
// 业务在 handler 里注册并 defer 注销:
//
//	connID, release := hub.RegisterConn(userID, sess) // sess 来自 OnConnect
//	defer release()
//	_ = wssession.Serve(ctx, w, r, opts, handlers) // 阻塞至连接结束 → defer 注销
//
// 若 userID 来自首帧(由 Handlers.ParseRequest 解析),用闭包把 release 从 ParseRequest
// 回传到 handler 的 defer 即可。单点登录踢旧连接的完整模式见 Example。
//
// 单点登录策略(谁踢谁、允许几端)由业务决定,本包保证注册 / 注销 /
// 枚举 / 句柄操作的并发安全。
package sessionhub

import (
	"context"
	"maps"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Conn 是注册表可选持有的连接操作句柄。
//
// *wssession.Session 结构性满足本接口(Push 推帧 / Kick 踢下线),本包与
// wssession 零 import 依赖,业务也可注入自定义实现(如测试 fake)。
//
// 句柄方法必须可在连接已收敛后安全调用(Push 返回错误、Kick 无害幂等)——
// wssession.Session 满足该约定。
type Conn interface {
	Push(ctx context.Context, payload any) error
	Kick(ctx context.Context, reason string)
}

// Entry 是一个活跃连接的元数据快照。
type Entry struct {
	ConnID      string
	UserID      string
	ConnectedAt time.Time
}

// record 是注册表内部存储单元:元数据 + 可选连接句柄。
type record struct {
	entry Entry
	conn  Conn // 可为 nil(Register 注册的纯元数据条目)
}

// Registry 是按 userID 索引的活跃连接注册表,并发安全。
//
// 零值不可用,请用 New 创建。
type Registry struct {
	mu     sync.RWMutex
	seq    atomic.Uint64
	byUser map[string]map[string]record // userID → connID → record
	now    func() time.Time             // 便于测试注入;默认 time.Now
}

// New 创建一个空 Registry。
func New() *Registry {
	return &Registry{
		byUser: make(map[string]map[string]record),
		now:    time.Now,
	}
}

// Register 登记一个属于 userID 的活跃连接(仅元数据,无操作句柄),
// 返回唯一 connID 与注销函数 release。需要定向推送 / 踢下线用 RegisterConn。
//
// 调用方应在连接结束时调用 release(通常 `defer release()`)。release 幂等,
// 多次调用只注销一次。
func (r *Registry) Register(userID string) (connID string, release func()) {
	return r.RegisterConn(userID, nil)
}

// RegisterConn 登记一个属于 userID 的活跃连接及其操作句柄(通常传
// *wssession.Session),句柄经 Conns 枚举后可对该连接定向 Push / Kick。
// conn 为 nil 时等价 Register。
//
// 调用方应在连接结束时调用 release(通常 `defer release()`)。release 幂等,
// 多次调用只注销一次,注销同时移除元数据与句柄。
func (r *Registry) RegisterConn(userID string, conn Conn) (connID string, release func()) {
	connID = strconv.FormatUint(r.seq.Add(1), 36)
	rec := record{
		entry: Entry{ConnID: connID, UserID: userID, ConnectedAt: r.now()},
		conn:  conn,
	}

	r.mu.Lock()
	conns := r.byUser[userID]
	if conns == nil {
		conns = make(map[string]record)
		r.byUser[userID] = conns
	}
	conns[connID] = rec
	r.mu.Unlock()

	var once sync.Once
	release = func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			conns := r.byUser[userID]
			delete(conns, connID)
			if len(conns) == 0 {
				delete(r.byUser, userID)
			}
		})
	}
	return connID, release
}

// Conns 返回 userID 当前所有**带句柄**连接的快照(Register 注册的纯元数据
// 条目不在内);无则返回 nil。
//
// 返回的是独立切片,可在注册表锁外安全遍历——Kick 会同步等待出帧 flush,
// 必须在锁外调用,这正是 Conns 返回快照而非回调遍历的原因。
// 快照与实时状态存在固有竞态:枚举到的连接可能已在收敛中,Push/Kick
// 对此安全(失败返回错误 / 幂等无害)。
func (r *Registry) Conns(userID string) []Conn {
	r.mu.RLock()
	defer r.mu.RUnlock()
	conns := r.byUser[userID]
	if len(conns) == 0 {
		return nil
	}
	out := make([]Conn, 0, len(conns))
	for _, rec := range conns {
		if rec.conn != nil {
			out = append(out, rec.conn)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// List 返回 userID 当前所有活跃连接的元数据(独立副本);无活跃连接时返回 nil。
func (r *Registry) List(userID string) []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	conns := r.byUser[userID]
	if len(conns) == 0 {
		return nil
	}
	out := make([]Entry, 0, len(conns))
	for _, rec := range conns {
		out = append(out, rec.entry)
	}
	return out
}

// Count 返回 userID 当前活跃连接数。
func (r *Registry) Count(userID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byUser[userID])
}

// Users 返回当前有活跃连接的所有 userID(顺序不确定);没有活跃连接时返回 nil,
// 与 List / Conns 的空值约定一致。
func (r *Registry) Users() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Collect(maps.Keys(r.byUser))
}

// Total 返回全局活跃连接总数。
func (r *Registry) Total() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	total := 0
	for _, conns := range r.byUser {
		total += len(conns)
	}
	return total
}
