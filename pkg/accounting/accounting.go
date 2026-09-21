// Package accounting maintains user/queue quotas, instantaneous allocations
// and the conserved resource-time ledger used for fair-share accounting.
//
// Resource-time is accrued in exactly one place at a time per task:
//
//   - while running it lives in Usage.Running (accrued one tick at a time);
//   - when a run segment stops (finish / cancel / fail / preemption / node
//     loss) the accrued segment is moved once, and only once, into the
//     matching terminal or Preempted category via Settle.
//
// Thus Total() = Running + Finished + Canceled + Failed + Preempted is
// conserved across scheduling, repeated preemption, cancel and retry:
// preempted work is never re-executed and never lost.
package accounting

import (
	"sort"
	"sync"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
)

// Quota is a maximum instantaneous allocation cap. A zero field means
// unlimited on that dimension.
type Quota struct {
	MilliCPU int64
	MemoryMB int64
	GPUCount int64
}

// Contains reports whether r fits inside q.
func (q Quota) Contains(r api.Resources) bool {
	if q.MilliCPU > 0 && r.MilliCPU > q.MilliCPU {
		return false
	}
	if q.MemoryMB > 0 && r.MemoryMB > q.MemoryMB {
		return false
	}
	if q.GPUCount > 0 && r.GPUCount > q.GPUCount {
		return false
	}
	return true
}

// ResourceTime is usage measured in resource quantity × ticks (milli-unit
// ticks), partitioned by lifecycle category.
type ResourceTime struct {
	Running   api.Resources
	Finished  api.Resources
	Canceled  api.Resources
	Failed    api.Resources
	Preempted api.Resources
}

// Total returns the conserved usage across every category.
func (u ResourceTime) Total() api.Resources {
	t := u.Running
	t = api.Add(t, u.Finished)
	t = api.Add(t, u.Canceled)
	t = api.Add(t, u.Failed)
	t = api.Add(t, u.Preempted)
	return t
}

// Account is one user's or queue's cumulative ledger.
type Account struct {
	Name      string
	Quota     Quota
	Allocated api.Resources // instantaneous running reservation
	Usage     ResourceTime  // cumulative resource-time
	Weight    int64
}

// Ledger is the concurrency-safe quota and usage bookkeeper.
type Ledger struct {
	mu     sync.Mutex
	users  map[string]*Account
	queues map[string]*Account
}

// NewLedger constructs an empty ledger.
func NewLedger() *Ledger {
	return &Ledger{users: map[string]*Account{}, queues: map[string]*Account{}}
}

func (l *Ledger) userLocked(name string) *Account {
	a := l.users[name]
	if a == nil {
		a = &Account{Name: name, Weight: 1}
		l.users[name] = a
	}
	return a
}

func (l *Ledger) queueLocked(name string) *Account {
	a := l.queues[name]
	if a == nil {
		a = &Account{Name: name, Weight: 1}
		l.queues[name] = a
	}
	return a
}

// SetUserQuota registers a per-user cap.
func (l *Ledger) SetUserQuota(user string, q Quota) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.userLocked(user).Quota = q
}

// SetQueueQuota registers a per-queue cap.
func (l *Ledger) SetQueueQuota(queue string, q Quota) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.queueLocked(queue).Quota = q
}

// SetWeight records a user's fair-share weight.
func (l *Ledger) SetWeight(user string, w int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if w <= 0 {
		w = 1
	}
	l.userLocked(user).Weight = w
}

// AllocatedUser returns a user's currently allocated (running) resources.
func (l *Ledger) AllocatedUser(user string) api.Resources {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.userLocked(user).Allocated
}

// AllocatedQueue returns a queue's currently allocated resources.
func (l *Ledger) AllocatedQueue(queue string) api.Resources {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.queueLocked(queue).Allocated
}

// CanAllocate checks user + queue quota headroom for an entire gang.
func (l *Ledger) CanAllocate(user, queue string, add api.Resources) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	u := l.userLocked(user)
	if !u.Quota.Contains(api.Add(u.Allocated, add)) {
		return api.NewError(api.ErrQuotaExceeded,
			"user %s quota %v exhausted by additional request %v (in use %v)",
			user, u.Quota, add, u.Allocated)
	}
	q := l.queueLocked(queue)
	if !q.Quota.Contains(api.Add(q.Allocated, add)) {
		return api.NewError(api.ErrQueueQuotaExceeded,
			"queue %s quota %v exhausted by additional request %v (in use %v)",
			queue, q.Quota, add, q.Allocated)
	}
	return nil
}

// Allocate atomically records an allocation against user and queue.
func (l *Ledger) Allocate(user, queue string, r api.Resources) {
	l.mu.Lock()
	defer l.mu.Unlock()
	u := l.userLocked(user)
	q := l.queueLocked(queue)
	u.Allocated = api.Add(u.Allocated, r)
	q.Allocated = api.Add(q.Allocated, r)
}

// Release reverses an allocation.
func (l *Ledger) Release(user, queue string, r api.Resources) {
	l.mu.Lock()
	defer l.mu.Unlock()
	u := l.userLocked(user)
	q := l.queueLocked(queue)
	u.Allocated = api.Sub(u.Allocated, r)
	q.Allocated = api.Sub(q.Allocated, r)
}

// Accrue records ticks of resource-time consumption against the Running
// category (called while a task is alive).
func (l *Ledger) Accrue(user, queue string, r api.Resources) {
	if r.IsZero() {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	u := l.userLocked(user)
	q := l.queueLocked(queue)
	u.Usage.Running = api.Add(u.Usage.Running, r)
	q.Usage.Running = api.Add(q.Usage.Running, r)
}

// Settle moves accrued resource-time out of Running into another category.
// It must be called exactly once per stopped run segment, with the exact
// amount accrued for that segment, so the conserved total never changes.
// category is one of: "finished", "canceled", "failed", "preempted".
func (l *Ledger) Settle(user, queue string, r api.Resources, category string) {
	if r.IsZero() {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.settleLocked(l.userLocked(user), r, category)
	l.settleLocked(l.queueLocked(queue), r, category)
}

func (l *Ledger) settleLocked(a *Account, r api.Resources, category string) {
	a.Usage.Running = api.Sub(a.Usage.Running, r)
	switch category {
	case "finished":
		a.Usage.Finished = api.Add(a.Usage.Finished, r)
	case "canceled":
		a.Usage.Canceled = api.Add(a.Usage.Canceled, r)
	case "failed":
		a.Usage.Failed = api.Add(a.Usage.Failed, r)
	case "preempted":
		a.Usage.Preempted = api.Add(a.Usage.Preempted, r)
	default:
		// Never silently drop: unknown categories remain in Running is
		// impossible because we just subtracted; re-add to be conservative.
		a.Usage.Running = api.Add(a.Usage.Running, r)
	}
}

// FairShare computes each named user's weighted share [0,1] of total.
// Weights default to 1; users absent from the weights map still receive a
// share from the ledger if they have activity.
func (l *Ledger) FairShare(total api.Resources, users map[string]int64) map[string]float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	names := make([]string, 0, len(l.users))
	weights := map[string]int64{}
	var sumW int64
	weightOf := func(name string) int64 {
		if x := users[name]; x > 0 {
			return x
		}
		if a := l.users[name]; a != nil && a.Weight > 0 {
			return a.Weight
		}
		return 1
	}
	for name := range l.users {
		names = append(names, name)
		w := weightOf(name)
		weights[name] = w
		sumW += w
	}

	out := map[string]float64{}
	if sumW <= 0 {
		return out
	}
	sort.Strings(names)
	for _, name := range names {
		out[name] = float64(weights[name]) / float64(sumW)
	}
	return out
}

// UsageRatio returns each named user's cumulative total resource-time per
// unit weight, the input to fairness-based victim ordering. Dimension picks
// the compared resource (CPU by default).
func (l *Ledger) UsageRatio(user string) float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	a := l.userLocked(user)
	w := a.Weight
	if w <= 0 {
		w = 1
	}
	return float64(a.Usage.Total().MilliCPU) / float64(w)
}

// Snapshot returns deep copies of every account.
func (l *Ledger) Snapshot() map[string]Account {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]Account, len(l.users))
	for name, a := range l.users {
		out[name] = *a
	}
	return out
}

// QueueSnapshot returns deep copies of every queue account.
func (l *Ledger) QueueSnapshot() map[string]Account {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]Account, len(l.queues))
	for name, a := range l.queues {
		out[name] = *a
	}
	return out
}
