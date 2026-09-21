// Package simulation drives a Scheduler with a deterministic local logical
// clock and injectable events (submits, cancels, node failures). No real
// cluster, external service or credential is involved.
package simulation

import (
	"sort"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/scheduler"
)

// Event is one scheduled input in a simulation timeline.
type Event struct {
	AtTick      int64
	Kind        string // "submit" | "cancel" | "node" | "failnode"
	Job         *api.JobSpec
	JobID       string
	NodeName    string
	State       api.NodeState
	Reason      string
	MaxRestarts int
}

// Runner executes an ordered event timeline.
type Runner struct {
	Sched *scheduler.Scheduler
}

// NewRunner wraps an existing scheduler.
func NewRunner(s *scheduler.Scheduler) *Runner { return &Runner{Sched: s} }

// Run processes events up to maxTicks, returning one TickResult per tick
// (including no-op ticks). Events at the same tick are applied in stable
// order sorted by (AtTick, then input order for same-tick events).
func (r *Runner) Run(events []Event, maxTicks int64) []api.TickResult {
	// Stable sort must not reorder same-tick events; sort.SliceStable on
	// AtTick only preserves input order.
	ev := append([]Event{}, events...)
	sort.SliceStable(ev, func(i, j int) bool { return ev[i].AtTick < ev[j].AtTick })

	var results []api.TickResult
	idx := 0
	for tick := int64(1); tick <= maxTicks; tick++ {
		for idx < len(ev) && ev[idx].AtTick <= tick {
			r.apply(ev[idx])
			idx++
		}
		results = append(results, r.Sched.Tick())
	}
	return results
}

// UntilDone ticks while non-terminal jobs remain (bounded by maxTicks).
func (r *Runner) UntilDone(maxTicks int64) []api.TickResult {
	var results []api.TickResult
	for tick := int64(0); tick < maxTicks; tick++ {
		results = append(results, r.Sched.Tick())
		if r.done() {
			break
		}
	}
	return results
}

func (r *Runner) done() bool {
	for _, j := range r.Sched.ListJobs() {
		st := j.State
		if st != api.JobSucceeded && st != api.JobFailed && st != api.JobCanceled {
			return false
		}
	}
	return true
}

func (r *Runner) apply(e Event) {
	switch e.Kind {
	case "submit":
		// Submit errors are surfaced via the returned error path in tests;
		// here they are intentionally ignored only for best-effort scripts.
		_ = r.Sched.Submit(*e.Job)
	case "cancel":
		_ = r.Sched.Cancel(e.JobID, e.Reason)
	case "node":
		_ = r.Sched.SetNodeState(e.NodeName, e.State)
	case "failnode":
		_ = r.Sched.FailNode(e.NodeName, e.MaxRestarts)
	}
}
