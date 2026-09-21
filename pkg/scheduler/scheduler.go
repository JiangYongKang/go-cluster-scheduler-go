// Package scheduler is the in-memory batch cluster scheduler. It provides
// gang scheduling of jobs and job arrays, hard GPU/NUMA/affinity topology
// matching, priority + fair-share preemption with conserved resource-time
// accounting, node-failure recovery and configurable decision-size limits.
//
// All state transitions are serialized under one mutex: concurrent Submit,
// Cancel, SetNodeState/FailNode and Tick calls can never produce partial
// updates or double placement. Every iteration order (jobs, tasks, nodes,
// victims) is fixed, so the result of an identical input sequence is
// reproducible.
package scheduler

import (
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/accounting"
	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/topology"
)

// Config bounds scheduler decision sizes to prevent unbounded growth.
type Config struct {
	MaxJobs              int // maximum live (non-terminal) jobs
	MaxTasksPerJob       int // maximum array size accepted at submit
	MaxPlacementsPerTick int // max tasks that may start in one tick
	MaxPreemptPerTick    int // max tasks that may be preempted in one tick
	MaxDecisionLog       int // retained TaskDecision entries (ring-trimmed)
}

// DefaultConfig returns conservative local-simulation limits.
func DefaultConfig() Config {
	return Config{
		MaxJobs:              4096,
		MaxTasksPerJob:       1024,
		MaxPlacementsPerTick: 256,
		MaxPreemptPerTick:    128,
		MaxDecisionLog:       8192,
	}
}

type jobEntry struct {
	job *api.Job
	seq int
}

// Scheduler is the concurrency-safe scheduling core.
type Scheduler struct {
	mu     sync.Mutex
	cfg    Config
	ledger *accounting.Ledger
	view   *topology.View
	nodes  map[string]*api.Node

	jobs     map[string]*jobEntry
	jobOrder []string

	tick     int64
	nextSeq  int
	decision []api.TaskDecision
	trace    io.Writer
}

// New constructs a Scheduler over the given nodes.
func New(nodes []*api.Node, cfg Config) *Scheduler {
	s := &Scheduler{
		cfg:    cfg,
		ledger: accounting.NewLedger(),
		nodes:  map[string]*api.Node{},
		jobs:   map[string]*jobEntry{},
	}
	for _, n := range nodes {
		s.nodes[n.Name] = n
	}
	s.view = topology.NewView(nodes)
	return s
}

// SetTrace enables internal reasoning output (used by tests to print
// scheduling inputs and judgment basis).
func (s *Scheduler) SetTrace(w io.Writer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trace = w
}

func (s *Scheduler) logf(format string, args ...any) {
	if s.trace != nil {
		fmt.Fprintf(s.trace, "[tick=%d] "+format+"\n", append([]any{s.tick}, args...)...)
	}
}

// Ledger exposes the accounting ledger for assertions/reporting.
func (s *Scheduler) Ledger() *accounting.Ledger { return s.ledger }

// Submit validates and admits a job.
func (s *Scheduler) Submit(spec api.JobSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.submitLocked(spec)
}

func (s *Scheduler) submitLocked(spec api.JobSpec) error {
	if spec.ID == "" {
		return api.NewError(api.ErrInvalid, "job id is required")
	}
	if _, exists := s.jobs[spec.ID]; exists {
		return api.NewError(api.ErrConflict, "job %q already exists", spec.ID)
	}
	if spec.ArraySize < 1 {
		return api.NewError(api.ErrInvalid, "job %q array size must be >= 1", spec.ID)
	}
	if spec.Runtime <= 0 {
		return api.NewError(api.ErrInvalid, "job %q runtime must be > 0", spec.ID)
	}
	if spec.TaskRes.MilliCPU < 0 || spec.TaskRes.MemoryMB < 0 || spec.TaskRes.GPUCount < 0 {
		return api.NewError(api.ErrInvalid, "job %q resources must be non-negative", spec.ID)
	}
	if s.cfg.MaxTasksPerJob > 0 && spec.ArraySize > s.cfg.MaxTasksPerJob {
		return api.NewError(api.ErrDecisionLimit,
			"job %q array size %d exceeds limit %d", spec.ID, spec.ArraySize, s.cfg.MaxTasksPerJob)
	}
	live := 0
	for _, e := range s.jobs {
		if e.job.State != api.JobSucceeded && e.job.State != api.JobFailed && e.job.State != api.JobCanceled {
			live++
		}
	}
	if s.cfg.MaxJobs > 0 && live >= s.cfg.MaxJobs {
		return api.NewError(api.ErrDecisionLimit,
			"live job count %d reaches limit %d", live, s.cfg.MaxJobs)
	}

	gang := api.Scale(spec.TaskRes, int64(spec.ArraySize))
	// Upfront aggregate quota check for the WHOLE gang: a partially admittable
	// gang is not admitted at all.
	if err := s.ledger.CanAllocate(spec.User, spec.Queue, gang); err != nil {
		return err
	}

	j := &api.Job{Spec: spec, State: api.JobQueued}
	for i := 0; i < spec.ArraySize; i++ {
		j.Tasks = append(j.Tasks, &api.Task{
			ID:          fmt.Sprintf("%s[%d]", spec.ID, i),
			Index:       i,
			State:       api.TaskPending,
			RuntimeNeed: spec.Runtime,
		})
	}
	s.nextSeq++
	s.jobs[spec.ID] = &jobEntry{job: j, seq: s.nextSeq}
	s.jobOrder = append(s.jobOrder, spec.ID)
	s.logf("submit job=%s user=%s queue=%s pri=%d array=%d gang=%+v",
		spec.ID, spec.User, spec.Queue, spec.Priority, spec.ArraySize, gang)
	return nil
}

// Cancel terminates all pending/running tasks of a job.
func (s *Scheduler) Cancel(jobID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.jobs[jobID]
	if e == nil {
		return api.NewError(api.ErrNotFound, "job %q not found", jobID)
	}
	j := e.job
	if j.State == api.JobCanceled {
		return api.NewError(api.ErrTerminal, "job %q already canceled", jobID)
	}
	s.logf("cancel job=%s reason=%s", jobID, reason)
	now := s.tick
	for i, t := range j.Tasks {
		if t.State.IsTerminal() {
			continue
		}
		if t.State == api.TaskRunning {
			s.stopRunningLocked(j, t, now, api.TaskCanceled, "canceled", reason)
		} else {
			t.State = api.TaskCanceled
			t.FailureNote = reason
		}
		s.record(api.TaskDecision{
			JobID: jobID, TaskID: t.ID, Action: api.DecisionCanceled,
			NodeName: t.NodeName, Reason: reason,
		})
		_ = i
	}
	j.State = api.JobCanceled
	return nil
}

// stopRunningLocked settles one run segment exactly once, releases resources
// and moves the task to a new state. category is the accounting bucket.
func (s *Scheduler) stopRunningLocked(j *api.Job, t *api.Task, now int64, newState api.TaskState, category, note string) {
	segTicks := now - t.StartTick
	res := j.Spec.TaskRes
	// Release the instantaneous reservation first; then move the accrued
	// segment resource-time once into its final category.
	s.view.FreeTask(t.ID)
	s.ledger.Release(j.Spec.User, j.Spec.Queue, res)
	if segTicks > 0 {
		s.ledger.Settle(j.Spec.User, j.Spec.Queue, api.Scale(res, segTicks), category)
	}
	t.NodeName = ""
	t.State = newState
	t.FailureNote = note
}

// SetNodeState flips a node's reachability.
func (s *Scheduler) SetNodeState(nodeName string, state api.NodeState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.nodes[nodeName]
	if n == nil {
		return api.NewError(api.ErrNotFound, "node %q not found", nodeName)
	}
	if n.State == state {
		return nil
	}
	n.State = state
	s.logf("node %s -> %s", nodeName, state)
	if state == api.NodeNotReady {
		s.evictLocked(nodeName, false, 0)
	}
	return nil
}

// FailNode injects permanent node loss. maxRestarts bounds how many times an
// evicted task may be rescheduled; tasks beyond the budget terminate Lost.
func (s *Scheduler) FailNode(nodeName string, maxRestarts int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.nodes[nodeName]
	if n == nil {
		return api.NewError(api.ErrNotFound, "node %q not found", nodeName)
	}
	n.State = api.NodeNotReady
	s.logf("node %s FAILED maxRestarts=%d", nodeName, maxRestarts)
	s.evictLocked(nodeName, true, maxRestarts)
	return nil
}

// evictLocked removes every task from a node. Soft evictions always requeue;
// permanent failures requeue only within the restart budget.
func (s *Scheduler) evictLocked(nodeName string, permanent bool, maxRestarts int) {
	placements := s.view.FreeOnNode(nodeName)
	now := s.tick
	for _, p := range placements {
		e := s.jobs[p.JobID]
		if e == nil {
			continue
		}
		j := e.job
		t := findTask(j, p.TaskID)
		if t == nil || t.State != api.TaskRunning {
			continue
		}
		if permanent && t.Restarts >= maxRestarts {
			// Unrecoverable: distinct terminal state.
			s.stopRunningLocked(j, t, now, api.TaskLost, "failed",
				fmt.Sprintf("node %s permanently lost after %d restarts", nodeName, t.Restarts))
			s.record(api.TaskDecision{
				JobID: p.JobID, TaskID: t.ID, Action: api.DecisionLost,
				NodeName: nodeName, Reason: t.FailureNote,
			})
			continue
		}
		s.stopRunningLocked(j, t, now, api.TaskPreempted, "preempted",
			fmt.Sprintf("evicted: node %s unreachable", nodeName))
		t.Restarts++
		s.record(api.TaskDecision{
			JobID: p.JobID, TaskID: t.ID, Action: api.DecisionRequeued,
			NodeName: nodeName,
			Reason:   fmt.Sprintf("node %s down; restarts=%d", nodeName, t.Restarts),
		})
	}
}

func findTask(j *api.Job, id string) *api.Task {
	for _, t := range j.Tasks {
		if t.ID == id {
			return t
		}
	}
	return nil
}

// Tick advances the simulation one logical step.
func (s *Scheduler) Tick() api.TickResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tick++
	tick := s.tick
	result := api.TickResult{Tick: tick}

	// 1. Runtime accrual + completions, in a fixed (job, task) order.
	for _, jid := range s.jobOrder {
		j := s.jobs[jid].job
		if j.State == api.JobCanceled {
			continue
		}
		for _, t := range j.Tasks {
			if t.State != api.TaskRunning {
				continue
			}
			res := j.Spec.TaskRes
			t.RunTicks++
			s.ledger.Accrue(j.Spec.User, j.Spec.Queue, res)
			if t.RemainingTicks() == 0 {
				node := t.NodeName
				s.stopRunningLocked(j, t, tick, api.TaskFinished, "finished", "")
				d := api.TaskDecision{JobID: j.Spec.ID, TaskID: t.ID,
					Action: api.DecisionFinished, NodeName: node,
					Reason: fmt.Sprintf("completed at tick %d", tick)}
				s.record(d)
				result.Finished = append(result.Finished, d)
			}
		}
	}

	// 2. Schedule pending/preempted tasks, gang by gang.
	placed, preempted, rejected, deferred := s.scheduleLocked(tick)
	result.Placed = append(result.Placed, placed...)
	result.Preempted = append(result.Preempted, preempted...)
	result.Rejected = append(result.Rejected, rejected...)
	result.Deferred = append(result.Deferred, deferred...)

	// 3. Recompute aggregate job states.
	for _, jid := range s.jobOrder {
		s.recomputeState(s.jobs[jid].job)
	}

	// 4. Bound retained memory.
	if s.cfg.MaxDecisionLog > 0 && len(s.decision) > s.cfg.MaxDecisionLog {
		s.decision = s.decision[len(s.decision)-s.cfg.MaxDecisionLog:]
	}
	return result
}

func (s *Scheduler) recomputeState(j *api.Job) {
	if j.State == api.JobCanceled {
		return
	}
	var finished, terminal, running int
	for _, t := range j.Tasks {
		if t.State.IsTerminal() {
			terminal++
		}
		if t.State == api.TaskFinished {
			finished++
		}
		if t.State == api.TaskRunning {
			running++
		}
	}
	switch {
	case finished == len(j.Tasks):
		j.State = api.JobSucceeded
	case terminal == len(j.Tasks):
		j.State = api.JobFailed
	case running > 0:
		j.State = api.JobActive
	default:
		j.State = api.JobQueued
	}
}

// schedulableTasks returns the gang for one scheduling cycle.
func schedulableTasks(j *api.Job) []*api.Task {
	var out []*api.Task
	for _, t := range j.Tasks {
		if t.State == api.TaskPending || t.State == api.TaskPreempted {
			out = append(out, t)
		}
	}
	return out
}

func (s *Scheduler) scheduleLocked(tick int64) (placed, preempted, rejected, deferred []api.TaskDecision) {
	type cand struct {
		e *jobEntry
		j *api.Job
	}
	var cands []cand
	for _, jid := range s.jobOrder {
		e := s.jobs[jid]
		j := e.job
		if j.State == api.JobCanceled {
			continue
		}
		if len(schedulableTasks(j)) > 0 {
			cands = append(cands, cand{e, j})
		}
	}
	// Higher priority first; ties broken by submission order -> deterministic.
	sort.SliceStable(cands, func(a, b int) bool {
		if cands[a].j.Spec.Priority != cands[b].j.Spec.Priority {
			return cands[a].j.Spec.Priority > cands[b].j.Spec.Priority
		}
		return cands[a].e.seq < cands[b].e.seq
	})

	placedThisTick, preemptedThisTick := 0, 0

	for _, c := range cands {
		j := c.j
		tasks := schedulableTasks(j)
		if len(tasks) == 0 {
			continue
		}

		if s.cfg.MaxPlacementsPerTick > 0 &&
			len(tasks) > s.cfg.MaxPlacementsPerTick-placedThisTick {
			d := api.TaskDecision{JobID: j.Spec.ID, TaskID: j.Spec.ID,
				Action: api.DecisionDeferred,
				Reason: fmt.Sprintf("gang size %d exceeds remaining placement cap %d",
					len(tasks), s.cfg.MaxPlacementsPerTick-placedThisTick)}
			s.record(d)
			deferred = append(deferred, d)
			continue
		}

		gang := api.Scale(j.Spec.TaskRes, int64(len(tasks)))
		if err := s.ledger.CanAllocate(j.Spec.User, j.Spec.Queue, gang); err != nil {
			// Transient saturation (same user/queue jobs running): wait.
			d := api.TaskDecision{JobID: j.Spec.ID, TaskID: j.Spec.ID,
				Action: api.DecisionDeferred, Reason: err.Error()}
			s.record(d)
			deferred = append(deferred, d)
			continue
		}

		plan, rejects := s.planGang(j, tasks, tick,
			s.cfg.MaxPreemptPerTick-preemptedThisTick)
		switch plan.kind {
		case planPlace:
			// Commit preemptions first, then the gang. Everything was proven
			// feasible on the cloned view, so no partial state is possible.
			for _, v := range plan.victims {
				vj := s.jobs[v.JobID].job
				vt := findTask(vj, v.TaskID)
				node := vt.NodeName
				s.stopRunningLocked(vj, vt, tick, api.TaskPreempted, "preempted",
					fmt.Sprintf("preempted by %s (prio %d)", j.Spec.ID, j.Spec.Priority))
				vt.Restarts++
				d := api.TaskDecision{JobID: vj.Spec.ID, TaskID: vt.ID,
					Action: api.DecisionPreempted, NodeName: node,
					Reason: dReason(v.Res, j.Spec.ID, j.Spec.Priority)}
				s.record(d)
				preempted = append(preempted, d)
				preemptedThisTick++
			}
			s.ledger.Allocate(j.Spec.User, j.Spec.Queue, gang)
			for _, pl := range plan.placements {
				t := findTask(j, pl.TaskID)
				if err := s.view.PlaceTask(pl.NodeName, j.Spec.ID, t.ID,
					j.Spec.TaskRes, j.Spec.Topology); err != nil {
					// Cannot happen: clone committed identical operations.
					panic("scheduler: commit after successful plan failed: " + err.Error())
				}
				t.State = api.TaskRunning
				t.NodeName = pl.NodeName
				t.StartTick = tick
				t.Version++
				d := api.TaskDecision{JobID: j.Spec.ID, TaskID: t.ID,
					Action: api.DecisionPlaced, NodeName: pl.NodeName,
					Reason: placementNote(pl, len(plan.victims))}
				s.record(d)
				placed = append(placed, d)
				placedThisTick++
			}
		case planReject:
			// Static topology/capacity cannot host the gang on ANY node,
			// even empty: this is permanent, so terminate the remaining
			// tasks rather than retrying forever. No task was placed, so
			// there is no partial state to roll back.
			for _, t := range tasks {
				t.State = api.TaskFailed
				t.FailureNote = "hard topology/capacity constraints unsatisfiable on every node"
			}
			d := api.TaskDecision{JobID: j.Spec.ID, TaskID: j.Spec.ID,
				Action: api.DecisionRejected, Rejects: rejects,
				Reason: "hard constraints unsatisfiable on every node with raw capacity; job terminated"}
			s.record(d)
			rejected = append(rejected, d)
		default: // planDefer
			d := api.TaskDecision{JobID: j.Spec.ID, TaskID: j.Spec.ID,
				Action: api.DecisionDeferred, Rejects: rejects, Reason: plan.detail}
			s.record(d)
			deferred = append(deferred, d)
		}
	}
	return
}

func dReason(res api.Resources, by string, prio int) string {
	return fmt.Sprintf("freed %+v for preemptor %s priority=%d", res, by, prio)
}

func placementNote(pl taskPlacement, victims int) string {
	if victims > 0 {
		return fmt.Sprintf("placed after %d preemptions; numa=%d gpus=%v",
			victims, pl.NumaID, pl.GPUIDs)
	}
	return fmt.Sprintf("placed; numa=%d gpus=%v", pl.NumaID, pl.GPUIDs)
}

func (s *Scheduler) record(d api.TaskDecision) {
	d.Tick = s.tick
	s.decision = append(s.decision, d)
	s.logf("decision %s", d)
}

// GetJob returns a deep copy of a job.
func (s *Scheduler) GetJob(jobID string) (*api.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.jobs[jobID]
	if e == nil {
		return nil, api.NewError(api.ErrNotFound, "job %q not found", jobID)
	}
	return cloneJob(e.job), nil
}

// ListJobs returns deep copies of all jobs in submission order.
func (s *Scheduler) ListJobs() []*api.Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*api.Job, 0, len(s.jobOrder))
	for _, jid := range s.jobOrder {
		out = append(out, cloneJob(s.jobs[jid].job))
	}
	return out
}

func cloneJob(j *api.Job) *api.Job {
	c := &api.Job{Spec: j.Spec, State: j.State}
	for _, t := range j.Tasks {
		ct := *t
		c.Tasks = append(c.Tasks, &ct)
	}
	return c
}

// Decisions returns a copy of the retained decision log.
func (s *Scheduler) Decisions() []api.TaskDecision {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]api.TaskDecision{}, s.decision...)
}

// CurrentTick returns the logical clock value.
func (s *Scheduler) CurrentTick() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tick
}
