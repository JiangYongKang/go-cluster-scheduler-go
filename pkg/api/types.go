// Package api defines the core domain types of the in-memory batch cluster
// scheduler: jobs, tasks, nodes, resources and topology constraints.
package api

// Resources describes a scalar resource vector. CPU and Memory are required;
// GPUCount is the number of accelerator devices. Quantities use milli-units
// (1000 == 1 unit) so that integer arithmetic stays exact.
type Resources struct {
	MilliCPU int64
	MemoryMB int64
	GPUCount int64
}

// TopologyReq captures GPU / NUMA / affinity placement constraints.
// Constraints are hard: a node that cannot satisfy them is excluded rather
// than silently downgraded.
type TopologyReq struct {
	GPUModel     string            // required GPU model; "" means any / no GPU
	NumaLocal    bool              // all allocations of one task must land in one NUMA node
	NodeAffinity map[string]string // node must carry all these labels
	AntiAffinity []string          // node must not already run a task of these job IDs
}

// NodeState is the lifecycle state of a simulated node.
type NodeState int

const (
	NodeReady NodeState = iota
	NodeNotReady
)

func (s NodeState) String() string {
	switch s {
	case NodeReady:
		return "Ready"
	case NodeNotReady:
		return "NotReady"
	default:
		return "Unknown"
	}
}

// NUMANode is one NUMA locality on a node.
type NUMANode struct {
	ID       int
	MilliCPU int64
	MemoryMB int64
	GPUIDs   []int // GPU devices local to this NUMA node
}

// GPU is one accelerator device.
type GPU struct {
	ID    int
	Model string
	Numa  int
}

// Node is a simulated worker node.
type Node struct {
	Name      string
	Capacity  Resources
	State     NodeState
	Labels    map[string]string
	NumaNodes []NUMANode
	GPUs      []GPU
}

// TaskState is the lifecycle state of a single task.
type TaskState int

const (
	TaskPending TaskState = iota
	TaskRunning
	TaskPreempted // preempted, pending re-scheduling
	TaskFailed    // terminal: unrecoverable failure
	TaskFinished  // terminal: completed successfully
	TaskCanceled  // terminal: canceled by the user
	TaskLost      // terminal: died with its node and recovery was impossible
)

func (s TaskState) String() string {
	switch s {
	case TaskPending:
		return "Pending"
	case TaskRunning:
		return "Running"
	case TaskPreempted:
		return "Preempted"
	case TaskFailed:
		return "Failed"
	case TaskFinished:
		return "Finished"
	case TaskCanceled:
		return "Canceled"
	case TaskLost:
		return "Lost"
	default:
		return "Unknown"
	}
}

// IsTerminal reports whether the state is a terminal state.
func (s TaskState) IsTerminal() bool {
	switch s {
	case TaskFailed, TaskFinished, TaskCanceled, TaskLost:
		return true
	default:
		return false
	}
}

// JobState is the aggregate state of a job / job array.
type JobState int

const (
	JobQueued JobState = iota
	JobActive
	JobSucceeded
	JobFailed
	JobCanceled
)

func (s JobState) String() string {
	switch s {
	case JobQueued:
		return "Queued"
	case JobActive:
		return "Active"
	case JobSucceeded:
		return "Succeeded"
	case JobFailed:
		return "Failed"
	case JobCanceled:
		return "Canceled"
	default:
		return "Unknown"
	}
}

// Task is one member of a job array.
type Task struct {
	ID       string
	Index    int
	State    TaskState
	NodeName string

	// Runtime bookkeeping.
	Version     int64  // bumped on every (re)placement; guards stale execution
	StartTick   int64  // tick at which the current run segment started
	RunTicks    int64  // accumulated running ticks over all segments
	RuntimeNeed int64  // total running ticks required to finish
	Restarts    int    // rescheduling attempts after eviction/preemption
	FailureNote string // human-readable explanation of a terminal failure
}

// RemainingTicks is the runtime still needed before completion. Accrued run
// time is never lost: it survives preemption and node loss, so partially
// executed tasks resume for the remainder only.
func (t *Task) RemainingTicks() int64 {
	r := t.RuntimeNeed - t.RunTicks
	if r < 0 {
		return 0
	}
	return r
}

// JobSpec is the immutable description of a submitted job / job array.
type JobSpec struct {
	ID       string
	User     string
	Queue    string
	Priority int // larger is more important
	Weight   int64
	// ArraySize tasks share one spec. Gang scheduling is all-or-nothing:
	// every cycle either all non-terminal tasks start together or none do.
	ArraySize int
	TaskRes   Resources
	Runtime   int64 // running ticks each task needs
	Topology  TopologyReq
}

// Job is the mutable runtime view of a JobSpec.
type Job struct {
	Spec  JobSpec
	Tasks []*Task
	State JobState
}

// PendingTasks returns tasks that may still be scheduled (pending or
// preempted; never terminal, never running).
func (j *Job) PendingTasks() []*Task {
	var out []*Task
	for _, t := range j.Tasks {
		if t.State == TaskPending || t.State == TaskPreempted {
			out = append(out, t)
		}
	}
	return out
}

// RunningTaskCount returns how many tasks are currently running.
func (j *Job) RunningTaskCount() int {
	c := 0
	for _, t := range j.Tasks {
		if t.State == TaskRunning {
			c++
		}
	}
	return c
}
