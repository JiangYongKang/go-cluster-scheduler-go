// Package preemption implements victim selection for priority- and
// fairness-based preemption.
//
// Eligibility and ordering are deterministic so the same input always yields
// the same victims:
//
//  1. Preemption is strictly priority-gated: a task may be preempted only by
//     a strictly higher-priority job of another user. Same-priority jobs can
//     never preempt each other, and a job can never preempt itself.
//  2. Among all eligible (lower-priority) victims, fair-share decides order:
//     the user with the greatest cumulative usage-per-weight (FairnessKey)
//     yields first; ties use older start tick then task ID.
//  3. Tasks are taken in that order until every dimension of the requested
//     resource vector is covered.
package preemption

import (
	"sort"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
)

// TaskInfo describes one running task exposed to the policy.
type TaskInfo struct {
	TaskID      string
	JobID       string
	User        string
	NodeName    string
	Res         api.Resources
	Priority    int
	StartTick   int64
	UsedTicks   int64
	FairnessKey float64 // cumulative resource-time per weight of the task's user
}

// JobInfo describes a running job exposed to the policy.
type JobInfo struct {
	ID       string
	User     string
	Priority int
	Tasks    []TaskInfo
}

// Victim is one task chosen for preemption.
type Victim struct {
	TaskID   string
	JobID    string
	NodeName string
	Res      api.Resources
}

// Plan is a chosen preemption set.
type Plan struct {
	Victims []Victim
}

// Freed reports resources released by the plan.
func (p *Plan) Freed() api.Resources {
	var f api.Resources
	for _, v := range p.Victims {
		f = api.Add(f, v.Res)
	}
	return f
}

func eligible(t TaskInfo, preemptorPriority int, preemptorUser string, preemptorKey float64) bool {
	_ = preemptorKey
	if t.User == preemptorUser {
		return false
	}
	return t.Priority < preemptorPriority
}

func ordered(candidates []TaskInfo, preemptorPriority int, preemptorUser string, preemptorKey float64, nodeFilter string) []TaskInfo {
	out := make([]TaskInfo, 0, len(candidates))
	for _, t := range candidates {
		if nodeFilter != "" && t.NodeName != nodeFilter {
			continue
		}
		if eligible(t, preemptorPriority, preemptorUser, preemptorKey) {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Priority != b.Priority {
			return a.Priority < b.Priority
		}
		if a.FairnessKey != b.FairnessKey {
			return a.FairnessKey > b.FairnessKey
		}
		if a.StartTick != b.StartTick {
			return a.StartTick < b.StartTick
		}
		return a.TaskID < b.TaskID
	})
	return out
}

// SelectVictims chooses victims, optionally restricted to one node, until
// enough resources are freed in every dimension. Returns nil when the
// eligible set cannot cover want.
func SelectVictims(candidates []TaskInfo, want api.Resources, preemptorPriority int, preemptorUser string, preemptorKey float64, nodeName string) []Victim {
	var freed api.Resources
	var victims []Victim
	for _, t := range ordered(candidates, preemptorPriority, preemptorUser, preemptorKey, nodeName) {
		if api.LessEqual(want, freed) {
			break
		}
		victims = append(victims, Victim{TaskID: t.TaskID, JobID: t.JobID, NodeName: t.NodeName, Res: t.Res})
		freed = api.Add(freed, t.Res)
	}
	if !api.LessEqual(want, freed) {
		return nil
	}
	return victims
}

// FindPlan deterministically selects enough victims (cluster-wide) to free
// want for a preemptor. Returns (nil,false) if preemption cannot free enough.
func FindPlan(jobs []JobInfo, want api.Resources, preemptorPriority int, preemptorUser string, preemptorKey float64) (*Plan, bool) {
	var tasks []TaskInfo
	for _, j := range jobs {
		tasks = append(tasks, j.Tasks...)
	}
	victims := SelectVictims(tasks, want, preemptorPriority, preemptorUser, preemptorKey, "")
	if victims == nil {
		return nil, false
	}
	return &Plan{Victims: victims}, true
}
