package scheduler

import (
	"fmt"
	"sort"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/preemption"
)

type planKind int

const (
	planDefer planKind = iota
	planPlace
	planReject
)

type taskPlacement struct {
	TaskID   string
	NodeName string
	NumaID   int
	GPUIDs   []int
}

type plannedVictim struct {
	TaskID string
	JobID  string
	Res    api.Resources
}

type gangPlan struct {
	kind       planKind
	placements []taskPlacement
	victims    []plannedVictim
	detail     string
}

// rawNodeCap is a topology-capability check that ignores occupancy: it
// answers whether an EMPTY node could ever host one task of the job. This is
// used to distinguish permanent hard-constraint rejection from transient
// shortage (which preemption or waiting can resolve).
func rawNodeCap(n *api.Node, res api.Resources, req api.TopologyReq) (int, api.RejectReason) {
	for k, val := range req.NodeAffinity {
		if n.Labels[k] != val {
			return 0, api.RejectNodeAffinity
		}
	}
	modelGPUs := 0
	if res.GPUCount > 0 {
		for _, g := range n.GPUs {
			if req.GPUModel == "" || g.Model == req.GPUModel {
				modelGPUs++
			}
		}
		if req.GPUModel != "" && modelGPUs == 0 {
			return 0, api.RejectGPUModel
		}
	}
	if req.NumaLocal {
		best := int64(0)
		numaGPU := map[int][]int{}
		for _, g := range n.GPUs {
			numaGPU[g.Numa] = append(numaGPU[g.Numa], g.ID)
		}
		if len(n.NumaNodes) == 0 {
			// Single synthetic NUMA over total capacity.
			cpu, mem := n.Capacity.MilliCPU, n.Capacity.MemoryMB
			gpuOK := modelGPUs
			hosts := min64(cpu/max1(res.MilliCPU), mem/max1(res.MemoryMB))
			if res.GPUCount > 0 {
				hosts = min64(hosts, int64(gpuOK)/res.GPUCount)
			}
			return int(hosts), ""
		}
		for _, nu := range n.NumaNodes {
			if nu.MilliCPU < res.MilliCPU || nu.MemoryMB < res.MemoryMB {
				continue
			}
			hosts := min64(nu.MilliCPU/max1(res.MilliCPU), nu.MemoryMB/max1(res.MemoryMB))
			if res.GPUCount > 0 {
				gpuOK := 0
				for _, gid := range nu.GPUIDs {
					if gid < len(n.GPUs) && (req.GPUModel == "" || n.GPUs[gid].Model == req.GPUModel) {
						gpuOK++
					}
				}
				hosts = min64(hosts, int64(gpuOK)/res.GPUCount)
			}
			if hosts > best {
				best = hosts
			}
		}
		if best == 0 {
			if res.GPUCount > 0 {
				return 0, api.RejectNumaLocal
			}
			return 0, api.RejectNumaLocal
		}
		return int(best), ""
	}
	hosts := min64(n.Capacity.MilliCPU/max1(res.MilliCPU), n.Capacity.MemoryMB/max1(res.MemoryMB))
	if res.GPUCount > 0 {
		hosts = min64(hosts, int64(modelGPUs)/res.GPUCount)
	}
	return int(hosts), ""
}

func max1(x int64) int64 {
	if x <= 0 {
		return 1
	}
	return x
}

// planGang simulates an all-or-nothing placement on a cloned topology view.
// On failure the clone is discarded, so real state is never half-updated.
func (s *Scheduler) planGang(j *api.Job, tasks []*api.Task, tick int64, preemptBudget int) (gangPlan, map[string]api.RejectReason) {
	res := j.Spec.TaskRes
	req := j.Spec.Topology

	// Current-occupancy reject reasons per node (explainability).
	rejects := map[string]api.RejectReason{}
	// Raw capacity of the cluster ignoring occupancy (hard-limit detection).
	rawCap := 0
	rawCapable := 0
	nodeNames := s.view.NodeNames()
	for _, name := range nodeNames {
		n := s.view.Node(name)
		if n.State != api.NodeReady {
			rejects[name] = api.RejectNodeNotReady
			continue
		}
		if hosts, reason := rawNodeCap(n, res, req); reason != "" {
			rejects[name] = reason
		} else {
			rawCap += hosts
			rawCapable++
		}
		if ok, reason := s.view.EvaluateTask(n, res, req, j.Spec.ID); !ok {
			rejects[name] = reason
		}
	}
	if rawCap < len(tasks) {
		// No amount of waiting or preemption can ever fit this gang.
		return gangPlan{kind: planReject, detail: fmt.Sprintf(
			"raw capacity %d tasks < gang %d; %d raw-capable nodes",
			rawCap, len(tasks), rawCapable)}, rejects
	}

	// committed holds victims and placements already locked in for this gang.
	committed := s.view.Clone()
	preemptorKey := s.ledger.UsageRatio(j.Spec.User)
	candidates := s.runningTaskInfos()
	var victims []plannedVictim
	usedPreempt := 0
	committedVictims := map[string]bool{}

	var placements []taskPlacement
	// placeOne runs each candidate node on an isolated scratch view; only a
	// successful attempt is merged into committed, so failed attempts can
	// never leak unrelated victims into the plan (no partial state). Nodes
	// are considered best-fit by least current utilization so gangs spread
	// evenly; among equally free nodes the first name wins (determinism).
	placeOne := func(t *api.Task) (string, []preemption.Victim, bool) {
		type try struct {
			name    string
			victims []preemption.Victim
			free    api.Resources
		}
		var best *try
		for _, name := range nodeNames {
			n := s.view.Node(name)
			if n.State != api.NodeReady {
				continue
			}
			scratch := committed.Clone()
			var vs []preemption.Victim
			if ok, _ := scratch.EvaluateTask(n, res, req, j.Spec.ID); !ok {
				// A hard static mismatch cannot be fixed by removing tasks.
				if _, reason := rawNodeCap(n, res, req); reason != "" {
					continue
				}
				// Preempt only the shortfall over capacity already free.
				free := scratch.NodeFree(name)
				short := api.Sub(res, free)
				if short.MilliCPU < 0 {
					short.MilliCPU = 0
				}
				if short.MemoryMB < 0 {
					short.MemoryMB = 0
				}
				if short.GPUCount < 0 {
					short.GPUCount = 0
				}
				nodeCand := []preemption.TaskInfo{}
				for _, c := range candidates {
					if c.NodeName == name && !committedVictims[c.TaskID] {
						nodeCand = append(nodeCand, c)
					}
				}
				selected := preemption.SelectVictims(nodeCand, short, j.Spec.Priority,
					j.Spec.User, preemptorKey, name)
				if len(selected) == 0 || usedPreempt+len(selected) > preemptBudget {
					continue
				}
				for _, v := range selected {
					scratch.FreeTask(v.TaskID)
				}
				if ok2, _ := scratch.EvaluateTask(n, res, req, j.Spec.ID); !ok2 {
					continue
				}
				vs = selected
			}
			free := scratch.NodeFree(name)
			// Least-utilized-first: pick the node with the MOST free CPU
			// (ties: most memory, most GPUs, then deterministic name order),
			// spreading gangs evenly across nodes.
			if best == nil ||
				free.MilliCPU > best.free.MilliCPU ||
				(free.MilliCPU == best.free.MilliCPU && free.MemoryMB > best.free.MemoryMB) ||
				(free.MilliCPU == best.free.MilliCPU && free.MemoryMB == best.free.MemoryMB && free.GPUCount > best.free.GPUCount) {
				best = &try{name: name, victims: vs, free: free}
			}
		}
		if best != nil {
			return best.name, best.victims, true
		}
		return "", nil, false
	}

	for _, t := range tasks {
		name, vs, ok := placeOne(t)
		if !ok {
			return gangPlan{kind: planDefer, detail: fmt.Sprintf(
				"insufficient preemptable capacity for gang of %d (budget %d)",
				len(tasks), preemptBudget)}, rejects
		}
		for _, v := range vs {
			committed.FreeTask(v.TaskID)
			committedVictims[v.TaskID] = true
			victims = append(victims, plannedVictim{TaskID: v.TaskID, JobID: v.JobID, Res: v.Res})
			usedPreempt++
		}
		if err := committed.PlaceTask(name, j.Spec.ID, t.ID, res, req); err != nil {
			return gangPlan{kind: planDefer, detail: "scratch placement failed: " + err.Error()}, rejects
		}
		pl, _ := committed.TaskPlacement(t.ID)
		placements = append(placements, taskPlacement{
			TaskID: t.ID, NodeName: name, NumaID: pl.NumaID, GPUIDs: pl.GPUIDs,
		})
	}

	return gangPlan{kind: planPlace, placements: placements, victims: victims}, rejects
}

// runningTaskInfos builds the preemption candidate set from the real,
// committed view. Iteration is sorted for determinism.
func (s *Scheduler) runningTaskInfos() []preemption.TaskInfo {
	var out []preemption.TaskInfo
	jids := append([]string{}, s.jobOrder...)
	sort.Strings(jids)
	for _, jid := range jids {
		j := s.jobs[jid].job
		for _, t := range j.Tasks {
			if t.State != api.TaskRunning {
				continue
			}
			pl, ok := s.view.TaskPlacement(t.ID)
			if !ok {
				continue
			}
			out = append(out, preemption.TaskInfo{
				TaskID:      t.ID,
				JobID:       jid,
				User:        j.Spec.User,
				NodeName:    pl.NodeName,
				Res:         pl.Res,
				Priority:    j.Spec.Priority,
				StartTick:   t.StartTick,
				UsedTicks:   t.RunTicks,
				FairnessKey: s.ledger.UsageRatio(j.Spec.User),
			})
		}
	}
	return out
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
