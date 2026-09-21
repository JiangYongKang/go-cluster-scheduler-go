// Package topology models node-local resource state (CPU/memory per NUMA
// node plus discrete GPUs) and evaluates hard topology constraints:
// GPU model, NUMA locality and node (anti-)affinity.
//
// All constraints are hard. EvaluateTask never downgrades: an unsatisfiable
// node is returned together with a specific, distinguishable RejectReason.
// Placement selection is deterministic (lowest index NUMA/GPU, best-fit) so
// identical inputs produce identical placements.
package topology

import (
	"sort"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
)

// Placement records where one task was placed.
type Placement struct {
	TaskID   string
	JobID    string
	NodeName string
	NumaID   int
	GPUIDs   []int
	Res      api.Resources
}

type taskAlloc struct {
	placement Placement
	numaUsed  []api.Resources // CPU/memory taken from each NUMA
	gpuIdx    []int           // local GPU indices taken
}

type nodeState struct {
	node     *api.Node
	numaFree []api.Resources // free CPU/mem per NUMA
	gpuTaken []bool          // per local GPU index
	numaGPU  [][]int         // local GPU indices attached to each NUMA
	tasks    map[string]*taskAlloc
}

// View is the mutable allocation view of all nodes. The Scheduler serializes
// access to it; View itself is not expected to be used concurrently.
type View struct {
	nodes map[string]*nodeState
	order []string
	// taskID -> nodeName, enables idempotent free and O(1) lookup.
	taskNode map[string]string
}

// NewView builds a view from node definitions. Nodes with no explicit NUMA
// topology are treated as one synthetic NUMA holding all capacity/GPUs.
func NewView(nodes []*api.Node) *View {
	v := &View{nodes: map[string]*nodeState{}, taskNode: map[string]string{}}
	for _, n := range nodes {
		st := &nodeState{node: n, tasks: map[string]*taskAlloc{}}
		if len(n.NumaNodes) == 0 {
			st.numaFree = []api.Resources{{MilliCPU: n.Capacity.MilliCPU, MemoryMB: n.Capacity.MemoryMB}}
			ids := make([]int, len(n.GPUs))
			for i := range n.GPUs {
				ids[i] = i
			}
			st.numaGPU = [][]int{ids}
		} else {
			st.numaFree = make([]api.Resources, len(n.NumaNodes))
			st.numaGPU = make([][]int, len(n.NumaNodes))
			for i, nu := range n.NumaNodes {
				st.numaFree[i] = api.Resources{MilliCPU: nu.MilliCPU, MemoryMB: nu.MemoryMB}
				st.numaGPU[i] = append([]int{}, nu.GPUIDs...)
			}
		}
		st.gpuTaken = make([]bool, len(n.GPUs))
		v.nodes[n.Name] = st
		v.order = append(v.order, n.Name)
	}
	sort.Strings(v.order)
	return v
}

// NodeNames returns node names in deterministic order.
func (v *View) NodeNames() []string { return append([]string{}, v.order...) }

// Clone returns a deep copy of the mutable allocation state. The immutable
// *api.Node definitions are shared. It is used to simulate a gang plan
// before committing it, so a failed plan never leaves partial reservations.
func (v *View) Clone() *View {
	c := &View{
		nodes:    map[string]*nodeState{},
		order:    append([]string{}, v.order...),
		taskNode: map[string]string{},
	}
	for name, st := range v.nodes {
		cs := &nodeState{
			node:     st.node,
			numaFree: append([]api.Resources{}, st.numaFree...),
			gpuTaken: append([]bool{}, st.gpuTaken...),
			numaGPU:  make([][]int, len(st.numaGPU)),
			tasks:    map[string]*taskAlloc{},
		}
		for i, g := range st.numaGPU {
			cs.numaGPU[i] = append([]int{}, g...)
		}
		for id, ta := range st.tasks {
			cta := &taskAlloc{
				placement: ta.placement,
				numaUsed:  append([]api.Resources{}, ta.numaUsed...),
				gpuIdx:    append([]int{}, ta.gpuIdx...),
			}
			cta.placement.GPUIDs = append([]int{}, ta.placement.GPUIDs...)
			cs.tasks[id] = cta
		}
		c.nodes[name] = cs
	}
	for id, n := range v.taskNode {
		c.taskNode[id] = n
	}
	return c
}

// Node returns the underlying node definition.
func (v *View) Node(name string) *api.Node {
	if st := v.nodes[name]; st != nil {
		return st.node
	}
	return nil
}

func gpuMatches(n *api.Node, idx int, model string) bool {
	if model == "" {
		return true
	}
	return idx < len(n.GPUs) && n.GPUs[idx].Model == model
}

func (s *nodeState) freeGPUCount(model string) int {
	c := 0
	for i, taken := range s.gpuTaken {
		if !taken && gpuMatches(s.node, i, model) {
			c++
		}
	}
	return c
}

func (s *nodeState) hasModel(model string) bool {
	if model == "" {
		return true
	}
	for _, g := range s.node.GPUs {
		if g.Model == model {
			return true
		}
	}
	return false
}

// EvaluateTask reports whether a task fits a node and, if not, why.
func (v *View) EvaluateTask(node *api.Node, res api.Resources, req api.TopologyReq, jobID string) (bool, api.RejectReason) {
	st, ok := v.nodes[node.Name]
	if !ok {
		return false, api.RejectNodeNotReady
	}
	if st.node.State != api.NodeReady {
		return false, api.RejectNodeNotReady
	}

	// Anti-affinity: no co-location with tasks of the listed job IDs.
	for _, other := range req.AntiAffinity {
		for _, ta := range st.tasks {
			if ta.placement.JobID == other {
				return false, api.RejectAntiAffinity
			}
		}
	}

	// Node affinity: every required label must match.
	for k, val := range req.NodeAffinity {
		if st.node.Labels[k] != val {
			return false, api.RejectNodeAffinity
		}
	}

	// GPU model + availability.
	if res.GPUCount > 0 {
		if req.GPUModel != "" && !st.hasModel(req.GPUModel) {
			return false, api.RejectGPUModel
		}
		if int64(st.freeGPUCount(req.GPUModel)) < res.GPUCount {
			if req.GPUModel != "" && st.hasModel(req.GPUModel) {
				return false, api.RejectGPUExhausted
			}
			return false, api.RejectInsufficient
		}
	}

	// CPU/memory, optionally constrained to one NUMA locality.
	if req.NumaLocal {
		found := false
		for ni := range st.numaFree {
			f := st.numaFree[ni]
			if f.MilliCPU >= res.MilliCPU && f.MemoryMB >= res.MemoryMB {
				if res.GPUCount == 0 {
					found = true
					break
				}
				free := 0
				for _, gi := range st.numaGPU[ni] {
					if !st.gpuTaken[gi] && gpuMatches(st.node, gi, req.GPUModel) {
						free++
					}
				}
				if int64(free) >= res.GPUCount {
					found = true
					break
				}
			}
		}
		if !found {
			// Distinguish "no NUMA fits CPU/mem" from "GPUs lack locality".
			for ni := range st.numaFree {
				f := st.numaFree[ni]
				if f.MilliCPU >= res.MilliCPU && f.MemoryMB >= res.MemoryMB {
					return false, api.RejectGPUExhausted
				}
			}
			return false, api.RejectNumaLocal
		}
		return true, ""
	}

	var freeCPU, freeMem int64
	for _, f := range st.numaFree {
		freeCPU += f.MilliCPU
		freeMem += f.MemoryMB
	}
	if freeCPU < res.MilliCPU || freeMem < res.MemoryMB {
		return false, api.RejectInsufficient
	}
	return true, ""
}

// pickNumaForGPU finds the NUMA whose local free matching GPUs suffice.
func (s *nodeState) pickNumaForGPU(model string, need int64) int {
	for ni, gids := range s.numaGPU {
		free := 0
		for _, gi := range gids {
			if !s.gpuTaken[gi] && gpuMatches(s.node, gi, model) {
				free++
			}
		}
		if int64(free) >= need {
			return ni
		}
	}
	return -1
}

func (s *nodeState) takeFreeGPUs(model string, n int64, preferNuma int) []int {
	var out []int
	// Deterministic order: preferred NUMA first, then by GPU index.
	order := make([]int, 0, len(s.gpuTaken))
	if preferNuma >= 0 {
		order = append(order, s.numaGPU[preferNuma]...)
	}
	for ni, gids := range s.numaGPU {
		if ni == preferNuma {
			continue
		}
		order = append(order, gids...)
	}
	sort.Ints(order)
	for _, gi := range order {
		if int64(len(out)) >= n {
			break
		}
		if !s.gpuTaken[gi] && gpuMatches(s.node, gi, model) {
			s.gpuTaken[gi] = true
			out = append(out, gi)
		}
	}
	return out
}

// PlaceTask atomically reserves resources for a task on a node. It returns an
// error if the task is already placed or the node no longer fits.
func (v *View) PlaceTask(nodeName, jobID, taskID string, res api.Resources, req api.TopologyReq) error {
	st := v.nodes[nodeName]
	if st == nil {
		return api.NewError(api.ErrNotFound, "unknown node %q", nodeName)
	}
	if _, dup := v.taskNode[taskID]; dup {
		return api.NewError(api.ErrConflict, "task %q already placed", taskID)
	}
	if ok, reason := v.EvaluateTask(st.node, res, req, jobID); !ok {
		return api.NewError(api.ErrConflict, "task %q rejected by node %q: %s", taskID, nodeName, reason)
	}

	alloc := &taskAlloc{
		placement: Placement{TaskID: taskID, JobID: jobID, NodeName: nodeName, Res: res},
		numaUsed:  make([]api.Resources, len(st.numaFree)),
	}

	numaChoice := -1
	if req.NumaLocal || res.GPUCount > 0 {
		if req.NumaLocal {
			numaChoice = st.pickNumaForGPU(req.GPUModel, res.GPUCount)
			if numaChoice < 0 {
				// CPU/mem NUMA without GPUs.
				for ni, f := range st.numaFree {
					if f.MilliCPU >= res.MilliCPU && f.MemoryMB >= res.MemoryMB {
						numaChoice = ni
						break
					}
				}
			}
		} else {
			numaChoice = st.pickNumaForGPU(req.GPUModel, res.GPUCount)
		}
	}

	if res.GPUCount > 0 {
		gpus := st.takeFreeGPUs(req.GPUModel, res.GPUCount, numaChoice)
		alloc.gpuIdx = gpus
		for _, gi := range gpus {
			alloc.placement.GPUIDs = append(alloc.placement.GPUIDs, st.node.GPUs[gi].ID)
		}
	}

	needCPU := res.MilliCPU
	needMem := res.MemoryMB
	if req.NumaLocal && numaChoice >= 0 {
		st.numaFree[numaChoice].MilliCPU -= needCPU
		st.numaFree[numaChoice].MemoryMB -= needMem
		alloc.numaUsed[numaChoice] = api.Resources{MilliCPU: needCPU, MemoryMB: needMem}
		alloc.placement.NumaID = numaChoice
	} else {
		start := 0
		if numaChoice >= 0 {
			start = numaChoice
		}
		for i := range st.numaFree {
			ni := (start + i) % len(st.numaFree)
			takeCPU := min64(needCPU, st.numaFree[ni].MilliCPU)
			takeMem := min64(needMem, st.numaFree[ni].MemoryMB)
			st.numaFree[ni].MilliCPU -= takeCPU
			st.numaFree[ni].MemoryMB -= takeMem
			alloc.numaUsed[ni] = api.Resources{MilliCPU: takeCPU, MemoryMB: takeMem}
			needCPU -= takeCPU
			needMem -= takeMem
			if alloc.placement.NumaID == 0 && (takeCPU > 0 || takeMem > 0) {
				alloc.placement.NumaID = ni
			}
			if needCPU == 0 && needMem == 0 {
				break
			}
		}
	}

	st.tasks[taskID] = alloc
	v.taskNode[taskID] = nodeName
	return nil
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// FreeTask releases a task's reservation. It is idempotent.
func (v *View) FreeTask(taskID string) {
	nodeName, ok := v.taskNode[taskID]
	if !ok {
		return
	}
	st := v.nodes[nodeName]
	alloc := st.tasks[taskID]
	for ni, used := range alloc.numaUsed {
		st.numaFree[ni] = api.Add(st.numaFree[ni], used)
	}
	for _, gi := range alloc.gpuIdx {
		st.gpuTaken[gi] = false
	}
	delete(st.tasks, taskID)
	delete(v.taskNode, taskID)
}

// FreeOnNode evicts every task placed on a node (node failure). It returns
// the evicted placements and releases all reserved resources.
func (v *View) FreeOnNode(nodeName string) []Placement {
	st := v.nodes[nodeName]
	if st == nil {
		return nil
	}
	out := make([]Placement, 0, len(st.tasks))
	for id := range st.tasks {
		out = append(out, st.tasks[id].placement)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TaskID < out[j].TaskID })
	for _, p := range out {
		v.FreeTask(p.TaskID)
	}
	return out
}

// NodeFree reports remaining allocatable resources on a node.
func (v *View) NodeFree(nodeName string) api.Resources {
	st := v.nodes[nodeName]
	if st == nil {
		return api.Resources{}
	}
	var f api.Resources
	for _, nu := range st.numaFree {
		f = api.Add(f, nu)
	}
	for i, taken := range st.gpuTaken {
		if !taken {
			f.GPUCount++
			_ = i
		}
	}
	return f
}

// TaskPlacement returns the placement of a running task, if any.
func (v *View) TaskPlacement(taskID string) (Placement, bool) {
	nodeName, ok := v.taskNode[taskID]
	if !ok {
		return Placement{}, false
	}
	st := v.nodes[nodeName]
	return st.tasks[taskID].placement, true
}

// TasksOnNode lists task IDs currently placed on a node.
func (v *View) TasksOnNode(nodeName string) []string {
	st := v.nodes[nodeName]
	if st == nil {
		return nil
	}
	out := make([]string, 0, len(st.tasks))
	for id := range st.tasks {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
