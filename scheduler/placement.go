package scheduler

import "sort"

// 拓扑放置：对作业的全部待启动任务做候选节点过滤与整体装箱。
// 任一任务无可行放置则整个作业不启动（gang），并给出每个节点被排除的
// 细分原因；绝不静默降级到违反约束的节点。
//
// 钉设备（TaskSpec.GPUDevice）在有无资源域两种节点上语义一致：钉哪块卡就
// 落在哪块卡，设备不存在给 GPU_ABSENT、已占用给 GPU_BUSY；有资源域时设备
// 不属于指定域给 GPU_ZONE_MISMATCH。任何情况下都不退回同节点其它设备。

// simView 是放置预演视图：removed 中的任务（拟抢占的受害者）
// 被视为已停止，节点可用资源相应回补，但调度器真实账本不变。
type simView struct {
	removed map[string]bool
}

// placeOn 是单个任务的放置决议。
type placeOn struct {
	n      *node
	numaID int // -1 表示不使用 NUMA 域
	gpu    string
}

// nodeAcc 是 DFS 过程中“本作业已分配到某节点”的累计占用。
type nodeAcc struct {
	cpu, mem int64
	count    int
	gpus     map[string]bool
	zoneCPU  map[int]int64
	zoneMem  map[int]int64
}

func newNodeAcc() *nodeAcc {
	return &nodeAcc{gpus: map[string]bool{}, zoneCPU: map[int]int64{}, zoneMem: map[int]int64{}}
}

// effView 计算节点在预演视图下的有效已用量（减去拟移除任务）。
type effNode struct {
	cpu, mem int64
	count    int
	busyGPU  map[string]bool
	zoneCPU  map[int]int64
	zoneMem  map[int]int64
	zoneGPU  map[int]map[string]bool
}

func (s *Scheduler) effNode(n *node, sim *simView) effNode {
	e := effNode{
		cpu: n.usedCPU, mem: n.usedMem, count: n.taskCount,
		busyGPU: map[string]bool{},
		zoneCPU: map[int]int64{}, zoneMem: map[int]int64{},
		zoneGPU: map[int]map[string]bool{},
	}
	for g := range n.busyGPUs {
		e.busyGPU[g] = true
	}
	for z, v := range n.numaCPU {
		e.zoneCPU[z] = v
	}
	for z, v := range n.numaMem {
		e.zoneMem[z] = v
	}
	for z, gs := range n.numaGPU {
		m := map[string]bool{}
		for g := range gs {
			m[g] = true
		}
		e.zoneGPU[z] = m
	}
	if sim == nil {
		return e
	}
	for key := range sim.removed {
		j, t := s.findTaskLocked(key)
		if j == nil || t == nil || t.state != TaskRunning || t.node != n.spec.Name {
			continue
		}
		r := t.spec.Resources
		e.cpu -= r.CPU
		e.mem -= r.Memory
		e.count--
		delete(e.busyGPU, t.gpu)
		if t.numaID >= 0 {
			e.zoneCPU[t.numaID] -= r.CPU
			e.zoneMem[t.numaID] -= r.Memory
			if t.gpu != "" {
				if gs := e.zoneGPU[t.numaID]; gs != nil {
					delete(gs, t.gpu)
				}
			}
		}
	}
	return e
}

// filterNode 在给定预演视图与本作业累计占用下判断任务能否放入节点，
// 返回全部不满足的细分原因（nil/空表示可行）。
func (s *Scheduler) filterNode(n *node, t TaskSpec, j *job, sim *simView, acc *nodeAcc) []FilterReason {
	var reasons []FilterReason
	if n.state != NodeReady {
		return []FilterReason{FilterNodeNotReady}
	}
	// 硬性标签亲和。
	if sel := j.spec.Affinity.Required; sel != nil {
		for k, v := range sel.MatchLabels {
			if n.spec.Labels[k] != v {
				reasons = append(reasons, FilterLabelMismatch)
				break
			}
		}
		for _, k := range sel.NoScheduleKeys {
			if _, ok := n.spec.Labels[k]; ok {
				reasons = append(reasons, FilterLabelMismatch)
				break
			}
		}
	}
	// 反亲和：反亲和作业的运行任务所在节点必须排除（考虑拟抢占移除）。
	if len(j.spec.Affinity.AntiAffinityWith) > 0 {
		bad := map[string]bool{}
		for _, oid := range j.spec.Affinity.AntiAffinityWith {
			if other, ok := s.jobs[oid]; ok {
				for _, ot := range other.tasks {
					if ot.state != TaskRunning || ot.node != n.spec.Name {
						continue
					}
					if sim != nil && sim.removed[s.taskKey(oid, ot.spec.Name)] {
						continue
					}
					bad[oid] = true
				}
			}
		}
		if len(bad) > 0 {
			reasons = append(reasons, FilterAntiAffinity)
		}
	}
	e := s.effNode(n, sim)
	addCPU := acc.cpu
	addMem := acc.mem
	// 节点总量。
	if e.cpu+addCPU+t.Resources.CPU > n.spec.CPU {
		reasons = append(reasons, FilterCPU)
	}
	if e.mem+addMem+t.Resources.Memory > n.spec.Memory {
		reasons = append(reasons, FilterMemory)
	}
	if n.spec.MaxTasks > 0 && e.count+acc.count+1 > n.spec.MaxTasks {
		reasons = append(reasons, FilterTaskCap)
	}
	// 指定 GPU 设备：设备必须真实存在、空闲、且与资源域严格一致；
	// 任何一条不满足都只报对应原因，绝不退回同节点其它设备或别的域。
	if t.GPUDevice != "" {
		if _, exists := gpuOnNode(n, t.GPUDevice); !exists {
			reasons = append(reasons, FilterGPUAbsent)
		} else {
			if e.busyGPU[t.GPUDevice] || acc.gpus[t.GPUDevice] {
				reasons = append(reasons, FilterGPUBusy)
			}
			// 设备的资源域归属必须与申请域一致：
			//   指定 NUMAID 时设备必须属于该域；
			//   NUMA 节点上未指定 NUMAID 时，CPU/内存/GPU 仍必须同域，
			//   设备只属于其归属域，不允许落到别的域或节点总量账本。
			if len(n.spec.NUMA) > 0 {
				if zid, ok := gpuZone(n, t.GPUDevice); !ok {
					reasons = append(reasons, FilterGPUAbsent)
				} else if t.NUMAID >= 0 && zid != t.NUMAID {
					reasons = append(reasons, FilterGPUZoneMismatch)
				} else if t.NUMAID < 0 {
					// 未指定域：设备归属域即唯一资源域，CPU/内存须同域容纳。
					z := zoneByID(n, zid)
					if e.zoneCPU[zid]+acc.zoneCPU[zid]+t.Resources.CPU > z.CPUs ||
						e.zoneMem[zid]+acc.zoneMem[zid]+t.Resources.Memory > z.Mem {
						reasons = append(reasons, FilterNUMAExhaust)
					}
				}
			}
		}
	}
	// NUMA 域约束。
	if t.NUMAID >= 0 {
		z := zoneByID(n, t.NUMAID)
		if z == nil {
			reasons = append(reasons, FilterNUMAAbsent)
		} else {
			if e.zoneCPU[t.NUMAID]+acc.zoneCPU[t.NUMAID]+t.Resources.CPU > z.CPUs {
				reasons = append(reasons, FilterNUMAExhaust)
			}
			if e.zoneMem[t.NUMAID]+acc.zoneMem[t.NUMAID]+t.Resources.Memory > z.Mem {
				reasons = append(reasons, FilterNUMAExhaust)
			}
			if t.Resources.GPU == 1 {
				gid := t.GPUDevice
				if gid == "" {
					gid = pickFreeGPU(n, t.NUMAID, e, acc)
				} else if zid, ok := gpuZone(n, gid); !ok || zid != t.NUMAID {
					// 钉了设备但设备不在该域：域不一致，已在上方钉设备分支
					// 记 GPU_ZONE_MISMATCH；这里不再挑别的卡。
					gid = ""
				} else if e.busyGPU[gid] || acc.gpus[gid] {
					gid = "" // 占用原因已在钉设备分支记录
				}
				if gid == "" && t.GPUDevice == "" {
					// 未钉设备：区分域内无设备 vs 域内全部繁忙。
					if len(z.GPUs) == 0 {
						reasons = append(reasons, FilterGPUAbsent)
					} else {
						reasons = append(reasons, FilterGPUBusy)
					}
				}
			}
		}
	} else if t.Resources.GPU == 1 && t.GPUDevice == "" {
		// 申请 GPU 但不钉设备：NUMA 节点上必须存在至少一个含空闲 GPU 的域。
		if len(n.spec.NUMA) > 0 {
			found := false
			for _, z := range n.spec.NUMA {
				if g := pickFreeGPU(n, z.ID, e, acc); g != "" &&
					e.zoneCPU[z.ID]+acc.zoneCPU[z.ID]+t.Resources.CPU <= z.CPUs &&
					e.zoneMem[z.ID]+acc.zoneMem[z.ID]+t.Resources.Memory <= z.Mem {
					found = true
					break
				}
			}
			if !found {
				reasons = append(reasons, FilterGPUBusy)
			}
		} else {
			if len(n.spec.GPUs) == 0 {
				reasons = append(reasons, FilterGPUAbsent)
			} else if freeGPUs(n, e, acc) == 0 {
				reasons = append(reasons, FilterGPUBusy)
			}
		}
	}
	return dedupReasons(reasons)
}

// optionsForTask 枚举任务在所有节点上确定性排序后的可行放置选项。
func (s *Scheduler) optionsForTask(t *task, j *job, sim *simView, accs map[string]*nodeAcc) []placeOn {
	names := make([]string, 0, len(s.nodes))
	for nm := range s.nodes {
		names = append(names, nm)
	}
	sort.Slice(names, func(a, b int) bool {
		pa := s.preferredScore(s.nodes[names[a]], j)
		pb := s.preferredScore(s.nodes[names[b]], j)
		if pa != pb {
			return pa > pb
		}
		return names[a] < names[b]
	})
	var opts []placeOn
	for _, nm := range names {
		n := s.nodes[nm]
		acc := accs[nm]
		if acc == nil {
			acc = newNodeAcc()
		}
		reasons := s.filterNode(n, t.spec, j, sim, acc)
		if len(reasons) > 0 {
			continue
		}
		opts = append(opts, s.placementChoices(n, t.spec, sim, acc)...)
	}
	return opts
}

// placementChoices 返回同一节点上可行的 (NUMA 域, GPU) 具体选择（按域 ID、设备 ID 排序）。
func (s *Scheduler) placementChoices(n *node, t TaskSpec, sim *simView, acc *nodeAcc) []placeOn {
	e := s.effNode(n, sim)
	base := placeOn{n: n, numaID: -1}
	if t.Resources.GPU == 0 && t.NUMAID < 0 {
		return []placeOn{base}
	}
	if t.NUMAID >= 0 {
		z := zoneByID(n, t.NUMAID)
		if z == nil {
			return nil
		}
		gid := t.GPUDevice
		if t.Resources.GPU == 1 && gid == "" {
			gid = pickFreeGPU(n, t.NUMAID, e, acc)
			if gid == "" {
				return nil
			}
		}
		if t.Resources.GPU == 1 && gid != "" {
			// 钉设备：设备必须恰好属于该域且空闲，否则该节点无可行放置。
			zid, ok := gpuZone(n, gid)
			if !ok || zid != t.NUMAID || e.busyGPU[gid] || acc.gpus[gid] {
				return nil
			}
		}
		return []placeOn{{n: n, numaID: t.NUMAID, gpu: gid}}
	}
	// NUMA 节点上钉了设备但未指定域：唯一合法放置是设备的归属域，
	// CPU/内存与 GPU 必须同域；不枚举其它域，更不回退到别的设备。
	if t.Resources.GPU == 1 && t.GPUDevice != "" && len(n.spec.NUMA) > 0 {
		zid, ok := gpuZone(n, t.GPUDevice)
		if !ok {
			return nil
		}
		z := zoneByID(n, zid)
		if z == nil || e.busyGPU[t.GPUDevice] || acc.gpus[t.GPUDevice] {
			return nil
		}
		if e.zoneCPU[zid]+acc.zoneCPU[zid]+t.Resources.CPU > z.CPUs ||
			e.zoneMem[zid]+acc.zoneMem[zid]+t.Resources.Memory > z.Mem {
			return nil
		}
		return []placeOn{{n: n, numaID: zid, gpu: t.GPUDevice}}
	}
	// 申请 GPU、不钉 NUMA：NUMA 节点逐域枚举；平铺（无资源域）节点上
	// 钉设备必须落在指定卡，未钉设备才取编号最小的空闲卡，绝不退回其它卡。
	if len(n.spec.NUMA) > 0 {
		var out []placeOn
		zones := append([]NUMAZone(nil), n.spec.NUMA...)
		sort.Slice(zones, func(a, b int) bool { return zones[a].ID < zones[b].ID })
		for _, z := range zones {
			if e.zoneCPU[z.ID]+acc.zoneCPU[z.ID]+t.Resources.CPU > z.CPUs ||
				e.zoneMem[z.ID]+acc.zoneMem[z.ID]+t.Resources.Memory > z.Mem {
				continue
			}
			g := pickFreeGPU(n, z.ID, e, acc)
			if g == "" {
				continue
			}
			out = append(out, placeOn{n: n, numaID: z.ID, gpu: g})
		}
		return out
	}
	if t.Resources.GPU == 1 {
		// 平铺节点钉设备：设备即唯一合法放置。设备不存在/已占用已在
		// filterNode 分别给出 GPU_ABSENT / GPU_BUSY，这里不改选其它卡。
		if t.GPUDevice != "" {
			if _, exists := gpuOnNode(n, t.GPUDevice); !exists ||
				e.busyGPU[t.GPUDevice] || acc.gpus[t.GPUDevice] {
				return nil
			}
			return []placeOn{{n: n, numaID: -1, gpu: t.GPUDevice}}
		}
		var ids []string
		for _, g := range n.spec.GPUs {
			if !e.busyGPU[g.ID] && !acc.gpus[g.ID] {
				ids = append(ids, g.ID)
			}
		}
		sort.Strings(ids)
		if len(ids) == 0 {
			return nil
		}
		return []placeOn{{n: n, numaID: -1, gpu: ids[0]}}
	}
	return []placeOn{base}
}

// placeJob 尝试把作业全部待启动任务整体放入集群。
// 返回每个任务的放置、各节点被排除原因聚合，以及是否成功。
func (s *Scheduler) placeJob(j *job, sim *simView) (map[*task]placeOn, map[string][]FilterReason, bool) {
	tasks := sortedPendingTasks(j)
	accs := map[string]*nodeAcc{}
	plan := map[*task]placeOn{}
	// 每个节点被本作业各任务（在回溯最终失败点处）排除的细分原因，跨任务去重。
	reasons := map[string]map[FilterReason]bool{}

	addReasons := func(rs map[string][]FilterReason) {
		for nm, list := range rs {
			if reasons[nm] == nil {
				reasons[nm] = map[FilterReason]bool{}
			}
			for _, r := range list {
				reasons[nm][r] = true
			}
		}
	}

	var dfs func(idx int) bool
	dfs = func(idx int) bool {
		if idx == len(tasks) {
			return true
		}
		t := tasks[idx]
		opts := s.optionsForTask(t, j, sim, accs)
		if len(opts) == 0 {
			// 当前任务在所有节点都不可行：记录每个节点对该任务的排除原因。
			for _, nm := range sortedNodeNames(s.nodes) {
				n := s.nodes[nm]
				acc := accs[nm]
				if acc == nil {
					acc = newNodeAcc()
				}
				if rs := s.filterNode(n, t.spec, j, sim, acc); len(rs) > 0 {
					addReasons(map[string][]FilterReason{nm: rs})
				}
			}
			return false
		}
		for _, op := range opts {
			nm := op.n.spec.Name
			acc := accs[nm]
			if acc == nil {
				acc = newNodeAcc()
				accs[nm] = acc
			}
			applyAcc(acc, t.spec, op)
			plan[t] = op
			if dfs(idx + 1) {
				return true
			}
			undoAcc(acc, t.spec, op)
			delete(plan, t)
		}
		return false
	}
	if dfs(0) {
		return plan, nil, true
	}
	out := map[string][]FilterReason{}
	for nm, set := range reasons {
		list := make([]FilterReason, 0, len(set))
		for r := range set {
			list = append(list, r)
		}
		out[nm] = dedupReasons(list)
	}
	return nil, out, false
}

func applyAcc(acc *nodeAcc, t TaskSpec, op placeOn) {
	acc.cpu += t.Resources.CPU
	acc.mem += t.Resources.Memory
	acc.count++
	if op.gpu != "" {
		acc.gpus[op.gpu] = true
	}
	if op.numaID >= 0 {
		acc.zoneCPU[op.numaID] += t.Resources.CPU
		acc.zoneMem[op.numaID] += t.Resources.Memory
	}
}

func undoAcc(acc *nodeAcc, t TaskSpec, op placeOn) {
	acc.cpu -= t.Resources.CPU
	acc.mem -= t.Resources.Memory
	acc.count--
	if op.gpu != "" {
		delete(acc.gpus, op.gpu)
	}
	if op.numaID >= 0 {
		acc.zoneCPU[op.numaID] -= t.Resources.CPU
		acc.zoneMem[op.numaID] -= t.Resources.Memory
	}
}

// preferredScore 返回节点对作业软亲和的匹配数（仅影响候选排序，不影响可行性）。
func (s *Scheduler) preferredScore(n *node, j *job) int {
	sel := j.spec.Affinity.Preferred
	if sel == nil {
		return 0
	}
	score := 0
	for k, v := range sel.MatchLabels {
		if n.spec.Labels[k] == v {
			score++
		}
	}
	return score
}

func gpuOnNode(n *node, id string) (GPUDevice, bool) {
	for _, g := range n.spec.GPUs {
		if g.ID == id {
			return g, true
		}
	}
	return GPUDevice{}, false
}

// gpuZone 返回设备在该 NUMA 节点上的归属域 ID；非 NUMA 节点或设备不存在时 ok=false。
// 域归属以 NUMAZone.GPUs 的互引关系为准（validateNode 保证每块设备恰属一域）。
func gpuZone(n *node, id string) (int, bool) {
	for i := range n.spec.NUMA {
		for _, gid := range n.spec.NUMA[i].GPUs {
			if gid == id {
				return n.spec.NUMA[i].ID, true
			}
		}
	}
	return 0, false
}

func zoneByID(n *node, id int) *NUMAZone {
	for i := range n.spec.NUMA {
		if n.spec.NUMA[i].ID == id {
			return &n.spec.NUMA[i]
		}
	}
	return nil
}

func pickFreeGPU(n *node, zoneID int, e effNode, acc *nodeAcc) string {
	z := zoneByID(n, zoneID)
	if z == nil {
		return ""
	}
	var ids []string
	for _, gid := range z.GPUs {
		busy := e.busyGPU[gid] || acc.gpus[gid]
		if gs := e.zoneGPU[zoneID]; gs != nil && gs[gid] {
			busy = true
		}
		if !busy {
			ids = append(ids, gid)
		}
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		return ""
	}
	return ids[0]
}

func freeGPUs(n *node, e effNode, acc *nodeAcc) int {
	cnt := 0
	for _, g := range n.spec.GPUs {
		if !e.busyGPU[g.ID] && !acc.gpus[g.ID] {
			cnt++
		}
	}
	return cnt
}

// sortedPendingTasks 将待启动任务按约束严格度确定性排序：
// 钉 GPU/钉 NUMA 优先，其次申请 GPU，再次资源需求降序，最后按名称。
func sortedPendingTasks(j *job) []*task {
	out := pendingTasks(j)
	sort.Slice(out, func(a, b int) bool {
		ta, tb := out[a].spec, out[b].spec
		sa := constraintRank(ta)
		sb := constraintRank(tb)
		if sa != sb {
			return sa > sb
		}
		dema := ta.Resources.CPU + ta.Resources.Memory
		demb := tb.Resources.CPU + tb.Resources.Memory
		if dema != demb {
			return dema > demb
		}
		return ta.Name < tb.Name
	})
	return out
}

func constraintRank(t TaskSpec) int {
	rank := 0
	if t.GPUDevice != "" {
		rank += 4
	}
	if t.NUMAID >= 0 {
		rank += 2
	}
	if t.Resources.GPU > 0 {
		rank++
	}
	return rank
}

func sortedNodeNames(m map[string]*node) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func dedupReasons(in []FilterReason) []FilterReason {
	seen := map[FilterReason]bool{}
	out := in[:0]
	for _, r := range in {
		if seen[r] {
			continue
		}
		seen[r] = true
		out = append(out, r)
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}
