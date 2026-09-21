package scheduler

import "sort"

// 公平共享与抢占。
//
// 调度排序：优先级降序 -> 用户归一化份额升序 -> 提交序升序 -> ID 升序。
// 归一化份额 = 用户 allocated（运行 + 预留）的标量值 / 权重（权重恒为 1，
// 即所有用户等权；预留份额也计入，防止“被抢占越多越优先”的作弊）。
//
// 抢占只允许针对严格更低优先级且仍在运行的任务；按整作业粒度贪心选取，
// 找到可行集合后再做最小化剪枝。抢占不改变任何用户的 allocated 总量
// （运行份额转为等量预留），因此配额冲突无法、也不会通过抢占绕过。

func shareValue(r Resources) float64 {
	return float64(r.CPU) + float64(r.Memory)/1024.0 + float64(r.GPU)*1024.0
}

func (s *Scheduler) normalizedShare(user string) float64 {
	return shareValue(s.userAllocated(user))
}

// schedulableJobs 返回存在待启动（排队/被抢占/待重试）任务且未终态的作业。
func (s *Scheduler) schedulableJobs() []*job {
	var out []*job
	for _, id := range s.order {
		j := s.jobs[id]
		if j.state == JobFinished || j.state == JobCanceled {
			continue
		}
		if len(pendingTasks(j)) > 0 {
			out = append(out, j)
		}
	}
	return out
}

// pendingOrder 按优先级 + 公平共享给出确定性的待调度作业顺序。
func (s *Scheduler) pendingOrder() []*job {
	out := s.schedulableJobs()
	sort.SliceStable(out, func(a, b int) bool {
		ja, jb := out[a], out[b]
		if ja.spec.Priority != jb.spec.Priority {
			return ja.spec.Priority > jb.spec.Priority
		}
		sa, sb := s.normalizedShare(ja.spec.User), s.normalizedShare(jb.spec.User)
		if sa != sb {
			return sa < sb
		}
		if ja.submitSeq != jb.submitSeq {
			return ja.submitSeq < jb.submitSeq
		}
		return ja.spec.ID < jb.spec.ID
	})
	return out
}

func runningTasksOf(j *job) []*task {
	var out []*task
	for _, t := range j.tasks {
		if t.state == TaskRunning {
			out = append(out, t)
		}
	}
	return out
}

// preemptVictims 尝试通过抢占严格低优先级任务为 j 腾出整组放置。
// 返回放置计划、受害者集合；失败时返回原因码与节点级细分原因。
func (s *Scheduler) preemptVictims(j *job) (map[*task]placeOn, map[*job]bool, RejectReason, string, map[string][]FilterReason) {
	candidates := make([]*job, 0)
	for _, id := range s.order {
		v := s.jobs[id]
		if v.spec.ID == j.spec.ID || v.state == JobFinished || v.state == JobCanceled {
			continue
		}
		if v.spec.Priority < j.spec.Priority && len(runningTasksOf(v)) > 0 {
			candidates = append(candidates, v)
		}
	}
	// 受害者选择顺序：低优先级优先 -> 运行任务少（少扰动）-> 新作业优先 -> ID。
	sort.SliceStable(candidates, func(a, b int) bool {
		va, vb := candidates[a], candidates[b]
		if va.spec.Priority != vb.spec.Priority {
			return va.spec.Priority < vb.spec.Priority
		}
		na, nb := len(runningTasksOf(va)), len(runningTasksOf(vb))
		if na != nb {
			return na < nb
		}
		if va.submitSeq != vb.submitSeq {
			return va.submitSeq > vb.submitSeq
		}
		return va.spec.ID < vb.spec.ID
	})

	sim := &simView{removed: map[string]bool{}}
	added := []*job{}
	var reasons map[string][]FilterReason
	feasible := false

	addJob := func(v *job) {
		for _, t := range runningTasksOf(v) {
			sim.removed[s.taskKey(v.spec.ID, t.spec.Name)] = true
		}
		added = append(added, v)
	}
	tryPlace := func() bool {
		_, r, ok := s.placeJob(j, sim)
		reasons, feasible = r, ok
		return ok
	}

	// 基线：不抢占先试一次（与普通放置等价），确保无受害者时原因仍可区分。
	if tryPlace() {
		return nil, nil, ReasonOK, "no preemption needed", nil
	}
	baseReasons := reasons
	for _, v := range candidates {
		addJob(v)
		if tryPlace() {
			break
		}
	}
	if !feasible {
		// 即便移除全部更低优先级任务仍放不下：区分硬性约束冲突与容量不足。
		final := reasons
		if len(final) == 0 {
			final = baseReasons
		}
		return nil, nil, s.classifyUnschedulable(final),
			s.describeFailure(j, final), final
	}

	// 最小化剪枝：逆序尝试撤销多余受害者，保持确定性。
	alive := make([]bool, len(added))
	for i := range alive {
		alive[i] = true
	}
	for i := len(added) - 1; i >= 0; i-- {
		for _, t := range runningTasksOf(added[i]) {
			delete(sim.removed, s.taskKey(added[i].spec.ID, t.spec.Name))
		}
		if _, r, ok := s.placeJob(j, sim); ok {
			reasons = r
			alive[i] = false
		} else {
			for _, t := range runningTasksOf(added[i]) {
				sim.removed[s.taskKey(added[i].spec.ID, t.spec.Name)] = true
			}
		}
	}
	victims := map[*job]bool{}
	var ids []string
	for i, v := range added {
		if alive[i] {
			victims[v] = true
			ids = append(ids, v.spec.ID)
		}
	}
	sort.Strings(ids)
	// 以最小集合再放置一次，拿到对应计划。
	p, r, ok := s.placeJob(j, sim)
	if !ok {
		return nil, nil, ReasonPreemptionNotEnough, "internal: minimized preemption set infeasible", r
	}
	return p, victims, ReasonOK, sprintf("preempt lower-priority jobs: %v", ids), nil
}

// classifyUnschedulable 依据节点级原因区分硬性约束冲突与容量/抢占不足。
func (s *Scheduler) classifyUnschedulable(reasons map[string][]FilterReason) RejectReason {
	if len(reasons) == 0 {
		return ReasonNoCandidates
	}
	for _, rs := range reasons {
		for _, r := range rs {
			switch r {
			case FilterCPU, FilterMemory, FilterGPUBusy, FilterNUMAExhaust, FilterTaskCap:
				return ReasonPreemptionNotEnough
			}
		}
	}
	return ReasonConstraintConflict
}

func (s *Scheduler) describeFailure(j *job, reasons map[string][]FilterReason) string {
	if len(reasons) == 0 {
		return sprintf("job %s: no candidate nodes registered", j.spec.ID)
	}
	return sprintf("job %s cannot be gang-scheduled even after maximal preemption: %s",
		j.spec.ID, summarizeReasons(reasons))
}
