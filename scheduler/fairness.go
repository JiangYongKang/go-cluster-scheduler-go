package scheduler

import (
	"sort"
	"time"
)

// 公平共享与抢占。
//
// 调度排序：优先级降序 -> 用户归一化份额升序 -> 提交序升序 -> ID 升序。
// 归一化份额 = 用户 allocated（运行 + 预留）的标量值 / 权重（权重缺省为 1）；
// 预留份额也计入，防止“被抢占越多越优先”的作弊。注意这里用的是瞬时持有份额
// 而非累计消耗：被抢占后正在等待重启的作业不持有运行份额以外的额外债务，
// 不会因为历史消耗多（结算后剩余份额少）而一直被压着起不来。
//
// 抢占只允许针对严格更低优先级且仍在运行的任务；按整作业粒度贪心选取，
// 找到可行集合后再做最小化剪枝。同优先级候选受害者之间按“已消耗资源份额”
// 排序：累计消耗的资源时间越多、按用户/队列权重归一化后份额越大的作业越先
// 被让出，避免“占得久、耗得多反而最安全”。抢占不改变任何用户的 allocated
// 总量（运行份额转为等量预留），因此配额冲突无法、也不会通过抢占绕过。

func shareValue(r Resources) float64 {
	return float64(r.CPU) + float64(r.Memory)/1024.0 + float64(r.GPU)*1024.0
}

// rtValue 是资源时间的标量值（量纲与 shareValue×时长 对应）。
func rtValue(rt ResourceTime) float64 {
	return float64(rt.CPUNanos) + float64(rt.MemoryNanos)/1024.0 + float64(rt.GPUNanos)*1024.0
}

func (s *Scheduler) userWeight(user string) float64 {
	if w, ok := s.quota.UserWeights[user]; ok && w > 0 {
		return w
	}
	return 1
}

func (s *Scheduler) queueWeight(queue string) float64 {
	if w, ok := s.quota.QueueWeights[queue]; ok && w > 0 {
		return w
	}
	return 1
}

func (s *Scheduler) normalizedShare(user string) float64 {
	return shareValue(s.userAllocated(user)) / s.userWeight(user)
}

// consumedShare 返回作业到 at 为止累计消耗资源时间按用户/队列权重归一化后
// 的份额：已结时段（完成/被抢占/取消/失败各桶）加当前仍在运行的时段。
// 同一作业多次被抢占、被抢占后重启的运行时间按段累加，既不重复也不漏记。
func (s *Scheduler) consumedShare(j *job, at time.Time) float64 {
	var sum ResourceTime
	for _, t := range j.tasks {
		sum = sum.add(taskBucketsLocked(t, at).Total)
	}
	return rtValue(sum) / (s.userWeight(j.spec.User) * s.queueWeight(j.spec.Queue))
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
	// 受害者选择顺序：
	//  1. 低优先级优先（仅严格更低优先级候选会进入此列表）；
	//  2. 同优先级内按“已消耗资源份额”降序：累计资源时间（按用户/队列权重
	//     归一化）越大的作业越先让出，长跑多耗者不再最安全；
	//  3. 份额相同按作业 ID 升序，保证确定性。
	sort.SliceStable(candidates, func(a, b int) bool {
		va, vb := candidates[a], candidates[b]
		if va.spec.Priority != vb.spec.Priority {
			return va.spec.Priority < vb.spec.Priority
		}
		ca, cb := s.consumedShare(va, s.now), s.consumedShare(vb, s.now)
		if ca != cb {
			return ca > cb
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
