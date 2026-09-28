package scheduler

import (
	"sort"
	"time"
)

// 公平共享与抢占。
//
// 调度排序：优先级降序 -> 提交序升序（被抢占者回到原始排队位置，可恢复、
// 有界等待）-> 用户归一化份额升序 -> ID 升序。同优先级的资源让出公平由
// 抢占规则负责，不与排队顺序混用。
// 归一化份额 = 用户 allocated（运行 + 预留）的标量值 / 权重，仅在其余键
// 相等时作平局裁决；预留份额仍计入配额核算，防止超额扩容。
//
// 抢占允许两类受害者：
//   - 严格更低优先级且仍在运行的任务（既有语义，不变）；
//   - 同优先级、但按用户/队列权重归一化后“已消耗资源份额”严格更多的作业。
//     已消耗份额是该作业全部任务各运行段（已结算段 + 当前运行段）的
//     资源×时间积分除以权重。谁长期占资源、谁先让；只被抢过短暂一段、
//     重新参与调度的作业不会因为“剩余份额少/已消耗归零”而被持续压着，
//     因为受害者资格看的是它自己已经跑出来的消耗，胜出者自身消耗更低时
//     才能发起抢占，消耗相近（相等）时不抢，退回到排队顺序。
//
// 抢占按整作业粒度贪心选取，找到可行集合后再做最小化剪枝。跨优先级抢占
// 不改变任何用户的 allocated 总量（运行份额转为等量预留），因此配额冲突
// 无法、也不会通过抢占绕过。

// jobWeight 返回用户权重 × 队列权重；缺省与 <=0 的权重均按 1 处理。
func (s *Scheduler) jobWeight(j *job) float64 {
	w := 1.0
	if v, ok := s.quota.UserWeight[j.spec.User]; ok && v > 0 {
		w *= v
	}
	if v, ok := s.quota.QueueWeight[j.spec.Queue]; ok && v > 0 {
		w *= v
	}
	return w
}

// consumedShare 返回作业已经“消耗掉”的资源份额（资源标量 × 运行时长，
// 秒），已结算各段与当前未结算运行段都计入；多轮抢占的分段在此自然累加，
// 不重复（每段只结算一次）也不遗漏（当前段实时计入）。
func (s *Scheduler) consumedShare(j *job, now time.Time) float64 {
	var c float64
	for _, t := range j.tasks {
		c += float64(t.runtime) * shareValue(t.spec.Resources)
		if t.state == TaskRunning && !t.startedAt.IsZero() {
			seg := now.Sub(t.startedAt)
			if seg < 0 {
				seg = 0
			}
			c += float64(seg) * shareValue(t.spec.Resources)
		}
	}
	return c
}

// normalizedConsumed 返回按用户/队列权重归一化的已消耗份额。
func (s *Scheduler) normalizedConsumed(j *job, now time.Time) float64 {
	return s.consumedShare(j, now) / s.jobWeight(j)
}

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
		// 同优先级严格按提交序排队：被抢占任务恢复后回到其原始排队位置，
		// 不会因为仍持有预留份额（或“剩余份额少”）而被后来的零消耗新作业
		// 无限插队，保证可恢复、有界等待；同优先级内的资源让出公平由
		// preemptVictims 的“已消耗份额”规则负责，二者不混用。
		if ja.submitSeq != jb.submitSeq {
			return ja.submitSeq < jb.submitSeq
		}
		sa, sb := s.normalizedShare(ja.spec.User), s.normalizedShare(jb.spec.User)
		if sa != sb {
			return sa < sb
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
	winnerConsumed := s.normalizedConsumed(j, s.now)
	for _, id := range s.order {
		v := s.jobs[id]
		if v.spec.ID == j.spec.ID || v.state == JobFinished || v.state == JobCanceled {
			continue
		}
		if len(runningTasksOf(v)) == 0 {
			continue
		}
		switch {
		case v.spec.Priority < j.spec.Priority:
			// 严格更低优先级：既有语义，总是可抢占。
			candidates = append(candidates, v)
		case v.spec.Priority == j.spec.Priority:
			// 同优先级：仅当受害者归一化已消耗份额严格更多时才可被让出；
			// 相等时不抢，避免平局抖动并保证消耗相近者公平共存。
			if s.normalizedConsumed(v, s.now) > winnerConsumed {
				candidates = append(candidates, v)
			}
		}
	}
	// 受害者选择顺序：
	// 1) 低优先级优先（跨优先级语义优先于同优先级公平）；
	// 2) 同层内归一化已消耗份额降序（长跑大户先让）；
	// 3) 运行任务少（少扰动）-> 提交较新 -> ID，仅作确定性平局裁决。
	sort.SliceStable(candidates, func(a, b int) bool {
		va, vb := candidates[a], candidates[b]
		if va.spec.Priority != vb.spec.Priority {
			return va.spec.Priority < vb.spec.Priority
		}
		ca, cb := s.normalizedConsumed(va, s.now), s.normalizedConsumed(vb, s.now)
		if ca != cb {
			return ca > cb
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
