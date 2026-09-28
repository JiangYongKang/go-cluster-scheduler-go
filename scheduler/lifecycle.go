package scheduler

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// validateNode 校验节点拓扑自洽：名称非空、资源非负、GPU/NUMA 标识唯一且互引一致。
func validateNode(spec NodeSpec) error {
	if spec.Name == "" {
		return schedError("AddNode", ReasonInvalidJob, "node name must not be empty")
	}
	if spec.CPU < 0 || spec.Memory < 0 {
		return schedError("AddNode", ReasonInvalidJob, "node %s has negative capacity", spec.Name)
	}
	if spec.MaxTasks < 0 {
		return schedError("AddNode", ReasonInvalidJob, "node %s has negative MaxTasks", spec.Name)
	}
	gpus := map[string]bool{}
	for _, g := range spec.GPUs {
		if g.ID == "" {
			return schedError("AddNode", ReasonInvalidJob, "node %s has GPU with empty ID", spec.Name)
		}
		if gpus[g.ID] {
			return schedError("AddNode", ReasonInvalidJob, "node %s has duplicate GPU %s", spec.Name, g.ID)
		}
		gpus[g.ID] = true
	}
	zones := map[int]bool{}
	var zCPU, zMem int64
	zoneGPU := map[string]bool{}
	for _, z := range spec.NUMA {
		if z.ID < 0 {
			return schedError("AddNode", ReasonInvalidJob, "node %s has negative NUMA id", spec.Name)
		}
		if zones[z.ID] {
			return schedError("AddNode", ReasonInvalidJob, "node %s has duplicate NUMA zone %d", spec.Name, z.ID)
		}
		zones[z.ID] = true
		zCPU += z.CPUs
		zMem += z.Mem
		for _, gid := range z.GPUs {
			if !gpus[gid] {
				return schedError("AddNode", ReasonInvalidJob,
					"node %s NUMA zone %d references unknown GPU %s", spec.Name, z.ID, gid)
			}
			if zoneGPU[gid] {
				return schedError("AddNode", ReasonInvalidJob,
					"node %s GPU %s assigned to multiple NUMA zones", spec.Name, gid)
			}
			zoneGPU[gid] = true
		}
	}
	if len(spec.NUMA) > 0 {
		if zCPU != spec.CPU || zMem != spec.Memory {
			return schedError("AddNode", ReasonInvalidJob,
				"node %s NUMA zone totals (cpu=%d mem=%d) must equal node totals (cpu=%d mem=%d)",
				spec.Name, zCPU, zMem, spec.CPU, spec.Memory)
		}
		for _, g := range spec.GPUs {
			if !zoneGPU[g.ID] {
				return schedError("AddNode", ReasonInvalidJob,
					"node %s GPU %s not assigned to any NUMA zone", spec.Name, g.ID)
			}
		}
	}
	return nil
}

// validate 校验作业：字段完整、任务名唯一、资源合法、NUMA/GPU 请求一致。
func validate(spec JobSpec) error {
	const op = "Submit"
	if spec.ID == "" {
		return schedError(op, ReasonInvalidJob, "job id must not be empty")
	}
	if spec.User == "" || spec.Queue == "" {
		return schedError(op, ReasonInvalidJob, "job %s user and queue must not be empty", spec.ID)
	}
	if len(spec.Tasks) == 0 {
		return schedError(op, ReasonInvalidJob, "job %s must contain at least one task", spec.ID)
	}
	if spec.MaxRetries < 0 {
		return schedError(op, ReasonInvalidJob, "job %s MaxRetries must not be negative", spec.ID)
	}
	names := map[string]bool{}
	for _, t := range spec.Tasks {
		if t.Name == "" {
			return schedError(op, ReasonInvalidJob, "job %s has task with empty name", spec.ID)
		}
		if names[t.Name] {
			return schedError(op, ReasonInvalidJob, "job %s has duplicate task name %s", spec.ID, t.Name)
		}
		names[t.Name] = true
		r := t.Resources
		if r.CPU < 0 || r.Memory < 0 || r.GPU < 0 {
			return schedError(op, ReasonInvalidJob, "job %s task %s has negative resources", spec.ID, t.Name)
		}
		if r.GPU > 1 {
			return schedError(op, ReasonInvalidJob, "job %s task %s requests %d GPUs; at most 1 per task",
				spec.ID, t.Name, r.GPU)
		}
		if t.GPUDevice != "" && r.GPU != 1 {
			return schedError(op, ReasonInvalidJob,
				"job %s task %s pins GPU %s but does not request exactly 1 GPU", spec.ID, t.Name, t.GPUDevice)
		}
		if t.NUMAID < -1 {
			return schedError(op, ReasonInvalidJob,
				"job %s task %s has invalid NUMAID %d (use -1 for unspecified)", spec.ID, t.Name, t.NUMAID)
		}
	}
	return nil
}

// committed 记录一次整组启动中已落账的任务，用于失败时完全回滚。
type committed struct {
	t    *task
	n    *node
	op   placeOn
	held bool // 启动前是否持有预留（回滚时需恢复预留而非直接释放）
}

// startTasks 将整组放置原子落账：份额计入（预留转运行或新计入）、节点占用、
// 任务运行态。任何一步失败则回滚全部已做变更并返回错误，绝不残留半启动状态。
func (s *Scheduler) startTasks(j *job, plan map[*task]placeOn, at time.Time) ([]string, error) {
	tasks := sortedPendingTasks(j)
	done := make([]committed, 0, len(tasks))
	rollback := func() {
		for i := len(done) - 1; i >= 0; i-- {
			c := done[i]
			key := s.taskKey(j.spec.ID, c.t.spec.Name)
			undoNodeCommit(c.n, c.t.spec, c.op)
			delete(c.n.running, key)
			if c.held {
				s.userUsed[j.spec.User] = s.userUsed[j.spec.User].sub(c.t.spec.Resources)
				s.queueUsed[j.spec.Queue] = s.queueUsed[j.spec.Queue].sub(c.t.spec.Resources)
				s.holdPreempt(key, c.t.spec.Resources) // 恢复预留
			} else {
				s.release(key)
			}
			c.t.state = TaskPending
			c.t.node, c.t.gpu = "", ""
			c.t.numaID = c.t.spec.NUMAID
		}
	}
	names := make([]string, 0, len(tasks))
	for _, t := range tasks {
		op, ok := plan[t]
		if !ok {
			rollback()
			return nil, schedError("startTasks", ReasonNothingScheduled,
				"job %s task %s missing placement", j.spec.ID, t.spec.Name)
		}
		key := s.taskKey(j.spec.ID, t.spec.Name)
		_, wasHeld := s.preemptHold[key]
		if wasHeld {
			if err := s.unholdToCharge(key); err != nil {
				rollback()
				return nil, err
			}
		} else {
			if err := s.charge(key, t.spec.Resources); err != nil {
				rollback()
				return nil, err
			}
		}
		commitNode(op.n, t.spec, op)
		op.n.running[key] = t
		t.state = TaskRunning
		t.node = op.n.spec.Name
		t.numaID = op.numaID
		t.gpu = op.gpu
		t.startedAt = at
		done = append(done, committed{t: t, n: op.n, op: op, held: wasHeld})
		names = append(names, t.spec.Name)
		s.logLocked(Decision{At: at, Kind: DecisionTaskStart, JobID: j.spec.ID,
			TaskID: t.spec.Name, Node: op.n.spec.Name,
			Detail: sprintf("start numa=%d gpu=%s res=%s (gang member %d/%d)",
				op.numaID, op.gpu, resString(t.spec.Resources), len(names), len(tasks))})
	}
	sort.Strings(names)
	if j.firstRun.IsZero() {
		j.firstRun = at
	}
	s.recomputeJobState(j)
	return names, nil
}

// commitNode 把任务占用写入节点账本。
func commitNode(n *node, t TaskSpec, op placeOn) {
	n.usedCPU += t.Resources.CPU
	n.usedMem += t.Resources.Memory
	n.taskCount++
	if op.gpu != "" {
		n.busyGPUs[op.gpu] = true
	}
	if op.numaID >= 0 {
		n.numaCPU[op.numaID] += t.Resources.CPU
		n.numaMem[op.numaID] += t.Resources.Memory
		if op.gpu != "" {
			if n.numaGPU[op.numaID] == nil {
				n.numaGPU[op.numaID] = map[string]bool{}
			}
			n.numaGPU[op.numaID][op.gpu] = true
		}
	}
}

// undoNodeCommit 撤销 commitNode 的占用（回滚用）。
func undoNodeCommit(n *node, t TaskSpec, op placeOn) {
	n.usedCPU -= t.Resources.CPU
	n.usedMem -= t.Resources.Memory
	n.taskCount--
	if op.gpu != "" {
		delete(n.busyGPUs, op.gpu)
	}
	if op.numaID >= 0 {
		n.numaCPU[op.numaID] -= t.Resources.CPU
		n.numaMem[op.numaID] -= t.Resources.Memory
		if op.gpu != "" && n.numaGPU[op.numaID] != nil {
			delete(n.numaGPU[op.numaID], op.gpu)
		}
	}
}

// settleTask 结算任务自上次启动以来的运行时段到累计运行时。
// 只做时长结算，不修改任务状态或资源记账；返回本次结算的时段长度。
// 同时把该时段的资源时间按终局原因计入唯一分桶（被抢占/取消/完成/失联
// 各归各桶），多次抢占各结算各的时段，累计相加，既不重复也不丢失。
func (s *Scheduler) settleTask(j *job, t *task, at time.Time, reason TerminalReason) time.Duration {
	var seg time.Duration
	if !t.startedAt.IsZero() {
		if at.Before(t.startedAt) {
			at = t.startedAt
		}
		seg = at.Sub(t.startedAt)
		if seg < 0 {
			seg = 0
		}
		t.runtime += seg
		t.startedAt = time.Time{}
	}
	if seg > 0 {
		t.rt.addSettled(reason, resourceTimeOf(t.spec.Resources, seg))
	}
	t.term = reason
	return seg
}

// handleNodeLost 原子处理节点失联：其上全部运行中任务脱离该节点，
// 按重试上限转为待重试（保留份额）或不可恢复终态，杜绝跨节点双开记账。
func (s *Scheduler) handleNodeLost(n *node, at time.Time) {
	keys := make([]string, 0, len(n.running))
	for k := range n.running {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	affected := map[string][]string{} // jobID -> task names
	for _, key := range keys {
		t := n.running[key]
		j, _ := s.findTaskLocked(key)
		if j == nil {
			continue
		}
		s.settleTask(j, t, at, TermNodeLost)
		delete(n.running, key)
		if t.retries < j.spec.MaxRetries {
			t.retries++
			t.state = TaskPending
			t.term = TermNodeLost
			s.release(key)                       // 节点占用回补
			s.holdPreempt(key, t.spec.Resources) // 待重试期间保留份额
			s.logLocked(Decision{At: at, Kind: DecisionRetry, JobID: j.spec.ID, TaskID: t.spec.Name,
				Node: n.spec.Name, Reason: string(TermNodeLost),
				Detail: sprintf("node lost; detached and queued for retry %d/%d; share held",
					t.retries, j.spec.MaxRetries)})
		} else {
			t.state = TaskFailed
			t.term = TermRetriesExhaust
			s.release(key)
			s.logLocked(Decision{At: at, Kind: DecisionRetryFail, JobID: j.spec.ID, TaskID: t.spec.Name,
				Node: n.spec.Name, Reason: string(TermRetriesExhaust),
				Detail: sprintf("node lost; retries exhausted (%d/%d), task unrecoverable; share returned",
					t.retries, j.spec.MaxRetries)})
		}
		affected[j.spec.ID] = append(affected[j.spec.ID], t.spec.Name)
		t.node = ""
		t.gpu = ""
		t.numaID = t.spec.NUMAID
	}
	// 节点账本清零：失联节点不再承载任何任务；恢复后从空账开始。
	n.usedCPU, n.usedMem, n.taskCount = 0, 0, 0
	n.busyGPUs = map[string]bool{}
	n.numaCPU = map[int]int64{}
	n.numaMem = map[int]int64{}
	n.numaGPU = map[int]map[string]bool{}
	for _, z := range n.spec.NUMA {
		n.numaCPU[z.ID] = 0
		n.numaMem[z.ID] = 0
		n.numaGPU[z.ID] = map[string]bool{}
	}
	jobIDs := make([]string, 0, len(affected))
	for id := range affected {
		jobIDs = append(jobIDs, id)
	}
	sort.Strings(jobIDs)
	for _, id := range jobIDs {
		s.logLocked(Decision{At: at, Kind: DecisionNodeLost, JobID: id, Node: n.spec.Name,
			Reason: string(TermNodeLost),
			Detail: sprintf("tasks detached from lost node: %s", strings.Join(affected[id], ","))})
		if j := s.jobs[id]; j != nil {
			s.recomputeJobState(j)
		}
	}
}

// recomputeJobState 依据任务终态/活态聚合作业状态。
func (s *Scheduler) recomputeJobState(j *job) {
	var nFin, nFail, nActive int
	for _, t := range j.tasks {
		switch t.state {
		case TaskFinished:
			nFin++
		case TaskFailed:
			nFail++
		default:
			nActive++
		}
	}
	switch {
	case nFin == len(j.tasks):
		j.state = JobFinished
	case nFail > 0:
		j.state = JobPartialFailure
	case nActive > 0:
		j.state = JobRunning
	default:
		j.state = JobPending
	}
}

// logLocked 追加一条决策；超过配置上限时丢弃最旧记录，保证内存有界。
func (s *Scheduler) logLocked(d Decision) {
	if s.cfg.MaxDecisionLog <= 0 {
		return
	}
	s.decisions = append(s.decisions, d)
	if len(s.decisions) > s.cfg.MaxDecisionLog {
		s.decisions = s.decisions[len(s.decisions)-s.cfg.MaxDecisionLog:]
	}
}

func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }

func summarizeJob(j *job) string {
	return sprintf("submit job id=%s user=%s queue=%s priority=%d tasks=%d total=%s",
		j.spec.ID, j.spec.User, j.spec.Queue, j.spec.Priority, len(j.spec.Tasks), resString(jobResourcesSum(j)))
}

func summarizeReport(r *ScheduleReport) string {
	return sprintf("cycle done: startedJobs=%d preemptedJobs=%d unschedulable=%d",
		len(r.StartedJobs), len(r.PreemptedJobs), len(r.Unschedulable))
}

func summarizeReasons(m map[string][]FilterReason) string {
	if len(m) == 0 {
		return "no node-level reasons"
	}
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, n := range names {
		rs := append([]FilterReason(nil), m[n]...)
		sort.Slice(rs, func(a, b int) bool { return rs[a] < rs[b] })
		parts = append(parts, sprintf("%s=%v", n, rs))
	}
	return strings.Join(parts, "; ")
}

func jobResourcesSum(j *job) (tot Resources) {
	for _, t := range j.tasks {
		tot = tot.add(t.spec.Resources)
	}
	return tot
}
