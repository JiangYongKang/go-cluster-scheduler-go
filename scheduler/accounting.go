package scheduler

import (
	"sort"
	"time"
)

// 记账不变量（全部在 mu 保护下成立）：
//  1. 每个运行中任务恰有一份运行份额（计入 userUsed/queueUsed）。
//  2. 每个被抢占/待重试任务恰有一份预留份额（preemptHold，按 key 去重），
//     重新启动时预留转运行，终止时释放；重复抢占不会叠加或丢失。
//  3. allocated = 运行份额 + 预留份额，任何时刻不超过提交时校验的配额。
//  4. 节点账本（usedCPU/usedMem/busyGPUs）与运行任务集合一一对应。

func (s *Scheduler) taskKey(jobID, taskName string) string {
	return jobID + "/" + taskName
}

// jobResources 返回作业全部任务的资源总和（提交配额校验用）。
func (s *Scheduler) jobResources(j *job) Resources { return jobResourcesSum(j) }

// pendingTasks 返回仍需参与成组调度的任务（排队或被抢占，未到终态）。
func pendingTasks(j *job) []*task {
	var out []*task
	for _, t := range j.tasks {
		if t.state == TaskPending || t.state == TaskPreempted {
			out = append(out, t)
		}
	}
	return out
}

// pendingResources 返回待启动任务的资源总和（数组维度整体参与核算）。
func (s *Scheduler) pendingResources(j *job) (tot Resources) {
	for _, t := range pendingTasks(j) {
		tot = tot.add(t.spec.Resources)
	}
	return tot
}

func (s *Scheduler) userAllocated(user string) (r Resources) {
	r = s.userUsed[user]
	for k, h := range s.preemptHold {
		if j, _ := s.findTaskLocked(k); j != nil && j.spec.User == user {
			r = r.add(h)
		}
	}
	return r
}

func (s *Scheduler) queueAllocated(queue string) (r Resources) {
	r = s.queueUsed[queue]
	for k, h := range s.preemptHold {
		if j, _ := s.findTaskLocked(k); j != nil && j.spec.Queue == queue {
			r = r.add(h)
		}
	}
	return r
}

// charge 计入一份新的运行份额（任务首次启动，无对应预留）。
func (s *Scheduler) charge(key string, r Resources) error {
	j, _ := s.findTaskLocked(key)
	if j == nil {
		return schedError("charge", ReasonJobNotFound, "task %s not found", key)
	}
	if uq, hasU := s.quota.UserQuota[j.spec.User]; hasU &&
		!r.add(s.userAllocated(j.spec.User)).fitsIn(uq) {
		return schedError("charge", ReasonQuotaExceeded,
			"user %s allocation would exceed quota: new %s + allocated %s > %s",
			j.spec.User, resString(r), resString(s.userAllocated(j.spec.User)), resString(uq))
	}
	if qq, hasQ := s.quota.QueueQuota[j.spec.Queue]; hasQ &&
		!r.add(s.queueAllocated(j.spec.Queue)).fitsIn(qq) {
		return schedError("charge", ReasonQuotaExceeded,
			"queue %s allocation would exceed quota: new %s + allocated %s > %s",
			j.spec.Queue, resString(r), resString(s.queueAllocated(j.spec.Queue)), resString(qq))
	}
	s.userUsed[j.spec.User] = s.userUsed[j.spec.User].add(r)
	s.queueUsed[j.spec.Queue] = s.queueUsed[j.spec.Queue].add(r)
	return nil
}

// release 回补一份运行份额（任务完成/取消/失联时调用）。
func (s *Scheduler) release(key string) {
	j, t := s.findTaskLocked(key)
	if j == nil || t == nil {
		return
	}
	r := t.spec.Resources
	s.userUsed[j.spec.User] = s.userUsed[j.spec.User].sub(r)
	s.queueUsed[j.spec.Queue] = s.queueUsed[j.spec.Queue].sub(r)
}

// holdPreempt 登记一份抢占/待重试预留份额。同一 key 幂等，重复抢占不重复计入。
func (s *Scheduler) holdPreempt(key string, r Resources) {
	if _, exists := s.preemptHold[key]; exists {
		return
	}
	s.preemptHold[key] = r
}

// unholdToCharge 将预留份额转为运行份额（被抢占任务重新启动）；allocated 总量不变。
func (s *Scheduler) unholdToCharge(key string) error {
	r, ok := s.preemptHold[key]
	if !ok {
		return schedError("accounting", ReasonTaskNotFound, "no held share for task %s", key)
	}
	delete(s.preemptHold, key)
	j, _ := s.findTaskLocked(key)
	s.userUsed[j.spec.User] = s.userUsed[j.spec.User].add(r)
	s.queueUsed[j.spec.Queue] = s.queueUsed[j.spec.Queue].add(r)
	return nil
}

// releaseHold 释放预留份额（被抢占作业被取消或重试耗尽时调用）。
func (s *Scheduler) releaseHold(key string) { delete(s.preemptHold, key) }

// checkStartQuota 校验整组待启动任务中新计入（非预留转换）的份额是否仍在配额内。
func (s *Scheduler) checkStartQuota(j *job) (RejectReason, string) {
	var extra Resources
	for _, t := range pendingTasks(j) {
		key := s.taskKey(j.spec.ID, t.spec.Name)
		if _, held := s.preemptHold[key]; !held {
			extra = extra.add(t.spec.Resources)
		}
	}
	if uq, hasU := s.quota.UserQuota[j.spec.User]; hasU &&
		!extra.add(s.userAllocated(j.spec.User)).fitsIn(uq) {
		return ReasonQuotaExceeded, sprintf("user %s quota exceeded: allocated %s + new %s > %s",
			j.spec.User, resString(s.userAllocated(j.spec.User)), resString(extra), resString(uq))
	}
	if qq, hasQ := s.quota.QueueQuota[j.spec.Queue]; hasQ &&
		!extra.add(s.queueAllocated(j.spec.Queue)).fitsIn(qq) {
		return ReasonQuotaExceeded, sprintf("queue %s quota exceeded: allocated %s + new %s > %s",
			j.spec.Queue, resString(s.queueAllocated(j.spec.Queue)), resString(extra), resString(qq))
	}
	return ReasonOK, ""
}

// checkSubmitLimits 在提交时原子地校验队列授权、待调度上限与配额。失败不写入状态。
func (s *Scheduler) checkSubmitLimits(spec JobSpec) (RejectReason, string) {
	if allowed, ok := s.quota.UserQueues[spec.User]; ok {
		authorized := false
		for _, q := range allowed {
			if q == spec.Queue {
				authorized = true
				break
			}
		}
		if !authorized {
			return ReasonQuotaExceeded, sprintf("user %s not allowed in queue %s", spec.User, spec.Queue)
		}
	}
	var total Resources
	for _, t := range spec.Tasks {
		total = total.add(t.Resources)
	}
	if uq, ok := s.quota.UserQuota[spec.User]; ok {
		if cur := s.userAllocated(spec.User); !total.add(cur).fitsIn(uq) {
			return ReasonQuotaExceeded, sprintf("user %s quota exceeded: allocated %s + submit %s > %s",
				spec.User, resString(cur), resString(total), resString(uq))
		}
	}
	if qq, ok := s.quota.QueueQuota[spec.Queue]; ok {
		if cur := s.queueAllocated(spec.Queue); !total.add(cur).fitsIn(qq) {
			return ReasonQuotaExceeded, sprintf("queue %s quota exceeded: allocated %s + submit %s > %s",
				spec.Queue, resString(cur), resString(total), resString(qq))
		}
	}
	var pendingTotal int
	queuePending := map[string]int{}
	for _, id := range s.order {
		old := s.jobs[id]
		if old.state == JobPending {
			pendingTotal++
			queuePending[old.spec.Queue]++
		}
	}
	if s.cfg.MaxPendingJobs > 0 && pendingTotal+1 > s.cfg.MaxPendingJobs {
		return ReasonPendingLimit, sprintf("global pending job limit %d reached", s.cfg.MaxPendingJobs)
	}
	if cap, ok := s.cfg.QueueMaxPending[spec.Queue]; ok && cap > 0 && queuePending[spec.Queue]+1 > cap {
		return ReasonQueueFull, sprintf("queue %s pending limit %d reached", spec.Queue, cap)
	}
	if s.quota.MaxJobsTotal > 0 && len(s.order)+1 > s.quota.MaxJobsTotal {
		return ReasonPendingLimit, sprintf("global active job limit %d reached", s.quota.MaxJobsTotal)
	}
	return ReasonOK, ""
}

func (s *Scheduler) snapshotLocked(at time.Time) *UsageSnapshot {
	snap := &UsageSnapshot{
		At:       at,
		Users:    map[string]Resources{},
		Queues:   map[string]Resources{},
		UserJobs: map[string]int{},
	}
	users, queues := map[string]bool{}, map[string]bool{}
	for u := range s.userUsed {
		users[u] = true
	}
	for q := range s.queueUsed {
		queues[q] = true
	}
	for k := range s.preemptHold {
		if j, _ := s.findTaskLocked(k); j != nil {
			users[j.spec.User] = true
			queues[j.spec.Queue] = true
		}
	}
	us := sortedKeys2(users)
	for _, u := range us {
		snap.Users[u] = s.userAllocated(u)
	}
	for _, q := range sortedKeys2(queues) {
		snap.Queues[q] = s.queueAllocated(q)
	}
	for _, nd := range s.nodes {
		for _, t := range nd.running {
			snap.Running = snap.Running.add(t.spec.Resources)
		}
	}
	var held Resources
	for _, h := range s.preemptHold {
		held = held.add(h)
	}
	snap.Allocated = snap.Running.add(held)
	for _, id := range s.order {
		j := s.jobs[id]
		if j.state != JobFinished && j.state != JobCanceled {
			snap.UserJobs[j.spec.User]++
		}
	}
	return snap
}

func sortedKeys2(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func resString(r Resources) string {
	return sprintf("(cpu=%d,mem=%d,gpu=%d)", r.CPU, r.Memory, r.GPU)
}
