package scheduler

import "time"

// ResourceTime 是“资源 × 时间”的累计量（资源时间）。
// 各维度独立累计、可相加；以纳秒为时间单位，保证跨抢占/重试的分段求和精确。
type ResourceTime struct {
	CPUNanos    int64
	MemoryNanos int64
	GPUNanos    int64
}

// ResourceTimeBuckets 按任务终局原因分桶的资源时间。五类之和恒等于总量：
// Running + Finished + Canceled + Failed + Preempted = Total。
type ResourceTimeBuckets struct {
	Running   ResourceTime // 当前仍在运行的时段（自最近一次启动起，按查询时刻结算）
	Finished  ResourceTime // 正常结束的已结时段
	Canceled  ResourceTime // 被取消的已结时段
	Failed    ResourceTime // 失败的已结时段（节点失联重试用尽等）
	Preempted ResourceTime // 被抢占的已结时段（其后可能重启，再按当时原因另计）
	Total     ResourceTime
}

// ResourceTimeView 是某一维度（用户/队列/作业）的累计资源时间只读视图。
type ResourceTimeView struct {
	Key     string
	User    string
	Queue   string
	JobID   string
	At      time.Time
	Buckets ResourceTimeBuckets
}

func (a ResourceTime) add(b ResourceTime) ResourceTime {
	return ResourceTime{
		CPUNanos:    a.CPUNanos + b.CPUNanos,
		MemoryNanos: a.MemoryNanos + b.MemoryNanos,
		GPUNanos:    a.GPUNanos + b.GPUNanos,
	}
}

// resourceTimeOf 计算一份资源在 d 时长上的资源时间。
func resourceTimeOf(r Resources, d time.Duration) ResourceTime {
	n := int64(d)
	return ResourceTime{CPUNanos: r.CPU * n, MemoryNanos: r.Memory * n, GPUNanos: r.GPU * n}
}

// addSettled 把一段已结运行时段按终局原因计入唯一分桶并重算总量。
func (b *ResourceTimeBuckets) addSettled(reason TerminalReason, rt ResourceTime) {
	switch reason {
	case TermFinished:
		b.Finished = b.Finished.add(rt)
	case TermCanceled:
		b.Canceled = b.Canceled.add(rt)
	case TermPreempted:
		b.Preempted = b.Preempted.add(rt)
	default:
		// TermNodeLost / TermRetriesExhaust 及其他失败均归失败桶。
		b.Failed = b.Failed.add(rt)
	}
	b.Total = b.Total.add(rt)
}

// addInto 把另一组分桶（含 Running 即时段）逐桶累加到 b。
func (b *ResourceTimeBuckets) addInto(o ResourceTimeBuckets) {
	b.Running = b.Running.add(o.Running)
	b.Finished = b.Finished.add(o.Finished)
	b.Canceled = b.Canceled.add(o.Canceled)
	b.Failed = b.Failed.add(o.Failed)
	b.Preempted = b.Preempted.add(o.Preempted)
	b.Total = b.Total.add(o.Total)
}

// taskBucketsLocked 返回任务在 at 时刻的资源时间分桶：已结部分直接取账，
// 当前运行时段（startedAt..at）即时计入 Running 桶（尚未入账，故不会重复）。
func taskBucketsLocked(t *task, at time.Time) ResourceTimeBuckets {
	b := t.rt // 复制
	if t.state == TaskRunning && !t.startedAt.IsZero() {
		end := at
		if end.Before(t.startedAt) {
			end = t.startedAt
		}
		if d := end.Sub(t.startedAt); d > 0 {
			live := resourceTimeOf(t.spec.Resources, d)
			b.Running = b.Running.add(live)
			b.Total = b.Total.add(live)
		}
	}
	return b
}

// resourceTimesLocked 聚合全部任务到用户/队列/作业三维报表。
// 每个任务恰归属其作业、用户、队列各一次，因此三维各自的五桶之和均守恒。
func (s *Scheduler) resourceTimesLocked() *ResourceTimeReport {
	rpt := &ResourceTimeReport{
		At:     s.now,
		Users:  map[string]*ResourceTimeView{},
		Queues: map[string]*ResourceTimeView{},
		Jobs:   map[string]*ResourceTimeView{},
	}
	jobView := func(j *job) *ResourceTimeView {
		v := rpt.Jobs[j.spec.ID]
		if v == nil {
			v = &ResourceTimeView{Key: j.spec.ID, User: j.spec.User,
				Queue: j.spec.Queue, JobID: j.spec.ID, At: s.now}
			rpt.Jobs[j.spec.ID] = v
		}
		return v
	}
	userView := func(j *job) *ResourceTimeView {
		v := rpt.Users[j.spec.User]
		if v == nil {
			v = &ResourceTimeView{Key: j.spec.User, User: j.spec.User, At: s.now}
			rpt.Users[j.spec.User] = v
		}
		return v
	}
	queueView := func(j *job) *ResourceTimeView {
		v := rpt.Queues[j.spec.Queue]
		if v == nil {
			v = &ResourceTimeView{Key: j.spec.Queue, Queue: j.spec.Queue, At: s.now}
			rpt.Queues[j.spec.Queue] = v
		}
		return v
	}
	for _, id := range s.order {
		j := s.jobs[id]
		jv := jobView(j)
		uv := userView(j)
		qv := queueView(j)
		for _, t := range j.tasks {
			b := taskBucketsLocked(t, s.now)
			jv.Buckets.addInto(b)
			uv.Buckets.addInto(b)
			qv.Buckets.addInto(b)
		}
	}
	return rpt
}

// ResourceTimeReport 是全集群用户/队列/作业三维累计资源时间报表。
type ResourceTimeReport struct {
	At     time.Time
	Users  map[string]*ResourceTimeView
	Queues map[string]*ResourceTimeView
	Jobs   map[string]*ResourceTimeView
}

// ResourceTimes 返回用户/队列/作业三维的累计资源时间报表（加锁快照，
// 可与调度/抢占/取消/失败并发安全地读取）。每个维度均满足
// Running + Finished + Canceled + Failed + Preempted = Total。
func (s *Scheduler) ResourceTimes() *ResourceTimeReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resourceTimesLocked()
}
