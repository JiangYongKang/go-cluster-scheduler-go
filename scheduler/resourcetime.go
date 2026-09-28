package scheduler

import "time"

// 资源时间（resource-time）核算：
//
// 每个任务的每一段运行时间在该段结束（完成/取消/失败/被抢占）时，
// 按离场原因落入且仅落入一个已结算桶；查询时刻仍在运行的段实时计入
// Running 桶。因此五类资源时间之和恒等于该任务全部历史运行段的总量，
// 跨调度、抢占、取消、失败重试既不重复也不遗漏：
//
//	Total = Running + Finished + Canceled + Failed + Preempted
//
// 资源时间 = 资源量 × 段长（CPU-seconds、Memory-seconds、GPU-seconds）。
// 所有查询与状态变更共用同一把互斥锁，读到的是一致快照。

// failedBills 列出计入 Failed 桶的终态原因：
// 失联摘出待重试（该段运行已终止于失败事件）与重试用尽。
func failedBill(term TerminalReason) bool {
	return term == TermNodeLost || term == TermRetriesExhaust
}

// accrueSettled 把一段已结束的运行时间按 t.term 落入对应桶。
// 仅由 settleTask 调用，保证所有结算路径统一入账。
func accrueSettled(t *task, seg time.Duration) {
	if seg <= 0 {
		return
	}
	switch {
	case t.term == TermFinished:
		t.rtFinished = t.rtFinished.addSegment(t.spec.Resources, seg)
	case t.term == TermCanceled:
		t.rtCanceled = t.rtCanceled.addSegment(t.spec.Resources, seg)
	case t.term == TermPreempted:
		t.rtPreempted = t.rtPreempted.addSegment(t.spec.Resources, seg)
	case failedBill(t.term):
		t.rtFailed = t.rtFailed.addSegment(t.spec.Resources, seg)
	}
}

// taskBreakdownLocked 返回任务在 at 时刻的五类资源时间（含未结算运行段）。
func taskBreakdownLocked(t *task, at time.Time) ResourceTimeBreakdown {
	b := ResourceTimeBreakdown{
		Finished:  t.rtFinished,
		Canceled:  t.rtCanceled,
		Failed:    t.rtFailed,
		Preempted: t.rtPreempted,
	}
	if t.state == TaskRunning && !t.startedAt.IsZero() {
		seg := at.Sub(t.startedAt)
		if seg < 0 {
			seg = 0
		}
		b.Running = b.Running.addSegment(t.spec.Resources, seg)
	}
	b.Total = b.Running.add(b.Finished).add(b.Canceled).add(b.Failed).add(b.Preempted)
	return b
}

// ResourceTimes 返回当前时钟下按用户/队列/作业维度的资源时间一致快照。
// 与 Submit/Schedule/CancelJob/Tick/SetNodeState 串行化，可安全并发调用。
func (s *Scheduler) ResourceTimes() *ResourceTimeView {
	s.mu.Lock()
	defer s.mu.Unlock()
	view := &ResourceTimeView{
		At:     s.now,
		Users:  map[string]ResourceTimeBreakdown{},
		Queues: map[string]ResourceTimeBreakdown{},
		Jobs:   map[string]JobResourceTime{},
	}
	for _, id := range s.order {
		j := s.jobs[id]
		var jb ResourceTimeBreakdown
		for _, t := range j.tasks {
			jb = addBreakdown(jb, taskBreakdownLocked(t, s.now))
		}
		view.Jobs[j.spec.ID] = JobResourceTime{
			ID: j.spec.ID, User: j.spec.User, Queue: j.spec.Queue, Breakdown: jb,
		}
		view.Users[j.spec.User] = addBreakdown(view.Users[j.spec.User], jb)
		view.Queues[j.spec.Queue] = addBreakdown(view.Queues[j.spec.Queue], jb)
		view.Total = addBreakdown(view.Total, jb)
	}
	return view
}

func addBreakdown(a, b ResourceTimeBreakdown) ResourceTimeBreakdown {
	out := ResourceTimeBreakdown{
		Running:   a.Running.add(b.Running),
		Finished:  a.Finished.add(b.Finished),
		Canceled:  a.Canceled.add(b.Canceled),
		Failed:    a.Failed.add(b.Failed),
		Preempted: a.Preempted.add(b.Preempted),
	}
	out.Total = out.Running.add(out.Finished).add(out.Canceled).add(out.Failed).add(out.Preempted)
	return out
}
