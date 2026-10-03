package scheduler

import (
	"testing"
	"time"
)

// 高优先级作业抢占低优先级作业；被抢占任务保留份额并可重新调度；
// 已耗运行时被结算；守恒。
func TestPreemptionSettlementAndRestart(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 8, Memory: 8})

	low := JobSpec{ID: "low", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("a", 6, 1), mkTask("b", 2, 1)}}
	mustSubmit(t, s, low)
	mustSchedule(t, s)
	if snap := s.Snapshot(); snap.Running.CPU != 8 || snap.Allocated.CPU != 8 {
		t.Fatalf("initial want running=allocated=8, got %d/%d", snap.Running.CPU, snap.Allocated.CPU)
	}

	// 高优先级作业需要 4 cpu，集群无空闲：必须抢占。
	high := JobSpec{ID: "high", User: "u2", Queue: "q1", Priority: 100,
		Tasks: []TaskSpec{mkTask("x", 4, 1)}}
	mustSubmit(t, s, high)
	s.Tick(t0.Add(time.Minute), nil) // 时钟推进到 1 分钟
	rpt := mustSchedule(t, s)
	if len(rpt.PreemptedJobs) == 0 {
		t.Fatalf("want preemption, got none")
	}
	rec := rpt.PreemptedJobs[0]
	if rec.PreemptedJobID != "low" || rec.PreemptingJobID != "high" {
		t.Fatalf("unexpected preemption record: %+v", rec)
	}
	if rec.SettledRuntime != time.Duration(len(rec.PreemptedTasks))*time.Minute {
		t.Fatalf("settled runtime %s want %d x 1m", rec.SettledRuntime, len(rec.PreemptedTasks))
	}
	t.Logf("JUDGE preempted %s by %s tasks=%v runtime=%s returned=%s",
		rec.PreemptedJobID, rec.PreemptingJobID, rec.PreemptedTasks, rec.SettledRuntime, resString(rec.ReturnedShare))

	hi := s.GetJob("high")
	if hi.Tasks[0].State != TaskRunning || hi.Tasks[0].Node != "n1" {
		t.Fatalf("high must be running on n1, got %+v", hi.Tasks[0])
	}

	// 每个被抢占任务结算的运行时必须恰为 1 分钟。
	for _, tk := range s.GetJob("low").Tasks {
		if tk.State == TaskPreempted && tk.Runtime != time.Minute {
			t.Fatalf("preempted task %s runtime = %s, want 1m", tk.Name, tk.Runtime)
		}
	}

	// 守恒：运行 cpu = high 4 + low 仍运行任务；allocated 始终为 12（high 4 + low 8）。
	infoLow := s.GetJob("low")
	var lowRun int64
	for _, tk := range infoLow.Tasks {
		if tk.State == TaskRunning {
			for _, ts := range low.Tasks {
				if ts.Name == tk.Name {
					lowRun += ts.Resources.CPU
				}
			}
		}
	}
	snap := s.Snapshot()
	if snap.Running.CPU != 4+lowRun {
		t.Fatalf("running cpu = %d, want %d", snap.Running.CPU, 4+lowRun)
	}
	if snap.Allocated.CPU != 12 {
		t.Fatalf("allocated cpu = %d, want 12", snap.Allocated.CPU)
	}
	t.Logf("JUDGE conservation after preempt: running=%d allocated=%d", snap.Running.CPU, snap.Allocated.CPU)

	// high 完成后，low 被抢占任务应在后续周期重新调度，预留转运行。
	s.Tick(t0.Add(2*time.Minute), []string{"high/x"})
	rpt2 := mustSchedule(t, s)
	if len(rpt2.StartedJobs) == 0 {
		t.Fatalf("low preempted tasks should restart after high completes")
	}
	for _, tk := range s.GetJob("low").Tasks {
		if tk.State != TaskRunning && tk.State != TaskFinished {
			t.Fatalf("low task %s state %s after restart cycle", tk.Name, tk.State)
		}
	}
	snap3 := s.Snapshot()
	if snap3.Allocated.CPU != 8 || snap3.Running.CPU != 8 {
		t.Fatalf("after restart want running=allocated=8, got run=%d alloc=%d",
			snap3.Running.CPU, snap3.Allocated.CPU)
	}
	t.Logf("JUDGE low restarted; hold converted to running; run=%d alloc=%d", snap3.Running.CPU, snap3.Allocated.CPU)
}

// 抢占不足：即使抢占全部更低优先级任务仍放不下时，以 PREEMPTION_NOT_ENOUGH 拒绝。
func TestPreemptionNotEnough(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "low", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("a", 2, 1)}})
	mustSchedule(t, s)
	mustSubmit(t, s, JobSpec{ID: "big", User: "u2", Queue: "q1", Priority: 100,
		Tasks: []TaskSpec{mkTask("x", 8, 1)}})
	rpt := mustSchedule(t, s)
	if len(rpt.StartedJobs) != 0 {
		t.Fatalf("big must not fit even after preemption")
	}
	if rpt.Unschedulable[0].Reason != ReasonPreemptionNotEnough {
		t.Fatalf("want PREEMPTION_NOT_ENOUGH, got %s", rpt.Unschedulable[0].Reason)
	}
	if st := s.GetJob("low").Tasks[0].State; st != TaskRunning {
		t.Fatalf("low must keep running after failed preemption attempt, got %s", st)
	}
	t.Logf("JUDGE preemption infeasible leaves low untouched; reason=%s", rpt.Unschedulable[0].Reason)
}

// 同优先级且没有任何“已消耗份额更大”的可让出受害者时：维持原状，
// 以可区分的 NO_PREEMPTABLE_VICTIM 拒绝（消耗更少/相等的一方不被抢）。
func TestNoPreemptionAtSamePriority(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "a", User: "u1", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	mustSchedule(t, s)
	// 不推进时钟：a 的已消耗份额为 0，不大于 b 的 0，构不成可让出受害者。
	mustSubmit(t, s, JobSpec{ID: "b", User: "u2", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	rpt := mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 0 || len(rpt.StartedJobs) != 0 {
		t.Fatalf("same priority without bigger-consumed victim must not preempt, got %+v", rpt)
	}
	if rpt.Unschedulable[0].Reason != ReasonNoPreemptableVictim {
		t.Fatalf("want NO_PREEMPTABLE_VICTIM, got %s", rpt.Unschedulable[0].Reason)
	}
	if st := s.GetJob("a").Tasks[0].State; st != TaskRunning {
		t.Fatalf("a must keep running (status quo), got %s", st)
	}
	t.Logf("JUDGE no yieldable same-priority victim: %s; a untouched", rpt.Unschedulable[0].Reason)
}

// 多次抢占不重复计入：任务被抢占两次，runtime 为两段之和，份额始终只有一份预留。
func TestRepeatedPreemptionNoDoubleCounting(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "vic", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("v", 4, 1)}})
	mustSchedule(t, s)

	// 第一次抢占：vic 运行 1 分钟后被 h1 抢占。
	mustSubmit(t, s, JobSpec{ID: "h1", User: "u2", Queue: "q1", Priority: 100,
		Tasks: []TaskSpec{mkTask("h", 4, 1)}})
	s.Tick(t0.Add(time.Minute), nil)
	rpt := mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 1 {
		t.Fatalf("want exactly one preemption, got %d", len(rpt.PreemptedJobs))
	}
	if rt := s.GetJob("vic").Tasks[0].Runtime; rt != time.Minute {
		t.Fatalf("after 1st preemption runtime=%s, want 1m", rt)
	}
	// h1 自然结束；vic 重新调度运行（2m 时刻启动）。
	s.Tick(t0.Add(2*time.Minute), []string{"h1/h"})
	mustSchedule(t, s)
	if st := s.GetJob("vic").Tasks[0].State; st != TaskRunning {
		t.Fatalf("vic should run again, got %s", st)
	}
	// 第二次抢占：vic 在 2m..3m 再运行 1 分钟后被 h2 抢占。
	mustSubmit(t, s, JobSpec{ID: "h2", User: "u2", Queue: "q1", Priority: 100,
		Tasks: []TaskSpec{mkTask("h", 4, 1)}})
	s.Tick(t0.Add(3*time.Minute), nil)
	rpt = mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 1 {
		t.Fatalf("want exactly one second preemption, got %d", len(rpt.PreemptedJobs))
	}
	if rt := s.GetJob("vic").Tasks[0].Runtime; rt != 2*time.Minute {
		t.Fatalf("after 2nd preemption runtime=%s, want 2m (two segments summed)", rt)
	}
	snap := s.Snapshot()
	if snap.Running.CPU != 4 || snap.Allocated.CPU != 8 {
		t.Fatalf("want running=4 allocated=8, got run=%d alloc=%d", snap.Running.CPU, snap.Allocated.CPU)
	}
	t.Logf("JUDGE repeated preemption: vic runtime=2m (two 1m segments); single held share; run=%d alloc=%d",
		snap.Running.CPU, snap.Allocated.CPU)
}
