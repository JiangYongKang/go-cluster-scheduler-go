package scheduler

import (
	"testing"
	"time"
)

// 节点失联：任务被标记、记账脱离；有重试额度时保留份额待重新调度。
func TestNodeLostRetryAndNoDoubleBooking(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustAddNode(t, s, NodeSpec{Name: "n2", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1, MaxRetries: 1,
		Tasks: []TaskSpec{mkTask("a", 4, 1)}})
	mustSchedule(t, s)
	if node := s.GetJob("j").Tasks[0].Node; node != "n1" {
		t.Fatalf("want n1, got %s", node)
	}

	// 失联：任务必须脱离 n1，节点账本清零，份额保留待重试。
	if err := s.SetNodeState("n1", NodeLost); err != nil {
		t.Fatal(err)
	}
	info := s.GetJob("j")
	if info.Tasks[0].State != TaskPending || info.Tasks[0].Node != "" || info.Tasks[0].Retries != 1 {
		t.Fatalf("want detached pending retry, got %+v", info.Tasks[0])
	}
	if s.nodes["n1"].usedCPU != 0 || len(s.nodes["n1"].running) != 0 {
		t.Fatalf("lost node ledger must be zero, got used=%d running=%d",
			s.nodes["n1"].usedCPU, len(s.nodes["n1"].running))
	}
	snap := s.Snapshot()
	if snap.Running.CPU != 0 || snap.Allocated.CPU != 4 {
		t.Fatalf("want running=0 allocated=4 (held), got run=%d alloc=%d",
			snap.Running.CPU, snap.Allocated.CPU)
	}
	t.Logf("JUDGE n1 lost: task detached, node ledger zeroed, share held; run=%d alloc=%d",
		snap.Running.CPU, snap.Allocated.CPU)

	// n1 仍失联时调度：任务只能去 n2，绝不可能同时出现在 n1。
	rpt := mustSchedule(t, s)
	if len(rpt.StartedJobs) != 1 {
		t.Fatalf("want reschedule on n2, got %+v", rpt.Unschedulable)
	}
	if node := s.GetJob("j").Tasks[0].Node; node != "n2" {
		t.Fatalf("want restart on n2, got %s", node)
	}
	if len(s.nodes["n1"].running) != 0 {
		t.Fatalf("lost node must never host tasks")
	}
	t.Logf("JUDGE task rescheduled exclusively on n2; no double-node booking")
}

// 重试耗尽：再次失联且无额度时任务进入可区分终态 RETRIES_EXHAUSTED，份额释放。
func TestNodeLostRetriesExhausted(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustAddNode(t, s, NodeSpec{Name: "n2", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1, MaxRetries: 1,
		Tasks: []TaskSpec{mkTask("a", 4, 1)}})
	mustSchedule(t, s)
	if err := s.SetNodeState("n1", NodeLost); err != nil {
		t.Fatal(err)
	}
	mustSchedule(t, s) // 在 n2 重启（retries=1）
	if node := s.GetJob("j").Tasks[0].Node; node != "n2" {
		t.Fatalf("want n2, got %s", node)
	}
	if err := s.SetNodeState("n2", NodeLost); err != nil {
		t.Fatal(err)
	}
	tk := s.GetJob("j").Tasks[0]
	if tk.State != TaskFailed || tk.TermReason != TermRetriesExhaust {
		t.Fatalf("want failed/RETRIES_EXHAUSTED, got %s/%s", tk.State, tk.TermReason)
	}
	if s.GetJob("j").State != JobPartialFailure {
		t.Fatalf("job want PartialFailure, got %s", s.GetJob("j").State)
	}
	snap := s.Snapshot()
	if snap.Running.CPU != 0 || snap.Allocated.CPU != 0 {
		t.Fatalf("want all shares released, got run=%d alloc=%d", snap.Running.CPU, snap.Allocated.CPU)
	}
	t.Logf("JUDGE retries exhausted -> task Failed/%s, job PartialFailure, shares released", TermRetriesExhaust)
}

// 失联节点恢复为 Ready 后可再次承载任务；已在别处运行的任务不会回迁。
func TestNodeRecoveredAcceptsNewTasks(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustAddNode(t, s, NodeSpec{Name: "n2", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1, MaxRetries: 1,
		Tasks: []TaskSpec{mkTask("a", 4, 1)}})
	mustSchedule(t, s)
	if err := s.SetNodeState("n1", NodeLost); err != nil {
		t.Fatal(err)
	}
	mustSchedule(t, s) // 在 n2 运行
	if err := s.SetNodeState("n1", NodeReady); err != nil {
		t.Fatal(err)
	}
	st, ok, _ := s.NodeStateView("n1")
	if !ok || st != NodeReady {
		t.Fatalf("n1 want ready, got %v/%v", st, ok)
	}
	if node := s.GetJob("j").Tasks[0].Node; node != "n2" {
		t.Fatalf("running task must not migrate back, got %s", node)
	}
	// 新作业可落在恢复的 n1。
	mustSubmit(t, s, JobSpec{ID: "j2", User: "u2", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("b", 4, 1)}})
	mustSchedule(t, s)
	if node := s.GetJob("j2").Tasks[0].Node; node != "n1" {
		t.Fatalf("new task should use recovered n1, got %s", node)
	}
	t.Logf("JUDGE n1 recovered; running task stays on n2; new task accepted on n1")
}

// 失联时结算运行时。
func TestNodeLostSettlesRuntime(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1, MaxRetries: 1,
		Tasks: []TaskSpec{mkTask("a", 4, 1)}})
	mustSchedule(t, s)
	s.Tick(t0.Add(90*time.Second), nil)
	if err := s.SetNodeState("n1", NodeLost); err != nil {
		t.Fatal(err)
	}
	if rt := s.GetJob("j").Tasks[0].Runtime; rt != 90*time.Second {
		t.Fatalf("want settled 90s, got %s", rt)
	}
	t.Logf("JUDGE node loss settled runtime=%s", s.GetJob("j").Tasks[0].Runtime)
}
