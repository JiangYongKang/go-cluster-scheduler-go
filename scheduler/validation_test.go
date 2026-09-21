package scheduler

import (
	"testing"
)

// 非法节点/作业在入口被归类为 INVALID_JOB，且不写入状态。
func TestInvalidSpecsRejected(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	if err := s.AddNode(NodeSpec{Name: "", CPU: 1}); !IsReason(err, ReasonInvalidJob) {
		t.Fatalf("empty node name want INVALID_JOB, got %v", err)
	}
	if err := s.AddNode(NodeSpec{Name: "n", CPU: 4, Memory: 4,
		NUMA: []NUMAZone{{ID: 0, CPUs: 2, Mem: 4}},
	}); !IsReason(err, ReasonInvalidJob) {
		t.Fatalf("NUMA totals mismatch want INVALID_JOB, got %v", err)
	}
	if err := s.AddNode(NodeSpec{Name: "n", CPU: 4, Memory: 4,
		GPUs: []GPUDevice{{ID: "g0"}},
		NUMA: []NUMAZone{{ID: 0, CPUs: 4, Mem: 4}},
	}); !IsReason(err, ReasonInvalidJob) {
		t.Fatalf("GPU not in any zone want INVALID_JOB, got %v", err)
	}

	bad := []JobSpec{
		{ID: "", User: "u", Queue: "q", Tasks: []TaskSpec{mkTask("a", 1, 1)}},
		{ID: "j", User: "u", Queue: "q"}, // 无任务
		{ID: "j", User: "u", Queue: "q", Tasks: []TaskSpec{
			{Name: "a", NUMAID: -1, Resources: Resources{CPU: -1}}}},
		{ID: "j", User: "u", Queue: "q", Tasks: []TaskSpec{
			{Name: "a", NUMAID: -1, Resources: Resources{GPU: 2}}}},
		{ID: "j", User: "u", Queue: "q", Tasks: []TaskSpec{
			{Name: "a", NUMAID: -1, GPUDevice: "g", Resources: Resources{CPU: 1}}}},
	}
	for i, b := range bad {
		if err := s.Submit(b); !IsReason(err, ReasonInvalidJob) {
			t.Fatalf("case %d want INVALID_JOB, got %v", i, err)
		}
	}
	if len(s.jobs) != 0 {
		t.Fatalf("invalid submits must leave no state, got %d jobs", len(s.jobs))
	}
	t.Logf("JUDGE all invalid specs rejected as INVALID_JOB, no state left")
}

func TestMissingEntities(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	if err := s.SetNodeState("ghost", NodeReady); !IsReason(err, ReasonNodeNotFound) {
		t.Fatalf("want NODE_NOT_FOUND, got %v", err)
	}
	if _, ok, err := s.NodeStateView("ghost"); ok || !IsReason(err, ReasonNodeNotFound) {
		t.Fatalf("want ok=false + NODE_NOT_FOUND")
	}
	if err := s.CancelJob("ghost"); !IsReason(err, ReasonJobNotFound) {
		t.Fatalf("want JOB_NOT_FOUND, got %v", err)
	}
	t.Logf("JUDGE missing node/job errors classified distinctly")
}

// 未知或重复的完成键不产生重复记账；全部完成后作业 Finished。
func TestTickUnknownAndCompletion(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "j", User: "u", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("a", 2, 1), mkTask("b", 2, 1)}})
	mustSchedule(t, s)
	s.Tick(t0, []string{"j/a", "j/nonexistent", "other/x"})
	if st := s.GetJob("j").Tasks[0].State; st != TaskFinished {
		t.Fatalf("a want finished, got %s", st)
	}
	// 再次完成同一任务：幂等忽略，份额只回补一次。
	s.Tick(t0, []string{"j/a"})
	if u := s.Snapshot().Users["u"]; u.CPU != 2 {
		t.Fatalf("double complete must not double-release, user cpu=%d want 2", u.CPU)
	}
	s.Tick(t0, []string{"j/b"})
	if s.GetJob("j").State != JobFinished {
		t.Fatalf("job want Finished, got %s", s.GetJob("j").State)
	}
	if snap := s.Snapshot(); snap.Running.CPU != 0 || snap.Allocated.CPU != 0 {
		t.Fatalf("all shares must be back, run=%d alloc=%d", snap.Running.CPU, snap.Allocated.CPU)
	}
	t.Logf("JUDGE tick idempotent; completion conserved; job Finished")
}

// Drained 节点不可调度新任务，但其上已运行任务不受影响。
func TestDrainedNode(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "j1", User: "u", Queue: "q", Tasks: []TaskSpec{mkTask("a", 2, 1)}})
	mustSchedule(t, s)
	if err := s.SetNodeState("n1", NodeDrained); err != nil {
		t.Fatal(err)
	}
	if s.GetJob("j1").Tasks[0].State != TaskRunning {
		t.Fatalf("draining must not kill running tasks")
	}
	mustSubmit(t, s, JobSpec{ID: "j2", User: "u", Queue: "q", Tasks: []TaskSpec{mkTask("b", 1, 1)}})
	rpt := mustSchedule(t, s)
	if !containsReason(rpt.Unschedulable[0].NodeReasons["n1"], FilterNodeNotReady) {
		t.Fatalf("want NODE_NOT_READY on drained node, got %v", rpt.Unschedulable[0].NodeReasons)
	}
	t.Logf("JUDGE drained node keeps running tasks but rejects new ones: %v", rpt.Unschedulable[0].NodeReasons)
}
