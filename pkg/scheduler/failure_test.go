package scheduler

import (
	"bytes"
	"testing"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
)

func failNodes() []*api.Node {
	return []*api.Node{
		{Name: "n1", Capacity: api.Resources{MilliCPU: 4000, MemoryMB: 8192}, State: api.NodeReady,
			NumaNodes: []api.NUMANode{{ID: 0, MilliCPU: 4000, MemoryMB: 8192}}},
		{Name: "n2", Capacity: api.Resources{MilliCPU: 4000, MemoryMB: 8192}, State: api.NodeReady,
			NumaNodes: []api.NUMANode{{ID: 0, MilliCPU: 4000, MemoryMB: 8192}}},
	}
}

func TestSoftNodeFailureReschedulesOnce(t *testing.T) {
	var trace bytes.Buffer
	s := New(failNodes(), DefaultConfig())
	s.SetTrace(&trace)
	_ = s.Submit(api.JobSpec{ID: "j", User: "u", Queue: "q", Priority: 1,
		ArraySize: 2, TaskRes: api.Resources{MilliCPU: 2000, MemoryMB: 256}, Runtime: 6})
	s.Tick() // place both
	j, _ := s.GetJob("j")
	if j.RunningTaskCount() != 2 {
		t.Fatalf("want 2 running got %d", j.RunningTaskCount())
	}
	if j.Tasks[0].NodeName == j.Tasks[1].NodeName {
		t.Fatalf("tasks should be on different nodes: %s %s", j.Tasks[0].NodeName, j.Tasks[1].NodeName)
	}
	s.Tick() // accrue tick 2

	// n1 goes unreachable between ticks.
	if err := s.SetNodeState("n1", api.NodeNotReady); err != nil {
		t.Fatal(err)
	}
	j, _ = s.GetJob("j")
	var evicted *api.Task
	for _, task := range j.Tasks {
		if task.State == api.TaskPreempted {
			evicted = task
		}
		if task.State == api.TaskRunning && task.NodeName == "n1" {
			t.Fatalf("task still bookkept running on lost node n1: %+v", task)
		}
	}
	if evicted == nil {
		t.Fatal("expected one preempted/requeued task")
	}
	// The task must no longer have a topology placement (no double-running).
	if _, ok := s.view.TaskPlacement(evicted.ID); ok {
		t.Fatal("evicted task still has a placement")
	}
	// n1 resources must be free while n2's task keeps running.
	if free := s.view.NodeFree("n1").MilliCPU; free != 4000 {
		t.Fatalf("n1 free cpu=%d want 4000", free)
	}

	// n1 comes back; next tick re-queued task restarts on n1.
	if err := s.SetNodeState("n1", api.NodeReady); err != nil {
		t.Fatal(err)
	}
	s.Tick()
	j, _ = s.GetJob("j")
	var onTwo int
	for _, task := range j.Tasks {
		if task.State != api.TaskRunning {
			t.Fatalf("task %s state=%s, want Running", task.ID, task.State)
		}
		onTwo++
		// Exactly one placement record per task.
		if _, ok := s.view.TaskPlacement(task.ID); !ok {
			t.Fatalf("task %s missing placement", task.ID)
		}
	}
	if onTwo != 2 {
		t.Fatalf("want 2 running, got %d", onTwo)
	}
	// Run to completion (requeued task keeps accrued ticks: 1 already done).
	for i := 0; i < 8; i++ {
		s.Tick()
	}
	j, _ = s.GetJob("j")
	if j.State != api.JobSucceeded {
		t.Fatalf("final state=%s", j.State)
	}
	acc := s.Ledger().Snapshot()["u"].Usage
	// Exactly 2 tasks * 6 ticks * 2000m conserved; none stranded in Running.
	if acc.Total().MilliCPU != int64(2*6*2000) {
		t.Fatalf("conserved total=%d want=%d", acc.Total().MilliCPU, 2*6*2000)
	}
	if acc.Running.MilliCPU != 0 {
		t.Fatalf("stranded running usage=%d", acc.Running.MilliCPU)
	}
	t.Logf("node-failure scheduling inputs/decisions:\n%s", trace.String())
}

func TestPermanentNodeFailureLostTerminal(t *testing.T) {
	var trace bytes.Buffer
	s := New(failNodes(), DefaultConfig())
	s.SetTrace(&trace)
	_ = s.Submit(api.JobSpec{ID: "j", User: "u", Queue: "q", Priority: 1,
		ArraySize: 2, TaskRes: api.Resources{MilliCPU: 2000, MemoryMB: 256}, Runtime: 6})
	s.Tick()
	// maxRestarts=0: every task on the failed node is unrecoverable -> Lost.
	r := s.Tick()
	_ = r
	if err := s.FailNode("n1", 0); err != nil {
		t.Fatal(err)
	}
	s.Tick()
	j, _ := s.GetJob("j")
	lost, running := 0, 0
	for _, task := range j.Tasks {
		switch task.State {
		case api.TaskLost:
			lost++
			if task.FailureNote == "" {
				t.Fatal("Lost task must carry a failure note")
			}
		case api.TaskRunning:
			running++
		}
	}
	if lost != 1 || running != 1 {
		t.Fatalf("want 1 Lost and 1 Running, got lost=%d running=%d states=%s/%s",
			lost, running, j.Tasks[0].State, j.Tasks[1].State)
	}
	// Job cannot succeed; eventually it is Failed (one Lost + one Finished).
	for i := 0; i < 8; i++ {
		s.Tick()
	}
	j, _ = s.GetJob("j")
	if j.State != api.JobFailed {
		t.Fatalf("job state=%s want Failed (mix of Lost/Finished)", j.State)
	}
	t.Logf("permanent-failure scheduling inputs/decisions:\n%s", trace.String())
}
