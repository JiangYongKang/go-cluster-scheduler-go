package scheduler

import (
	"testing"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/accounting"
	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
)

func TestQuotasRejectOverExpansion(t *testing.T) {
	s := New(testNodes(), DefaultConfig())
	s.Ledger().SetUserQuota("alice", accounting.Quota{MilliCPU: 5000, MemoryMB: 1 << 30})
	s.Ledger().SetQueueQuota("q1", accounting.Quota{MilliCPU: 6000, MemoryMB: 1 << 30})

	// Gang of 2 * 3000m = 6000m exceeds alice's 5000m user quota.
	err := s.Submit(api.JobSpec{ID: "big", User: "alice", Queue: "q1", Priority: 1,
		ArraySize: 2, TaskRes: api.Resources{MilliCPU: 3000, MemoryMB: 128}, Runtime: 2})
	if err == nil {
		t.Fatal("expected quota rejection")
	}
	se, ok := api.AsError(err)
	if !ok || se.Kind != api.ErrQuotaExceeded {
		t.Fatalf("kind=%v want ErrQuotaExceeded err=%v", se, err)
	}
	if jobs := s.ListJobs(); len(jobs) != 0 {
		t.Fatalf("rejected submit must not create job state, got %d", len(jobs))
	}

	// Fits user quota (3000) but queue quota is 6000 and bob joins with 4000.
	s.Ledger().SetUserQuota("bob", accounting.Quota{MilliCPU: 9999})
	if err := s.Submit(api.JobSpec{ID: "a", User: "alice", Queue: "q1", Priority: 1,
		ArraySize: 1, TaskRes: api.Resources{MilliCPU: 3000, MemoryMB: 128}, Runtime: 4}); err != nil {
		t.Fatal(err)
	}
	s.Tick() // place a
	err = s.Submit(api.JobSpec{ID: "b", User: "bob", Queue: "q1", Priority: 1,
		ArraySize: 1, TaskRes: api.Resources{MilliCPU: 4000, MemoryMB: 128}, Runtime: 4})
	if err == nil {
		t.Fatal("expected queue quota rejection")
	}
	if se, ok := api.AsError(err); !ok || se.Kind != api.ErrQueueQuotaExceeded {
		t.Fatalf("want queue quota exceeded, got %v", err)
	}
	// Queue allocation must remain exactly what running job a holds.
	if q := s.Ledger().AllocatedQueue("q1").MilliCPU; q != 3000 {
		t.Fatalf("queue alloc=%d want 3000", q)
	}
}

func TestCancelConservationNoResidue(t *testing.T) {
	s := New(testNodes(), DefaultConfig())
	_ = s.Submit(api.JobSpec{ID: "j", User: "u", Queue: "q", Priority: 1,
		ArraySize: 2, TaskRes: api.Resources{MilliCPU: 2000, MemoryMB: 256}, Runtime: 8})
	s.Tick() // place both at tick 1
	s.Tick() // accrue at tick 2
	if err := s.Cancel("j", "user asked"); err != nil {
		t.Fatal(err)
	}
	j, _ := s.GetJob("j")
	if j.State != api.JobCanceled {
		t.Fatalf("state=%s", j.State)
	}
	for _, task := range j.Tasks {
		if task.State != api.TaskCanceled {
			t.Fatalf("%s state=%s", task.ID, task.State)
		}
		if _, ok := s.view.TaskPlacement(task.ID); ok {
			t.Fatalf("canceled task %s still placed", task.ID)
		}
	}
	if alloc := s.Ledger().AllocatedUser("u").MilliCPU; alloc != 0 {
		t.Fatalf("allocations must be released after cancel, got %d", alloc)
	}
	acc := s.Ledger().Snapshot()["u"].Usage
	// Both tasks accrued exactly one tick (tick 2) before cancel.
	if acc.Canceled.MilliCPU != 2*2000 {
		t.Fatalf("canceled bucket=%d want=%d", acc.Canceled.MilliCPU, 2*2000)
	}
	if acc.Running.MilliCPU != 0 || acc.Total().MilliCPU != 2*2000 {
		t.Fatalf("usage not conserved: running=%d total=%d", acc.Running.MilliCPU, acc.Total().MilliCPU)
	}
	// Cancel is terminal.
	if err := s.Cancel("j", "again"); err == nil {
		t.Fatal("double cancel must error")
	}
}

func TestInvalidSubmissions(t *testing.T) {
	s := New(testNodes(), DefaultConfig())
	cases := []api.JobSpec{
		{ID: "", ArraySize: 1, Runtime: 1},
		{ID: "z", ArraySize: 0, Runtime: 1},
		{ID: "z", ArraySize: 1, Runtime: 0},
		{ID: "z", ArraySize: 1, Runtime: -1},
	}
	for i, spec := range cases {
		spec.User, spec.Queue = "u", "q"
		if err := s.Submit(spec); err == nil {
			t.Fatalf("case %d expected error", i)
		}
	}
}
