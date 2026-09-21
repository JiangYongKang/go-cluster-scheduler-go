package scheduler

import (
	"bytes"
	"testing"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
)

func preemptNodes() []*api.Node {
	return []*api.Node{
		{Name: "n1", Capacity: api.Resources{MilliCPU: 4000, MemoryMB: 8192}, State: api.NodeReady,
			NumaNodes: []api.NUMANode{{ID: 0, MilliCPU: 4000, MemoryMB: 8192}}},
	}
}

// totalUserResourceTime is the conserved cumulative usage of one user.
func totalUserUsage(s *Scheduler, user string) api.Resources {
	return s.Ledger().Snapshot()[user].Usage.Total()
}

func runningUsage(s *Scheduler, user string) api.Resources {
	return s.Ledger().Snapshot()[user].Usage.Running
}

func TestPriorityPreemptionSettlement(t *testing.T) {
	var trace bytes.Buffer
	s := New(preemptNodes(), DefaultConfig())
	s.SetTrace(&trace)

	// Low-priority job occupies the only node (2000m), needs 10 ticks.
	// Tick 1 places it; accrual starts on the tick after placement.
	low := api.JobSpec{ID: "low", User: "u-low", Queue: "q", Priority: 1,
		ArraySize: 1, TaskRes: api.Resources{MilliCPU: 2000, MemoryMB: 512}, Runtime: 10}
	if err := s.Submit(low); err != nil {
		t.Fatal(err)
	}
	s.Tick()
	s.Tick()
	s.Tick()
	s.Tick() // place@1, accrue@2,@3,@4
	jLow, _ := s.GetJob("low")
	if jLow.Tasks[0].RunTicks != 3 {
		t.Fatalf("expected 3 run ticks, got %d", jLow.Tasks[0].RunTicks)
	}
	_ = jLow

	// High-priority job needs 4000m -> must preempt low entirely.
	high := api.JobSpec{ID: "high", User: "u-high", Queue: "q", Priority: 9,
		ArraySize: 1, TaskRes: api.Resources{MilliCPU: 4000, MemoryMB: 512}, Runtime: 3}
	if err := s.Submit(high); err != nil {
		t.Fatal(err)
	}
	// tick5: preempt low, place high.
	r := s.Tick()
	if len(r.Preempted) != 1 || len(r.Placed) != 1 {
		t.Fatalf("expected 1 preempt + 1 place, got preempt=%d place=%d",
			len(r.Preempted), len(r.Placed))
	}
	jLow, _ = s.GetJob("low")
	if jLow.Tasks[0].State != api.TaskPreempted {
		t.Fatalf("low task state=%s", jLow.Tasks[0].State)
	}
	// At the preempt tick the victim first accrues (tick 5), then is
	// preempted: exactly 4 ticks are settled as Preempted, 6 remain.
	if got := totalUserUsage(s, "u-low"); got.MilliCPU != 2000*4 {
		t.Fatalf("low conserved usage=%d want=%d", got.MilliCPU, 2000*4)
	}
	if got := s.Ledger().Snapshot()["u-low"].Usage.Preempted.MilliCPU; got != 2000*4 {
		t.Fatalf("preempted bucket=%d want=%d", got, 2000*4)
	}
	if got := runningUsage(s, "u-low").MilliCPU; got != 0 {
		t.Fatalf("low running bucket must be 0 after settle, got %d", got)
	}
	if jLow.Tasks[0].RemainingTicks() != 6 {
		t.Fatalf("remaining=%d want 6", jLow.Tasks[0].RemainingTicks())
	}
	// Allocation released.
	if alloc := s.Ledger().AllocatedUser("u-low").MilliCPU; alloc != 0 {
		t.Fatalf("low alloc not released: %d", alloc)
	}

	// ticks 5..7: high finishes at tick 7; low re-runs at tick 8..14 (7 ticks).
	// high placed@5 accrues @6,@7,@8 and completes at tick 8.
	s.Tick()
	s.Tick()
	s.Tick()
	jHigh, _ := s.GetJob("high")
	if jHigh.State != api.JobSucceeded {
		t.Fatalf("high state=%s", jHigh.State)
	}
	// low re-placed at tick 9, accrues @10..@15 (6 ticks), completes @15.
	for i := 0; i < 7; i++ {
		s.Tick()
	}
	jLow, _ = s.GetJob("low")
	if jLow.State != api.JobSucceeded {
		t.Fatalf("low final state=%s tasks=%+v", jLow.State, jLow.Tasks[0])
	}
	// Conservation: low total = 10 ticks * 2000m; 4 preempted + 6 finished.
	acc := s.Ledger().Snapshot()["u-low"]
	if acc.Usage.Total().MilliCPU != 2000*10 {
		t.Fatalf("low total usage=%d want=%d", acc.Usage.Total().MilliCPU, 2000*10)
	}
	if acc.Usage.Finished.MilliCPU != 2000*6 {
		t.Fatalf("low finished=%d want=%d", acc.Usage.Finished.MilliCPU, 2000*6)
	}
	if acc.Usage.Preempted.MilliCPU != 2000*4 {
		t.Fatalf("low preempted=%d must remain %d (no double count)",
			acc.Usage.Preempted.MilliCPU, 2000*4)
	}
	if acc.Usage.Running.MilliCPU != 0 {
		t.Fatalf("running bucket must be empty at quiescence, got %d", acc.Usage.Running.MilliCPU)
	}
	t.Logf("preemption inputs/decisions:\n%s", trace.String())
}

func TestNoPreemptionOfEqualOrHigher(t *testing.T) {
	s := New(preemptNodes(), DefaultConfig())
	_ = s.Submit(api.JobSpec{ID: "a", User: "ua", Queue: "q", Priority: 5,
		ArraySize: 1, TaskRes: api.Resources{MilliCPU: 3000, MemoryMB: 128}, Runtime: 5})
	s.Tick()
	// Same priority, different user, no usage advantage -> cannot preempt.
	_ = s.Submit(api.JobSpec{ID: "b", User: "ub", Queue: "q", Priority: 5,
		ArraySize: 1, TaskRes: api.Resources{MilliCPU: 3000, MemoryMB: 128}, Runtime: 5})
	r := s.Tick()
	if len(r.Preempted) != 0 {
		t.Fatalf("equal priority must not preempt, got %+v", r.Preempted)
	}
	if len(r.Deferred) == 0 {
		t.Fatalf("incoming job must be deferred, got %+v", r)
	}
	// Lower priority certainly cannot preempt the running priority-5 job.
	_ = s.Submit(api.JobSpec{ID: "c", User: "uc", Queue: "q", Priority: 1,
		ArraySize: 1, TaskRes: api.Resources{MilliCPU: 3000, MemoryMB: 128}, Runtime: 5})
	r = s.Tick()
	if len(r.Preempted) != 0 {
		t.Fatalf("lower priority must not preempt, got %+v", r.Preempted)
	}
}

func TestRepeatedPreemptionNoDoubleCount(t *testing.T) {
	// One node; a victim is preempted twice by two waves of high-priority
	// jobs. Each preempted segment is settled once; accrued ticks survive.
	s := New(preemptNodes(), DefaultConfig())
	_ = s.Submit(api.JobSpec{ID: "victim", User: "lo", Queue: "q", Priority: 1,
		ArraySize: 1, TaskRes: api.Resources{MilliCPU: 1000, MemoryMB: 64}, Runtime: 12})
	s.Tick() // place @1
	s.Tick()
	s.Tick() // accrue @2,@3 (2 ticks)

	_ = s.Submit(api.JobSpec{ID: "h1", User: "hi1", Queue: "q", Priority: 9,
		ArraySize: 1, TaskRes: api.Resources{MilliCPU: 4000, MemoryMB: 64}, Runtime: 2})
	s.Tick() // @4: victim accrues (3 total), then preempted; h1 placed
	acc := s.Ledger().Snapshot()["lo"].Usage
	if acc.Preempted.MilliCPU != 1000*3 {
		t.Fatalf("after 1st preempt bucket=%d want %d", acc.Preempted.MilliCPU, 1000*3)
	}

	// h1 placed@4 accrues @5,@6 and completes at tick6; the victim is
	// re-placed in the same scheduling phase of tick6 and accrues @7,@8.
	s.Tick()
	s.Tick()
	s.Tick() // @7 accrue
	s.Tick() // @8 accrue: total run ticks = 3 + 2 = 5
	jv, _ := s.GetJob("victim")
	if jv.Tasks[0].RunTicks != 5 {
		t.Fatalf("run ticks=%d want 5 (3+2)", jv.Tasks[0].RunTicks)
	}

	_ = s.Submit(api.JobSpec{ID: "h2", User: "hi2", Queue: "q", Priority: 9,
		ArraySize: 1, TaskRes: api.Resources{MilliCPU: 4000, MemoryMB: 64}, Runtime: 1})
	s.Tick() // @9: second segment accrues (@7,@8,@9 = 3 ticks), preempted
	acc = s.Ledger().Snapshot()["lo"].Usage
	if acc.Preempted.MilliCPU != 1000*6 {
		t.Fatalf("after 2nd preempt bucket=%d want %d", acc.Preempted.MilliCPU, 1000*6)
	}
	if acc.Running.MilliCPU != 0 {
		t.Fatalf("running bucket must be zero after settle, got %d", acc.Running.MilliCPU)
	}

	// h2 placed@9 accrues@10 and completes at tick10; the victim is re-placed
	// in that same phase and accrues its final 6 ticks.
	for i := 0; i < 10; i++ {
		s.Tick()
	}
	jv, _ = s.GetJob("victim")
	if jv.State != api.JobSucceeded {
		t.Fatalf("victim final=%s", jv.State)
	}
	acc = s.Ledger().Snapshot()["lo"].Usage
	if acc.Total().MilliCPU != 1000*12 {
		t.Fatalf("conserved total=%d want %d", acc.Total().MilliCPU, 1000*12)
	}
	// Preempted bucket frozen at 6; finished holds the other 6.
	if acc.Preempted.MilliCPU != 1000*6 || acc.Finished.MilliCPU != 1000*6 {
		t.Fatalf("split wrong: preempted=%d finished=%d", acc.Preempted.MilliCPU, acc.Finished.MilliCPU)
	}
}
