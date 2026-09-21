package scheduler

import (
	"fmt"
	"sync"
	"testing"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/accounting"
	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
)

// buildScenario submits a fixed set of jobs and ticks deterministically.
func buildScenario() []*api.Job {
	nodes := []*api.Node{
		{Name: "n1", Capacity: api.Resources{MilliCPU: 6000, MemoryMB: 8192}, State: api.NodeReady,
			NumaNodes: []api.NUMANode{{ID: 0, MilliCPU: 6000, MemoryMB: 8192}}},
		{Name: "n2", Capacity: api.Resources{MilliCPU: 6000, MemoryMB: 8192}, State: api.NodeReady,
			NumaNodes: []api.NUMANode{{ID: 0, MilliCPU: 6000, MemoryMB: 8192}}},
	}
	s := New(nodes, DefaultConfig())
	for i := 0; i < 6; i++ {
		_ = s.Submit(api.JobSpec{
			ID: fmt.Sprintf("j%02d", i), User: fmt.Sprintf("u%d", i%3), Queue: "q",
			Priority: (i % 3) + 1, ArraySize: 2,
			TaskRes: api.Resources{MilliCPU: 2000, MemoryMB: 128}, Runtime: 4,
		})
	}
	for i := 0; i < 12; i++ {
		s.Tick()
	}
	return s.ListJobs()
}

func sig(jobs []*api.Job) string {
	var b []byte
	for _, j := range jobs {
		b = append(b, j.Spec.ID...)
		b = append(b, byte(j.State+'0'))
		for _, t := range j.Tasks {
			b = append(b, ';')
			b = append(b, []byte(t.ID)...)
			b = append(b, byte(t.State+'0'))
			b = append(b, t.NodeName...)
			b = append(b, byte(t.RunTicks+'0'))
		}
		b = append(b, '\n')
	}
	return string(b)
}

func TestDeterministicSameInput(t *testing.T) {
	a := sig(buildScenario())
	b := sig(buildScenario())
	if a != b {
		t.Fatalf("non-deterministic results\n--- A ---\n%s\n--- B ---\n%s", a, b)
	}
}

func TestConcurrentAPISafety(t *testing.T) {
	nodes := []*api.Node{
		{Name: "n1", Capacity: api.Resources{MilliCPU: 20000, MemoryMB: 1 << 20}, State: api.NodeReady,
			NumaNodes: []api.NUMANode{{ID: 0, MilliCPU: 20000, MemoryMB: 1 << 20}}},
	}
	s := New(nodes, DefaultConfig())
	var wg sync.WaitGroup

	wg.Add(1)
	go func() { // bounded ticker
		defer wg.Done()
		for i := 0; i < 400; i++ {
			s.Tick()
		}
	}()
	wg.Add(1)
	go func() { // submitter
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = s.Submit(api.JobSpec{
				ID: fmt.Sprintf("c%04d", i), User: "u", Queue: "q", Priority: i % 5,
				ArraySize: 1 + i%3, TaskRes: api.Resources{MilliCPU: 500, MemoryMB: 64},
				Runtime: 2,
			})
		}
	}()
	wg.Add(1)
	go func() { // canceler + reader
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = s.Cancel(fmt.Sprintf("c%04d", i), "stress")
			_ = s.ListJobs()
			_ = s.Decisions()
		}
	}()
	wg.Add(1)
	go func() { // node flapping
		defer wg.Done()
		for i := 0; i < 100; i++ {
			st := api.NodeReady
			if i%2 == 0 {
				st = api.NodeNotReady
			}
			_ = s.SetNodeState("n1", st)
		}
		_ = s.SetNodeState("n1", api.NodeReady)
	}()
	wg.Wait()

	// Invariant: every task id appears on at most one node in the placement
	// view and instantaneous allocation matches running tasks exactly.
	tasksRunning := map[string]api.Resources{}
	var runningRes api.Resources
	for _, j := range s.ListJobs() {
		for _, task := range j.Tasks {
			if task.State == api.TaskRunning {
				if _, dup := tasksRunning[task.ID]; dup {
					t.Fatalf("task %s double-booked", task.ID)
				}
				tasksRunning[task.ID] = j.Spec.TaskRes
				runningRes = api.Add(runningRes, j.Spec.TaskRes)
				if _, ok := s.view.TaskPlacement(task.ID); !ok {
					t.Fatalf("running task %s missing placement", task.ID)
				}
			}
		}
	}
	alloc := s.Ledger().AllocatedUser("u")
	if alloc.MilliCPU != runningRes.MilliCPU {
		t.Fatalf("allocation/run mismatch: ledger=%d tasks=%d", alloc.MilliCPU, runningRes.MilliCPU)
	}
}

func TestDecisionLimits(t *testing.T) {
	cfg := Config{
		MaxJobs: 2, MaxTasksPerJob: 3, MaxPlacementsPerTick: 2,
		MaxPreemptPerTick: 1, MaxDecisionLog: 5,
	}
	s := New(testNodes(), cfg)
	if err := s.Submit(api.JobSpec{ID: "toobig", User: "u", Queue: "q", ArraySize: 4, Runtime: 1}); err == nil {
		t.Fatal("array over limit must be rejected")
	}
	_ = s.Submit(api.JobSpec{ID: "a", User: "u", Queue: "q", ArraySize: 1, Runtime: 1})
	_ = s.Submit(api.JobSpec{ID: "b", User: "u2", Queue: "q", ArraySize: 1, Runtime: 1})
	if err := s.Submit(api.JobSpec{ID: "c", User: "u3", Queue: "q", ArraySize: 1, Runtime: 1}); err == nil {
		t.Fatal("max live jobs must be enforced")
	}
	s.Tick()
	s.Tick()
	// Gang of 3 with a 2-per-tick cap must be deferred.
	s.Ledger().SetUserQuota("big", accounting.Quota{MilliCPU: 1 << 30})
	_ = s.Submit(api.JobSpec{ID: "d", User: "big", Queue: "q", ArraySize: 3,
		TaskRes: api.Resources{MilliCPU: 100, MemoryMB: 8}, Runtime: 1})
	r := s.Tick()
	if len(r.Deferred) == 0 {
		t.Fatalf("oversized gang should be deferred by per-tick cap: %+v", r)
	}
	for i := 0; i < 10; i++ {
		s.Tick()
	}
	if len(s.Decisions()) > cfg.MaxDecisionLog {
		t.Fatalf("decision log unbounded: %d > %d", len(s.Decisions()), cfg.MaxDecisionLog)
	}
}
