package simulation

import (
	"testing"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/scheduler"
)

func nodes() []*api.Node {
	return []*api.Node{
		{Name: "n1", Capacity: api.Resources{MilliCPU: 4000, MemoryMB: 8192}, State: api.NodeReady,
			NumaNodes: []api.NUMANode{{ID: 0, MilliCPU: 4000, MemoryMB: 8192}}},
		{Name: "n2", Capacity: api.Resources{MilliCPU: 4000, MemoryMB: 8192}, State: api.NodeReady,
			NumaNodes: []api.NUMANode{{ID: 0, MilliCPU: 4000, MemoryMB: 8192}}},
	}
}

func TestTimelineArrayAggregationAndFailure(t *testing.T) {
	s := scheduler.New(nodes(), scheduler.DefaultConfig())
	r := NewRunner(s)

	arr := &api.JobSpec{ID: "arr", User: "u", Queue: "q", Priority: 1,
		ArraySize: 4, TaskRes: api.Resources{MilliCPU: 2000, MemoryMB: 256}, Runtime: 5}
	events := []Event{
		{AtTick: 0, Kind: "submit", Job: arr},
		{AtTick: 3, Kind: "failnode", NodeName: "n1", MaxRestarts: 0},
	}
	results := r.Run(events, 20)

	j, _ := s.GetJob("arr")
	// Array participates as a whole: all 4 started on the first feasible tick.
	first := results[0]
	if len(first.Placed) != 4 {
		t.Fatalf("gang of 4 must place together, got %d", len(first.Placed))
	}
	// At tick 3 two tasks on n1 become Lost permanently.
	if j.State != api.JobFailed {
		t.Fatalf("array with Lost members must aggregate to Failed, got %s", j.State)
	}
	lost := 0
	for _, task := range j.Tasks {
		if task.State == api.TaskLost {
			lost++
		}
	}
	if lost != 2 {
		t.Fatalf("want 2 lost tasks, got %d", lost)
	}
}

func TestTimelinePreemptionAndComplete(t *testing.T) {
	s := scheduler.New(nodes(), scheduler.DefaultConfig())
	r := NewRunner(s)
	low := &api.JobSpec{ID: "low", User: "a", Queue: "q", Priority: 1,
		ArraySize: 2, TaskRes: api.Resources{MilliCPU: 2000, MemoryMB: 128}, Runtime: 6}
	high := &api.JobSpec{ID: "high", User: "b", Queue: "q", Priority: 9,
		ArraySize: 2, TaskRes: api.Resources{MilliCPU: 2000, MemoryMB: 128}, Runtime: 2}
	events := []Event{
		{AtTick: 0, Kind: "submit", Job: low},
		{AtTick: 4, Kind: "submit", Job: high},
	}
	r.Run(events, 25)
	lj, _ := s.GetJob("low")
	hj, _ := s.GetJob("high")
	if hj.State != api.JobSucceeded || lj.State != api.JobSucceeded {
		t.Fatalf("want both succeeded, low=%s high=%s", lj.State, hj.State)
	}
	// Determinism of the timeline: rerun and compare the decision log.
	s2 := scheduler.New(nodes(), scheduler.DefaultConfig())
	NewRunner(s2).Run(events, 25)
	d1, d2 := s.Decisions(), s2.Decisions()
	if len(d1) != len(d2) {
		t.Fatalf("decision length differs: %d vs %d", len(d1), len(d2))
	}
	for i := range d1 {
		if d1[i].String() != d2[i].String() {
			t.Fatalf("decision %d differs:\n%s\n%s", i, d1[i], d2[i])
		}
	}
}
