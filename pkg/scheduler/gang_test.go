package scheduler

import (
	"bytes"
	"strings"
	"testing"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
)

func testNodes() []*api.Node {
	return []*api.Node{
		{Name: "n1", Capacity: api.Resources{MilliCPU: 4000, MemoryMB: 8192, GPUCount: 0}, State: api.NodeReady,
			NumaNodes: []api.NUMANode{{ID: 0, MilliCPU: 4000, MemoryMB: 8192}}},
		{Name: "n2", Capacity: api.Resources{MilliCPU: 4000, MemoryMB: 8192, GPUCount: 0}, State: api.NodeReady,
			NumaNodes: []api.NUMANode{{ID: 0, MilliCPU: 4000, MemoryMB: 8192}}},
	}
}

func TestGangAllOrNothingUnderShortage(t *testing.T) {
	var trace bytes.Buffer
	s := New(testNodes(), DefaultConfig())
	s.SetTrace(&trace)

	// One task needs 3000m CPU; gang of 4 needs 12000m but cluster has 8000m.
	spec := api.JobSpec{
		ID: "j-big", User: "alice", Queue: "q1", Priority: 10,
		ArraySize: 4, TaskRes: api.Resources{MilliCPU: 3000, MemoryMB: 1024},
		Runtime: 3,
	}
	if err := s.Submit(spec); err != nil {
		t.Fatalf("submit: %v", err)
	}

	res := s.Tick()
	j, _ := s.GetJob("j-big")

	if j.RunningTaskCount() != 0 {
		t.Fatalf("expected ZERO tasks started under shortage, got %d running", j.RunningTaskCount())
	}
	if len(res.Placed) != 0 {
		t.Fatalf("expected no placements, got %+v", res.Placed)
	}
	// Raw capacity (8000/3000 => 2 hosts) < gang 4 => hard reject.
	if len(res.Rejected) == 0 {
		t.Fatalf("expected a Rejected decision, got deferred=%+v", res.Deferred)
	}
	// Smaller gang of 2 fits exactly and starts fully.
	spec2 := api.JobSpec{
		ID: "j-fit", User: "bob", Queue: "q1", Priority: 5,
		ArraySize: 2, TaskRes: api.Resources{MilliCPU: 3000, MemoryMB: 1024},
		Runtime: 2,
	}
	if err := s.Submit(spec2); err != nil {
		t.Fatalf("submit j-fit: %v", err)
	}
	s.Tick()
	j2, _ := s.GetJob("j-fit")
	if j2.RunningTaskCount() != 2 {
		t.Fatalf("expected 2 running, got %d", j2.RunningTaskCount())
	}
	if !strings.Contains(trace.String(), "submit job=") {
		t.Fatalf("trace missing scheduling input:\n%s", trace.String())
	}
	if !strings.Contains(trace.String(), "InsufficientResources") &&
		!strings.Contains(trace.String(), "raw capacity") {
		t.Fatalf("trace missing judgment basis:\n%s", trace.String())
	}
	t.Logf("scheduling input trace:\n%s", trace.String())
}
