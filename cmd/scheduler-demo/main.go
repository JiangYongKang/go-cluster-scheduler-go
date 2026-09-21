// Command scheduler-demo runs a fully local end-to-end scenario and prints
// every scheduling decision with its reason: gang scheduling under shortage,
// GPU-model/NUMA/affinity rejection, priority preemption, node-failure
// recovery and quota enforcement. No network, external service or
// credential is used.
//
//	go run ./cmd/scheduler-demo
package main

import (
	"fmt"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/accounting"
	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/scheduler"
)

func main() {
	nodes := []*api.Node{
		{
			Name: "gpu-1", Labels: map[string]string{"zone": "a"}, State: api.NodeReady,
			Capacity: api.Resources{MilliCPU: 8000, MemoryMB: 16384, GPUCount: 2},
			NumaNodes: []api.NUMANode{
				{ID: 0, MilliCPU: 4000, MemoryMB: 8192, GPUIDs: []int{0}},
				{ID: 1, MilliCPU: 4000, MemoryMB: 8192, GPUIDs: []int{1}},
			}, GPUs: []api.GPU{{ID: 0, Model: "A100", Numa: 0}, {ID: 1, Model: "A100", Numa: 1}},
		},
		{
			Name: "gpu-2", Labels: map[string]string{"zone": "a"}, State: api.NodeReady,
			Capacity: api.Resources{MilliCPU: 8000, MemoryMB: 16384, GPUCount: 2},
			NumaNodes: []api.NUMANode{
				{ID: 0, MilliCPU: 4000, MemoryMB: 8192, GPUIDs: []int{0}},
				{ID: 1, MilliCPU: 4000, MemoryMB: 8192, GPUIDs: []int{1}},
			},
			GPUs: []api.GPU{{ID: 0, Model: "A100", Numa: 0}, {ID: 1, Model: "A100", Numa: 1}},
		},
		{
			// cpu-1 is sized so one batch task (3000m) fits but an urgent
			// task (4000m) only fits after preemption.
			Name: "cpu-1", Labels: map[string]string{"zone": "b"}, State: api.NodeReady,
			Capacity:  api.Resources{MilliCPU: 4000, MemoryMB: 8192},
			NumaNodes: []api.NUMANode{{ID: 0, MilliCPU: 4000, MemoryMB: 8192}},
		},
	}

	cfg := scheduler.DefaultConfig()
	s := scheduler.New(nodes, cfg)
	s.SetTrace(traceWriter{})

	// User/queue quotas.
	s.Ledger().SetUserQuota("alice", accounting.Quota{MilliCPU: 12000, MemoryMB: 1 << 30, GPUCount: 2})
	s.Ledger().SetUserQuota("bob", accounting.Quota{MilliCPU: 12000, MemoryMB: 1 << 30, GPUCount: 2})
	s.Ledger().SetQueueQuota("prod", accounting.Quota{MilliCPU: 20000, MemoryMB: 1 << 30, GPUCount: 4})

	must := func(err error, ctx string) {
		if err != nil {
			fmt.Printf("REJECTED [%s]: %v\n", ctx, err)
		}
	}

	// 1) Low-priority batch gang occupies cpu-1 (2 tasks x 3000m).
	must(s.Submit(api.JobSpec{
		ID: "batch-low", User: "alice", Queue: "prod", Priority: 1,
		ArraySize: 2, TaskRes: api.Resources{MilliCPU: 3000, MemoryMB: 1024}, Runtime: 8,
	}), "submit batch-low")
	s.Tick()

	// 2) GPU job with hard NUMA-locality + model + affinity constraints.
	//    Each task pins one A100 and its CPU/memory to that GPU's NUMA node.
	must(s.Submit(api.JobSpec{
		ID: "train", User: "bob", Queue: "prod", Priority: 5,
		ArraySize: 2, TaskRes: api.Resources{MilliCPU: 1000, MemoryMB: 1024, GPUCount: 1},
		Runtime: 3,
		Topology: api.TopologyReq{GPUModel: "A100", NumaLocal: true,
			NodeAffinity: map[string]string{"zone": "a"}},
	}), "submit train")
	s.Tick()

	// 3) Impossible request: H100 GPUs exist nowhere -> hard reject, job fails.
	must(s.Submit(api.JobSpec{
		ID: "train-h100", User: "bob", Queue: "prod", Priority: 5,
		ArraySize: 1, TaskRes: api.Resources{MilliCPU: 1000, MemoryMB: 1024, GPUCount: 1},
		Runtime:  2,
		Topology: api.TopologyReq{GPUModel: "H100"},
	}), "submit train-h100")
	s.Tick()

	// 4) High-priority urgent gang (2 x 4000m) preempts the low batch gang.
	must(s.Submit(api.JobSpec{
		ID: "urgent", User: "carol", Queue: "prod", Priority: 9,
		ArraySize: 2, TaskRes: api.Resources{MilliCPU: 4000, MemoryMB: 1024}, Runtime: 3,
	}), "submit urgent")
	s.Tick()

	// 5) gpu-1 becomes unreachable: the train tasks are evicted and requeued.
	must(s.SetNodeState("gpu-1", api.NodeNotReady), "gpu-1 down")
	s.Tick()
	// Recovers: train resumes (accrued runtime is retained).
	must(s.SetNodeState("gpu-1", api.NodeReady), "gpu-1 up")
	for i := 0; i < 8; i++ {
		s.Tick()
	}

	fmt.Println("\n=== Final job states ===")
	for _, j := range s.ListJobs() {
		fmt.Printf("job=%-12s state=%-9s tasks=%d\n", j.Spec.ID, j.State, len(j.Tasks))
	}

	fmt.Println("\n=== Conserved resource-time ledger (user, cpu-milli-ticks) ===")
	for name, a := range s.Ledger().Snapshot() {
		fmt.Printf("user=%-6s running=%d finished=%d preempted=%d failed=%d canceled=%d total=%d\n",
			name, a.Usage.Running.MilliCPU, a.Usage.Finished.MilliCPU,
			a.Usage.Preempted.MilliCPU, a.Usage.Failed.MilliCPU,
			a.Usage.Canceled.MilliCPU, a.Usage.Total().MilliCPU)
	}
}

type traceWriter struct{}

func (traceWriter) Write(p []byte) (int, error) {
	fmt.Print("  " + string(p))
	return len(p), nil
}
