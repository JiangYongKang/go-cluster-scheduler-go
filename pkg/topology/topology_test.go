package topology

import (
	"testing"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
)

func TestEvaluateReasons(t *testing.T) {
	nodes := []*api.Node{
		{
			Name: "gpu-a", Capacity: api.Resources{MilliCPU: 4000, MemoryMB: 4096, GPUCount: 2},
			State: api.NodeReady, Labels: map[string]string{"zone": "a"},
			GPUs:      []api.GPU{{ID: 0, Model: "A100", Numa: 0}, {ID: 1, Model: "A100", Numa: 1}},
			NumaNodes: []api.NUMANode{{ID: 0, MilliCPU: 2000, MemoryMB: 2048, GPUIDs: []int{0}}, {ID: 1, MilliCPU: 2000, MemoryMB: 2048, GPUIDs: []int{1}}},
		},
		{
			Name: "gpu-b", Capacity: api.Resources{MilliCPU: 4000, MemoryMB: 4096, GPUCount: 1},
			State: api.NodeReady, Labels: map[string]string{"zone": "b"},
			GPUs:      []api.GPU{{ID: 0, Model: "H100", Numa: 0}},
			NumaNodes: []api.NUMANode{{ID: 0, MilliCPU: 4000, MemoryMB: 4096, GPUIDs: []int{0}}},
		},
		{
			Name: "cpu", Capacity: api.Resources{MilliCPU: 8000, MemoryMB: 8192},
			State: api.NodeReady, Labels: map[string]string{"zone": "a"},
			NumaNodes: []api.NUMANode{{ID: 0, MilliCPU: 8000, MemoryMB: 8192}},
		},
		{
			Name: "dead", Capacity: api.Resources{MilliCPU: 8000, MemoryMB: 8192},
			State:     api.NodeNotReady,
			NumaNodes: []api.NUMANode{{ID: 0, MilliCPU: 8000, MemoryMB: 8192}},
		},
	}
	v := NewView(nodes)

	cases := []struct {
		node   string
		res    api.Resources
		req    api.TopologyReq
		jobID  string
		wantOK bool
		want   api.RejectReason
	}{
		{"dead", api.Resources{MilliCPU: 1}, api.TopologyReq{}, "j", false, api.RejectNodeNotReady},
		{"cpu", api.Resources{MilliCPU: 1, GPUCount: 1}, api.TopologyReq{GPUModel: "A100"}, "j", false, api.RejectGPUModel},
		{"gpu-a", api.Resources{MilliCPU: 1, GPUCount: 1}, api.TopologyReq{GPUModel: "H100"}, "j", false, api.RejectGPUModel},
		{"gpu-b", api.Resources{MilliCPU: 1, GPUCount: 1}, api.TopologyReq{GPUModel: "H100"}, "j", true, ""},
		{"gpu-a", api.Resources{MilliCPU: 3000, MemoryMB: 100, GPUCount: 1}, api.TopologyReq{NumaLocal: true}, "j", false, api.RejectNumaLocal},
		{"gpu-a", api.Resources{MilliCPU: 2000, MemoryMB: 2048, GPUCount: 1}, api.TopologyReq{NumaLocal: true}, "j", true, ""},
		{"gpu-a", api.Resources{MilliCPU: 1}, api.TopologyReq{NodeAffinity: map[string]string{"zone": "b"}}, "j", false, api.RejectNodeAffinity},
		{"gpu-a", api.Resources{MilliCPU: 1}, api.TopologyReq{NodeAffinity: map[string]string{"zone": "a"}}, "j", true, ""},
	}
	for _, c := range cases {
		n := v.Node(c.node)
		ok, reason := v.EvaluateTask(n, c.res, c.req, c.jobID)
		t.Logf("EvaluateTask input: node=%s res=%+v req={model=%q numa=%v affinity=%v} -> ok=%v reason=%q",
			c.node, c.res, c.req.GPUModel, c.req.NumaLocal, c.req.NodeAffinity, ok, reason)
		if ok != c.wantOK || reason != c.want {
			t.Errorf("node=%s got(ok=%v,reason=%q) want(ok=%v,reason=%q)",
				c.node, ok, reason, c.wantOK, c.want)
		}
	}

	// Anti-affinity becomes visible only after placement.
	res := api.Resources{MilliCPU: 100, MemoryMB: 100}
	if err := v.PlaceTask("gpu-a", "j-old", "t-old", res, api.TopologyReq{}); err != nil {
		t.Fatal(err)
	}
	if ok, reason := v.EvaluateTask(v.Node("gpu-a"), res,
		api.TopologyReq{AntiAffinity: []string{"j-old"}}, "j-new"); ok || reason != api.RejectAntiAffinity {
		t.Fatalf("anti-affinity: got ok=%v reason=%q", ok, reason)
	}

	// No silent downgrade: requesting 2 A100 on gpu-b is a model mismatch,
	// not a placement on its single H100.
	if err := v.PlaceTask("gpu-b", "j", "t1",
		api.Resources{MilliCPU: 1, GPUCount: 2}, api.TopologyReq{GPUModel: "A100"}); err == nil {
		t.Fatal("expected placement of wrong GPU model to fail")
	}
	if _, placed := v.TaskPlacement("t1"); placed {
		t.Fatal("rejected task must not be placed")
	}
}
