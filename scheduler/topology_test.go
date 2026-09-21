package scheduler

import (
	"sort"
	"testing"
)

func numaNode() NodeSpec {
	// 2 个 NUMA 域，各 4 cpu/8 mem，域0挂 gpu0，域1挂 gpu1；节点总量与之相等。
	return NodeSpec{
		Name: "nu", CPU: 8, Memory: 16,
		NUMA: []NUMAZone{
			{ID: 0, CPUs: 4, Mem: 8, GPUs: []string{"gpu0"}},
			{ID: 1, CPUs: 4, Mem: 8, GPUs: []string{"gpu1"}},
		},
		GPUs: []GPUDevice{{ID: "gpu0", NUMAID: 0}, {ID: "gpu1", NUMAID: 1}},
	}
}

// GPU 设备钉选：指定设备忙时不得降级到其他设备。
func TestGPUAffinityNoSilentFallback(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, numaNode())

	// j1 钉 gpu0（域0）。
	j1 := JobSpec{ID: "j1", User: "u1", Queue: "q1", Priority: 10,
		Tasks: []TaskSpec{mkGPUTask("t", 1, 1, 0, "gpu0")}}
	mustSubmit(t, s, j1)
	mustSchedule(t, s)
	if info := s.GetJob("j1"); info.Tasks[0].GPU != "gpu0" || info.Tasks[0].NUMAID != 0 {
		t.Fatalf("j1 want gpu0/numa0, got %s/%d", info.Tasks[0].GPU, info.Tasks[0].NUMAID)
	}

	// j2 也钉 gpu0：必须被拒，原因含 GPU_BUSY，且不得跑到 gpu1。
	j2 := JobSpec{ID: "j2", User: "u2", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkGPUTask("t", 1, 1, 0, "gpu0")}}
	mustSubmit(t, s, j2)
	rpt := mustSchedule(t, s)
	if len(rpt.StartedJobs) != 0 {
		t.Fatalf("j2 must not start on busy pinned GPU")
	}
	u := rpt.Unschedulable[0]
	if !containsReason(u.NodeReasons["nu"], FilterGPUBusy) {
		t.Fatalf("want GPU_BUSY reason on nu, got %v", u.NodeReasons)
	}
	t.Logf("JUDGE j2 pinned gpu0 busy -> reasons %v (no fallback to gpu1)", u.NodeReasons)

	// j3 申请任意 GPU 但要求域0：域0 的 gpu0 忙，域1 的 gpu1 空闲也不能用。
	j3 := JobSpec{ID: "j3", User: "u3", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkGPUTask("t", 1, 1, 0, "")}}
	mustSubmit(t, s, j3)
	rpt = mustSchedule(t, s)
	u = rpt.Unschedulable[0]
	if !containsReason(u.NodeReasons["nu"], FilterGPUBusy) {
		t.Fatalf("want GPU_BUSY for NUMA-pinned zone0, got %v", u.NodeReasons)
	}
	t.Logf("JUDGE j3 zone0 GPU exhausted -> %v (no cross-zone fallback)", u.NodeReasons)
}

// NUMA 域不存在必须以 NUMA_ZONE_ABSENT 拒绝。
func TestNUMAAbsent(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "flat", CPU: 8, Memory: 8}) // 无 NUMA
	j := JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("t", 1, 1)}}
	j.Tasks[0].NUMAID = 3
	mustSubmit(t, s, j)
	rpt := mustSchedule(t, s)
	rs := rpt.Unschedulable[0].NodeReasons["flat"]
	if !containsReason(rs, FilterNUMAAbsent) {
		t.Fatalf("want NUMA_ZONE_ABSENT, got %v", rs)
	}
	t.Logf("JUDGE NUMA zone 3 absent on flat node -> %v", rs)
}

// 节点标签硬亲和不满足 -> LABEL_MISMATCH，且不得调度到违反标签的节点。
func TestRequiredLabelAffinity(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "gpu-node", CPU: 4, Memory: 4, Labels: map[string]string{"accel": "gpu"}})
	mustAddNode(t, s, NodeSpec{Name: "cpu-node", CPU: 64, Memory: 64, Labels: map[string]string{"accel": "cpu"}})
	j := JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1,
		Affinity: Affinity{Required: &LabelSelector{MatchLabels: map[string]string{"accel": "gpu"}}},
		Tasks:    []TaskSpec{mkTask("t", 2, 2)}}
	mustSubmit(t, s, j)
	rpt := mustSchedule(t, s)
	if len(rpt.StartedJobs) != 1 {
		t.Fatalf("want scheduled on gpu-node, got %+v", rpt.Unschedulable)
	}
	if got := s.GetJob("j").Tasks[0].Node; got != "gpu-node" {
		t.Fatalf("want gpu-node, got %s (silent constraint violation)", got)
	}
	t.Logf("JUDGE required label satisfied only on gpu-node; placed there")

	// 提高需求使 gpu-node 放不下：必须整体不调度，不得降级到 cpu-node。
	j2 := JobSpec{ID: "j2", User: "u1", Queue: "q1", Priority: 1,
		Affinity: Affinity{Required: &LabelSelector{MatchLabels: map[string]string{"accel": "gpu"}}},
		Tasks:    []TaskSpec{mkTask("t", 32, 32)}}
	mustSubmit(t, s, j2)
	rpt = mustSchedule(t, s)
	u := rpt.Unschedulable[0]
	if !containsReason(u.NodeReasons["gpu-node"], FilterCPU) ||
		!containsReason(u.NodeReasons["cpu-node"], FilterLabelMismatch) {
		t.Fatalf("want CPU_INSUFFICIENT on gpu-node and LABEL_MISMATCH on cpu-node, got %v", u.NodeReasons)
	}
	t.Logf("JUDGE no silent downgrade: %v", u.NodeReasons)
}

// 反亲和：两个作业不得落在同一节点。
func TestAntiAffinity(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 8, Memory: 8})
	mustAddNode(t, s, NodeSpec{Name: "n2", CPU: 8, Memory: 8})
	j1 := JobSpec{ID: "j1", User: "u1", Queue: "q1", Priority: 10,
		Tasks: []TaskSpec{mkTask("t", 2, 2)}}
	mustSubmit(t, s, j1)
	mustSchedule(t, s)
	n1node := s.GetJob("j1").Tasks[0].Node

	j2 := JobSpec{ID: "j2", User: "u2", Queue: "q1", Priority: 5,
		Affinity: Affinity{AntiAffinityWith: []string{"j1"}},
		Tasks:    []TaskSpec{mkTask("t", 2, 2)}}
	mustSubmit(t, s, j2)
	mustSchedule(t, s)
	got := s.GetJob("j2").Tasks[0].Node
	if got == n1node {
		t.Fatalf("anti-affinity violated: both on %s", got)
	}
	t.Logf("JUDGE j1 on %s, j2 anti-affinitized to %s", n1node, got)
}

func containsReason(list []FilterReason, want FilterReason) bool {
	for _, r := range list {
		if r == want {
			return true
		}
	}
	sort.Strings([]string{})
	return false
}
