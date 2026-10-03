package scheduler

import (
	"testing"
)

// 钉选的设备不存在：GPU_ABSENT，不降级到同节点其它设备。
func TestPinnedGPUAbsentNoFallback(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, numaNode()) // 只有 gpu0(域0)/gpu1(域1)
	t.Logf("INPUT node nu has gpu0@zone0 gpu1@zone1; task pins missing gpu9")
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("t", 1, 1, -1, "gpu9")}})
	rpt := mustSchedule(t, s)
	if len(rpt.StartedJobs) != 0 {
		t.Fatalf("job pinning absent GPU must not start (no fallback), got %v", rpt.StartedJobs)
	}
	rs := rpt.Unschedulable[0].NodeReasons["nu"]
	if !containsReason(rs, FilterGPUAbsent) {
		t.Fatalf("want GPU_ABSENT, got %v", rs)
	}
	if containsReason(rs, FilterGPUZoneMismatch) {
		t.Fatalf("absent device must not also be flagged as zone mismatch: %v", rs)
	}
	t.Logf("JUDGE pinned gpu9 absent -> %v; never fell back to gpu0/gpu1", rs)
}

// 同时指定设备与资源域但设备属于别的域：GPU_ZONE_MISMATCH，
// 既不能用该设备（跨域），也不能退回域内其它设备。
func TestPinnedGPUDeviceZoneMismatch(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, numaNode())
	t.Logf("INPUT task pins gpu1 (belongs zone1) but requires NUMA zone0")
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("t", 1, 1, 0, "gpu1")}})
	rpt := mustSchedule(t, s)
	if len(rpt.StartedJobs) != 0 {
		t.Fatalf("cross-zone pinned GPU must not start, got %v", rpt.StartedJobs)
	}
	rs := rpt.Unschedulable[0].NodeReasons["nu"]
	if !containsReason(rs, FilterGPUZoneMismatch) {
		t.Fatalf("want GPU_ZONE_MISMATCH, got %v", rs)
	}
	if containsReason(rs, FilterGPUBusy) {
		t.Fatalf("gpu1 is free; must not report busy: %v", rs)
	}
	t.Logf("JUDGE gpu1 not in zone0 -> GPU_ZONE_MISMATCH %v; no fallback to gpu0", rs)
}

// 钉设备且指定正确域：必须确实落在该设备与该域上。
func TestPinnedGPURunsOnExactDeviceAndZone(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, numaNode())
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("t", 1, 1, 1, "gpu1")}})
	mustSchedule(t, s)
	tk := s.GetJob("j").Tasks[0]
	if tk.GPU != "gpu1" || tk.NUMAID != 1 || tk.Node != "nu" {
		t.Fatalf("want nu/gpu1/zone1, got %s/%s/%d", tk.Node, tk.GPU, tk.NUMAID)
	}
	t.Logf("JUDGE pinned task landed exactly on nu zone1 gpu1")
}

// 钉设备但不指定域（NUMA 节点）：CPU/内存/GPU 必须同落在设备归属域。
// 当归属域 CPU 不足时必须判 NUMA_ZONE_EXHAUSTED，不得借另一域的 CPU 跑。
func TestPinnedGPUWithoutZonePinsHomeZone(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, numaNode()) // 每域 4cpu/8mem
	// 先用一个不钉 GPU 的任务占满域1 的 3cpu（选 gpu1）。
	mustSubmit(t, s, JobSpec{ID: "occ", User: "u9", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("o", 3, 1, 1, "")}})
	mustSchedule(t, s)
	if tk := s.GetJob("occ").Tasks[0]; tk.GPU != "gpu1" || tk.NUMAID != 1 {
		t.Fatalf("setup: occupier should take gpu1/zone1, got %s/%d", tk.GPU, tk.NUMAID)
	}
	t.Logf("INPUT occupier holds 3cpu+gpu1 in zone1; task pins gpu1 with no NUMA id, needs 2cpu same zone")

	// 钉 gpu1：设备忙 -> GPU_BUSY（不退回 gpu0）。
	mustSubmit(t, s, JobSpec{ID: "busy", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("b", 1, 1, -1, "gpu1")}})
	rpt := mustSchedule(t, s)
	u := unschedFor(rpt, "busy")
	if u == nil {
		t.Fatalf("busy must be unschedulable")
	}
	rs := u.NodeReasons["nu"]
	if !containsReason(rs, FilterGPUBusy) {
		t.Fatalf("want GPU_BUSY for pinned busy gpu1, got %v", rs)
	}
	t.Logf("JUDGE pinned gpu1 busy -> GPU_BUSY %v; no fallback to gpu0/zone0", rs)

	// 钉空闲的 gpu0（域0），但要 5cpu > 域0 的 4cpu：
	// 必须 NUMA_ZONE_EXHAUSTED，不能借域1 的 CPU，也不能换域。
	mustSubmit(t, s, JobSpec{ID: "zonefull", User: "u2", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("z", 5, 1, -1, "gpu0")}})
	rpt = mustSchedule(t, s)
	u2 := unschedFor(rpt, "zonefull")
	if u2 == nil {
		t.Fatalf("zonefull must be unschedulable")
	}
	rs = u2.NodeReasons["nu"]
	if !containsReason(rs, FilterNUMAExhaust) {
		t.Fatalf("want NUMA_ZONE_EXHAUSTED in home zone0, got %v", rs)
	}
	if containsReason(rs, FilterGPUZoneMismatch) {
		t.Fatalf("device zone not specified by user; no mismatch should be reported: %v", rs)
	}
	t.Logf("JUDGE pinned gpu0 home zone0 cannot fit 5cpu -> NUMA_ZONE_EXHAUSTED %v (no cross-zone CPU)", rs)
}

// 钉设备不指定域、资源足够时：落在设备归属域且资源确实记在该域账本。
func TestPinnedGPUWithoutZoneCommitsHomeZone(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, numaNode())
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("t", 2, 2, -1, "gpu1")}})
	mustSchedule(t, s)
	tk := s.GetJob("j").Tasks[0]
	if tk.GPU != "gpu1" || tk.NUMAID != 1 {
		t.Fatalf("want gpu1/zone1, got %s/%d", tk.GPU, tk.NUMAID)
	}
	n := s.nodes["nu"]
	if n.numaCPU[1] != 2 || n.numaMem[1] != 2 || !n.numaGPU[1]["gpu1"] {
		t.Fatalf("resources must book in zone1: cpu=%d mem=%d gpu=%v",
			n.numaCPU[1], n.numaMem[1], n.numaGPU[1])
	}
	if n.numaCPU[0] != 0 {
		t.Fatalf("zone0 must stay empty, got cpu=%d", n.numaCPU[0])
	}
	t.Logf("JUDGE pinned gpu1 task booked CPU/mem/GPU together in zone1")
}

// 平铺（无资源域）节点也必须严格尊重钉设备：
// 指定 gpu1 时，即使 gpu0 空闲且编号更靠前，也必须落在 gpu1。
func TestPinnedGPUOnFlatNodeLandsOnExactDevice(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "flat", CPU: 8, Memory: 16,
		GPUs: []GPUDevice{{ID: "gpu0", NUMAID: -1}, {ID: "gpu1", NUMAID: -1}}})
	t.Logf("INPUT flat node (no NUMA zones) has gpu0,gpu1; task pins gpu1 while gpu0 is also free")
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("t", 1, 1, -1, "gpu1")}})
	mustSchedule(t, s)
	tk := s.GetJob("j").Tasks[0]
	if tk.State != TaskRunning || tk.Node != "flat" || tk.GPU != "gpu1" {
		t.Fatalf("pinned task must land on flat/gpu1, got node=%s gpu=%s state=%s",
			tk.Node, tk.GPU, tk.State)
	}
	n := s.nodes["flat"]
	if !n.busyGPUs["gpu1"] {
		t.Fatalf("gpu1 must be marked busy, busyGPUs=%v", n.busyGPUs)
	}
	if n.busyGPUs["gpu0"] {
		t.Fatalf("gpu0 must stay free, busyGPUs=%v", n.busyGPUs)
	}
	t.Logf("JUDGE pinned task on flat node landed exactly on gpu1; gpu0 untouched, busy=%v", n.busyGPUs)
}

// 平铺节点上钉选的设备已被占用：GPU_BUSY，绝不退回同节点编号更靠前的空闲卡。
func TestPinnedGPUBusyOnFlatNodeNoFallback(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "flat", CPU: 8, Memory: 16,
		GPUs: []GPUDevice{{ID: "gpu0", NUMAID: -1}, {ID: "gpu1", NUMAID: -1}}})
	// occ 钉住 gpu1；gpu0 保持空闲。
	mustSubmit(t, s, JobSpec{ID: "occ", User: "u9", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("o", 1, 1, -1, "gpu1")}})
	mustSchedule(t, s)
	t.Logf("INPUT occ holds gpu1 on flat node; j pins gpu1 (busy) while gpu0 is free")
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("t", 1, 1, -1, "gpu1")}})
	rpt := mustSchedule(t, s)
	if len(rpt.StartedJobs) != 0 {
		t.Fatalf("job pinning busy gpu1 must not start, got %v", rpt.StartedJobs)
	}
	rs := rpt.Unschedulable[0].NodeReasons["flat"]
	if !containsReason(rs, FilterGPUBusy) {
		t.Fatalf("want GPU_BUSY on flat, got %v", rs)
	}
	if containsReason(rs, FilterGPUZoneMismatch) {
		t.Fatalf("flat node has no zones: GPU_ZONE_MISMATCH must not appear, got %v", rs)
	}
	if s.GetJob("occ").Tasks[0].GPU != "gpu1" {
		t.Fatalf("occupier must remain on gpu1, got %s", s.GetJob("occ").Tasks[0].GPU)
	}
	t.Logf("JUDGE pinned busy gpu1 on flat -> %v; never fell back to free gpu0", rs)
}

// 平铺节点上钉选不存在的设备：GPU_ABSENT，不启动、不换卡。
func TestPinnedGPUAbsentOnFlatNode(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "flat", CPU: 8, Memory: 16,
		GPUs: []GPUDevice{{ID: "gpu0", NUMAID: -1}}})
	t.Logf("INPUT flat node has only gpu0; task pins missing gpu7")
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("t", 1, 1, -1, "gpu7")}})
	rpt := mustSchedule(t, s)
	if len(rpt.StartedJobs) != 0 {
		t.Fatalf("job pinning absent gpu7 must not start, got %v", rpt.StartedJobs)
	}
	rs := rpt.Unschedulable[0].NodeReasons["flat"]
	if !containsReason(rs, FilterGPUAbsent) {
		t.Fatalf("want GPU_ABSENT on flat, got %v", rs)
	}
	if containsReason(rs, FilterGPUBusy) || containsReason(rs, FilterGPUZoneMismatch) {
		t.Fatalf("absent device must only report GPU_ABSENT, got %v", rs)
	}
	t.Logf("JUDGE pinned gpu7 absent on flat node -> %v; no fallback to gpu0", rs)
}

// unschedFor 按作业 ID 取出本周期该作业的不可调度记录。
func unschedFor(rpt *ScheduleReport, id string) *UnschedulableJob {
	for i := range rpt.Unschedulable {
		if rpt.Unschedulable[i].JobID == id {
			return &rpt.Unschedulable[i]
		}
	}
	return nil
}
