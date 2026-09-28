package scheduler

import (
	"testing"
)

func unschedFor(rpt *ScheduleReport, jobID string) UnschedulableJob {
	for _, u := range rpt.Unschedulable {
		if u.JobID == jobID {
			return u
		}
	}
	return UnschedulableJob{}
}

// 钉选设备必须真的落在该设备上；指定域时设备必须属于该域。
// 设备不存在 / 已被占用 / 不在指定域，三类原因必须可区分，且绝不降级。
func TestPinnedDeviceZoneStrictness(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, numaNode()) // 域0=gpu0，域1=gpu1

	// 场景 1：指定 gpu1 但要求域 0 -> GPU_ZONE_MISMATCH，不得落到 gpu0 或域1。
	t.Logf("INPUT pin gpu1 with NUMA zone0 (gpu1 belongs to zone1): must be ZONE_MISMATCH")
	mustSubmit(t, s, JobSpec{ID: "j-zone", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("t", 1, 1, 0, "gpu1")}})
	rpt := mustSchedule(t, s)
	if len(rpt.StartedJobs) != 0 {
		t.Fatalf("zone-mismatched pin must not start")
	}
	rs := rpt.Unschedulable[0].NodeReasons["nu"]
	if !containsReason(rs, FilterGPUZoneMismatch) {
		t.Fatalf("want GPU_ZONE_MISMATCH, got %v", rs)
	}
	t.Logf("JUDGE pinned gpu1@zone0 -> %v (no cross-zone/cross-device fallback)", rs)

	// 场景 2：指定一个节点上根本不存在的设备 -> GPU_ABSENT。
	t.Logf("INPUT pin gpu9 (absent): must be GPU_ABSENT")
	mustSubmit(t, s, JobSpec{ID: "j-absent", User: "u2", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("t", 1, 1, -1, "gpu9")}})
	rpt = mustSchedule(t, s)
	rs = unschedFor(rpt, "j-absent").NodeReasons["nu"]
	if !containsReason(rs, FilterGPUAbsent) || containsReason(rs, FilterGPUZoneMismatch) {
		t.Fatalf("want exactly GPU_ABSENT, got %v", rs)
	}
	t.Logf("JUDGE absent device -> %v", rs)

	// 场景 3：钉选设备合法且空闲 -> 必须真的跑在那块设备及其所属域。
	t.Logf("INPUT pin gpu0 (zone0): must run on gpu0/zone0")
	mustSubmit(t, s, JobSpec{ID: "j-ok", User: "u3", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("t", 1, 1, 0, "gpu0")}})
	rpt = mustSchedule(t, s)
	if len(rpt.StartedJobs) != 1 {
		t.Fatalf("legal pin must start, got unschedulable %+v", rpt.Unschedulable)
	}
	info := s.GetJob("j-ok").Tasks[0]
	if info.GPU != "gpu0" || info.NUMAID != 0 || info.Node != "nu" {
		t.Fatalf("want gpu0/zone0/nu, got %s/%d/%s", info.GPU, info.NUMAID, info.Node)
	}
	t.Logf("JUDGE legal pin actually runs on device=%s zone=%d node=%s", info.GPU, info.NUMAID, info.Node)

	// 场景 4：gpu0 已被占用，另一个作业钉 gpu0 -> GPU_BUSY，不得换成 gpu1。
	t.Logf("INPUT pin busy gpu0 again: must be GPU_BUSY, no fallback to gpu1")
	mustSubmit(t, s, JobSpec{ID: "j-busy", User: "u4", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("t", 1, 1, 0, "gpu0")}})
	rpt = mustSchedule(t, s)
	if len(rpt.StartedJobs) != 0 {
		t.Fatalf("busy pinned device must not start (and must not switch device)")
	}
	rs = unschedFor(rpt, "j-busy").NodeReasons["nu"]
	if !containsReason(rs, FilterGPUBusy) || containsReason(rs, FilterGPUZoneMismatch) {
		t.Fatalf("want GPU_BUSY without zone mismatch, got %v", rs)
	}
	t.Logf("JUDGE busy pinned device -> %v", rs)
}

// 设备在别的节点上存在、但硬亲和只允许本节点：不得跨节点借用设备。
func TestPinnedDeviceAbsentOnAffinityNode(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n-a", CPU: 4, Memory: 8,
		GPUs:   []GPUDevice{{ID: "g-a", NUMAID: -1}},
		Labels: map[string]string{"role": "a"}})
	mustAddNode(t, s, NodeSpec{Name: "n-b", CPU: 4, Memory: 8,
		GPUs:   []GPUDevice{{ID: "g-b", NUMAID: -1}},
		Labels: map[string]string{"role": "b"}})
	t.Logf("INPUT pin g-a but hard-require label role=b (only n-b matches, which lacks g-a)")
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1,
		Affinity: Affinity{Required: &LabelSelector{MatchLabels: map[string]string{"role": "b"}}},
		Tasks:    []TaskSpec{mkGPUTask("t", 1, 1, -1, "g-a")}})
	rpt := mustSchedule(t, s)
	if len(rpt.StartedJobs) != 0 {
		t.Fatalf("job must not borrow g-a across affinity boundary")
	}
	u := rpt.Unschedulable[0]
	if !containsReason(u.NodeReasons["n-b"], FilterGPUAbsent) ||
		!containsReason(u.NodeReasons["n-a"], FilterLabelMismatch) {
		t.Fatalf("want GPU_ABSENT on n-b and LABEL_MISMATCH on n-a, got %v", u.NodeReasons)
	}
	t.Logf("JUDGE device on other node not borrowed; affinity node reports %v", u.NodeReasons)
}

// 域内 CPU/内存与设备归属必须对得上：钉 gpu0（域0）但 CPU 需求超出域0容量，
// 只能以 NUMA_ZONE_EXHAUSTED 等待，不得迁到域1 的 gpu1。
func TestPinnedDeviceZoneResourceAlignment(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, numaNode()) // 每域 4 cpu / 8 mem
	t.Logf("INPUT pin gpu0(zone0) but request 8 cpu > zone0 capacity 4")
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("t", 8, 1, 0, "gpu0")}})
	rpt := mustSchedule(t, s)
	if len(rpt.StartedJobs) != 0 {
		t.Fatalf("zone-overfull pinned job must not start")
	}
	rs := rpt.Unschedulable[0].NodeReasons["nu"]
	if !containsReason(rs, FilterNUMAExhaust) {
		t.Fatalf("want NUMA_ZONE_EXHAUSTED, got %v", rs)
	}
	t.Logf("JUDGE pinned gpu0 with overfull zone0 CPU -> %v (stays in zone0, never zone1)", rs)
}

// 多节点集群：钉选设备只存在于某节点时，即使该节点 CPU 不足、
// 其它节点空闲也不得改放其它节点/其它设备。
func TestPinnedDeviceNoCrossNodeFallback(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n-gpu", CPU: 4, Memory: 8,
		GPUs: []GPUDevice{{ID: "gpu0", NUMAID: -1}}})
	mustAddNode(t, s, NodeSpec{Name: "n-plain", CPU: 64, Memory: 64})
	t.Logf("INPUT pin gpu0 with 32 cpu: n-gpu lacks CPU, n-plain lacks device")
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkGPUTask("t", 32, 1, -1, "gpu0")}})
	rpt := mustSchedule(t, s)
	if len(rpt.StartedJobs) != 0 {
		t.Fatalf("must not move pinned job onto device-less node")
	}
	u := rpt.Unschedulable[0]
	if !containsReason(u.NodeReasons["n-gpu"], FilterCPU) ||
		!containsReason(u.NodeReasons["n-plain"], FilterGPUAbsent) {
		t.Fatalf("want CPU insufficient on n-gpu and GPU_ABSENT on n-plain, got %v", u.NodeReasons)
	}
	t.Logf("JUDGE pinned-device placement not downgraded: %v", u.NodeReasons)
}
