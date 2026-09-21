package scheduler

import (
	"testing"
	"time"
)

// 测试约定：每个用例在开始处打印调度输入，在判定处打印依据，
// 使失败可解释、可复现。

func newTestScheduler(t *testing.T, cfg *Config, quota *QuotaConfig) *Scheduler {
	t.Helper()
	if cfg == nil {
		cfg = &Config{MaxDecisionLog: 10000}
	} else if cfg.MaxDecisionLog == 0 {
		cfg.MaxDecisionLog = 10000
	}
	s := New(cfg, quota)
	s.SetClock(t0)
	return s
}

func mustAddNode(t *testing.T, s *Scheduler, spec NodeSpec) {
	t.Helper()
	if err := s.AddNode(spec); err != nil {
		t.Fatalf("AddNode(%s) unexpected error: %v", spec.Name, err)
	}
}

func mustSubmit(t *testing.T, s *Scheduler, spec JobSpec) {
	t.Helper()
	t.Logf("INPUT submit job id=%s user=%s queue=%s prio=%d tasks=%d",
		spec.ID, spec.User, spec.Queue, spec.Priority, len(spec.Tasks))
	if err := s.Submit(spec); err != nil {
		t.Fatalf("Submit(%s) unexpected error: %v", spec.ID, err)
	}
}

func mustSchedule(t *testing.T, s *Scheduler) *ScheduleReport {
	t.Helper()
	rpt, err := s.Schedule()
	if err != nil {
		t.Fatalf("Schedule unexpected error: %v", err)
	}
	for _, u := range rpt.Unschedulable {
		t.Logf("JUDGE unschedulable job=%s reason=%s detail=%s", u.JobID, u.Reason, u.Detail)
	}
	for _, id := range rpt.StartedJobs {
		t.Logf("JUDGE started job=%s tasks=%v", id, rpt.StartedTasks[id])
	}
	return rpt
}

func assertTaskStates(t *testing.T, s *Scheduler, jobID string, want map[string]TaskState) {
	t.Helper()
	info := s.GetJob(jobID)
	if info == nil {
		t.Fatalf("job %s not found", jobID)
	}
	got := map[string]TaskState{}
	for _, tk := range info.Tasks {
		got[tk.Name] = tk.State
	}
	for name, st := range want {
		if got[name] != st {
			t.Fatalf("job %s task %s state = %s, want %s", jobID, name, got[name], st)
		}
	}
}

var t0 = time.Unix(1_700_000_000, 0)

// task 构造一个不感知 NUMA 的普通任务（NUMAID=-1）。
func mkTask(name string, cpu, mem int64) TaskSpec {
	return TaskSpec{Name: name, NUMAID: -1, Resources: Resources{CPU: cpu, Memory: mem}}
}

// gpuTask 构造申请 1 块 GPU 的任务，可指定 NUMA 域（-1 不指定）与设备（"" 任意）。
func mkGPUTask(name string, cpu, mem int64, numa int, dev string) TaskSpec {
	return TaskSpec{Name: name, NUMAID: numa, GPUDevice: dev,
		Resources: Resources{CPU: cpu, Memory: mem, GPU: 1}}
}
