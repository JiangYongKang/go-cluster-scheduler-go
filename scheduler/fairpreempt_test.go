package scheduler

import (
	"testing"
	"time"
)

// 同优先级受害者选择：在“运行任务数/提交先后”旧规则下最安全的长跑多耗作业，
// 必须因归一化已消耗资源时间更大而最先被让出。
func TestFairPreemptionVictimByConsumedShare(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 10, Memory: 10})

	// long：单任务 8cpu，最早提交并启动，长时间占用。
	mustSubmit(t, s, JobSpec{ID: "long", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("a", 8, 1)}})
	mustSchedule(t, s)
	// short：2 个 1cpu 任务，晚提交；运行任务数更多、提交更新，但消耗时间很短。
	mustSubmit(t, s, JobSpec{ID: "short", User: "u2", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("a", 1, 1), mkTask("b", 1, 1)}})
	mustSchedule(t, s)

	t.Logf("INPUT long=1 task x8cpu submitted first; short=2 tasks x1cpu submitted later; node=10cpu")

	// long 跑 10 分钟，short 仅跑 1 分钟。
	s.Tick(t0.Add(10*time.Minute), nil)
	clong := s.consumedShare(s.jobs["long"], s.now)
	cshort := s.consumedShare(s.jobs["short"], s.now)
	t.Logf("JUDGE consumed share before preempt: long=%.0f short=%.0f (long must be greater)", clong, cshort)
	if !(clong > cshort) {
		t.Fatalf("long consumed share must exceed short, got long=%v short=%v", clong, cshort)
	}

	// 高优先级作业要 8cpu：旧规则会先抢任务数更少的 long（恰好够），
	// 新规则同样应选消耗最大的 long 为唯一受害者。
	mustSubmit(t, s, JobSpec{ID: "urgent", User: "u3", Queue: "q1", Priority: 100,
		Tasks: []TaskSpec{mkTask("u", 8, 1)}})
	rpt := mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 1 || rpt.PreemptedJobs[0].PreemptedJobID != "long" {
		got := []string{}
		for _, p := range rpt.PreemptedJobs {
			got = append(got, p.PreemptedJobID)
		}
		t.Fatalf("want victim=[long], got %v", got)
	}
	t.Logf("JUDGE victim=%s (biggest normalized consumed share), short keeps running",
		rpt.PreemptedJobs[0].PreemptedJobID)
	if st := s.GetJob("short").Tasks[0].State; st != TaskRunning {
		t.Fatalf("short tasks must keep running, got %s", st)
	}
}

// 权重归一化：高权重用户的作业虽然消耗时间相近，归一化后份额更小，
// 应晚于低权重用户的作业被抢占。
func TestFairPreemptionWeightNormalized(t *testing.T) {
	q := &QuotaConfig{
		UserWeights:  map[string]float64{"heavy": 4, "light": 1},
		QueueWeights: map[string]float64{"q1": 1},
	}
	s := newTestScheduler(t, nil, q)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 8, Memory: 8})

	mustSubmit(t, s, JobSpec{ID: "heavyJob", User: "heavy", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("a", 4, 1)}})
	mustSubmit(t, s, JobSpec{ID: "lightJob", User: "light", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("a", 4, 1)}})
	mustSchedule(t, s)
	t.Logf("INPUT heavy(weight4)/light(weight1) each 4cpu, same elapsed time")

	s.Tick(t0.Add(time.Minute), nil)
	ch := s.consumedShare(s.jobs["heavyJob"], s.now)
	cl := s.consumedShare(s.jobs["lightJob"], s.now)
	t.Logf("JUDGE normalized consumed: heavy=%.2f light=%.2f (raw equal, light normalized bigger)", ch, cl)
	if !(cl > ch) {
		t.Fatalf("light normalized share must exceed heavy, got light=%v heavy=%v", cl, ch)
	}

	mustSubmit(t, s, JobSpec{ID: "urgent", User: "u3", Queue: "q1", Priority: 100,
		Tasks: []TaskSpec{mkTask("u", 4, 1)}})
	rpt := mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 1 || rpt.PreemptedJobs[0].PreemptedJobID != "lightJob" {
		got := []string{}
		for _, p := range rpt.PreemptedJobs {
			got = append(got, p.PreemptedJobID)
		}
		t.Fatalf("want victim=[lightJob] (larger normalized share), got %v", got)
	}
	t.Logf("JUDGE victim=%s; weight-normalized fairness applied", rpt.PreemptedJobs[0].PreemptedJobID)
}

// 被抢占后重启不被永久压住，且多轮抢占运行时段按段累加。
func TestFairPreemptionRestartsAndSegmentsAccumulate(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "vic", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("v", 4, 1)}})
	mustSchedule(t, s)

	// 第 1 轮：vic 跑 1 分钟被抢。
	mustSubmit(t, s, JobSpec{ID: "h1", User: "u2", Queue: "q1", Priority: 100,
		Tasks: []TaskSpec{mkTask("h", 4, 1)}})
	s.Tick(t0.Add(time.Minute), nil)
	rpt := mustSchedule(t, s)
	if rpt.PreemptedJobs[0].PreemptedJobID != "vic" {
		t.Fatalf("want vic preempted by h1")
	}
	b := s.ResourceTimes().Jobs["vic"].Buckets
	if b.Preempted.CPUNanos != int64(4)*int64(time.Minute) {
		t.Fatalf("preempted bucket want 4cpu*1m, got %v", b.Preempted)
	}
	t.Logf("JUDGE seg1 preempted 4cpu*1m; buckets preempted=%v total=%v", b.Preempted.CPUNanos, b.Total.CPUNanos)

	// h1 结束后 vic 必须能重新启动（不被历史消耗永久压住）。
	s.Tick(t0.Add(2*time.Minute), []string{"h1/h"})
	mustSchedule(t, s)
	if st := s.GetJob("vic").Tasks[0].State; st != TaskRunning {
		t.Fatalf("vic must restart, got %s", st)
	}
	t.Logf("JUDGE vic restarted after winner completion; not starved by historical consumption")

	// 第 2 轮：vic 在 2m..4m 又跑 2 分钟，再次被抢；旧段 1m + 新段 2m = 3m 累计。
	mustSubmit(t, s, JobSpec{ID: "h2", User: "u2", Queue: "q1", Priority: 100,
		Tasks: []TaskSpec{mkTask("h", 4, 1)}})
	s.Tick(t0.Add(4*time.Minute), nil)
	rpt = mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 1 {
		t.Fatalf("want second preemption, got %d", len(rpt.PreemptedJobs))
	}
	if rt := s.GetJob("vic").Tasks[0].Runtime; rt != 3*time.Minute {
		t.Fatalf("vic runtime want 3m (1m+2m segments), got %s", rt)
	}
	b = s.ResourceTimes().Jobs["vic"].Buckets
	want := int64(4) * int64(3*time.Minute)
	if b.Preempted.CPUNanos != want || b.Total.CPUNanos != want {
		t.Fatalf("preempted/total want 4cpu*3m=%d, got preempted=%d total=%d",
			want, b.Preempted.CPUNanos, b.Total.CPUNanos)
	}
	if b.Running.CPUNanos != 0 {
		t.Fatalf("preempted task must have no running segment, got %d", b.Running.CPUNanos)
	}
	t.Logf("JUDGE seg2 preempted 4cpu*2m; accumulated preempted=4cpu*3m, no double/missing count")
}
