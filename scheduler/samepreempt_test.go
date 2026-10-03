package scheduler

import (
	"testing"
	"time"
)

// 用户场景复现：4 核节点上同优先级长跑作业占满资源，跑 10 分钟后提交同优先级
// 新作业；新作业已消耗份额更小，必须顶掉长跑作业（而不是一直排队）。
func TestSamePriorityPreemptionLongVsNew(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})

	// long：同优先级长跑作业，4cpu 占满整节点。
	mustSubmit(t, s, JobSpec{ID: "long", User: "u1", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	mustSchedule(t, s)
	t.Logf("INPUT node=4cpu; long(prio5,4cpu) fills node and runs 10m")

	// 长跑 10 分钟，累计消耗 4cpu*10m。
	s.Tick(t0.Add(10*time.Minute), nil)
	clong := s.consumedShare(s.jobs["long"], s.now)
	t.Logf("JUDGE long consumed share after 10m = %.0f (cpu*s)", clong)
	if clong <= 0 {
		t.Fatalf("long must have positive consumed share, got %v", clong)
	}

	// new：同优先级新作业，已消耗份额为 0，必须能顶上去。
	mustSubmit(t, s, JobSpec{ID: "new", User: "u2", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	rpt := mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 1 || rpt.PreemptedJobs[0].PreemptedJobID != "long" {
		got := []string{}
		for _, p := range rpt.PreemptedJobs {
			got = append(got, p.PreemptedJobID)
		}
		t.Fatalf("want same-priority victim=[long], got %v", got)
	}
	if st := s.GetJob("new").Tasks[0].State; st != TaskRunning {
		t.Fatalf("new must be running after same-priority preemption, got %s", st)
	}
	if st := s.GetJob("long").Tasks[0].State; st != TaskPreempted {
		t.Fatalf("long must be preempted, got %s", st)
	}
	t.Logf("JUDGE new(share=0) preempted long(share=%.0f) at same priority", clong)

	// 份额守恒：long 的运行份额转为等量预留，allocated 不变。
	snap := s.Snapshot()
	if snap.Running.CPU != 4 || snap.Allocated.CPU != 8 {
		t.Fatalf("want running=4 allocated=8, got run=%d alloc=%d",
			snap.Running.CPU, snap.Allocated.CPU)
	}
	t.Logf("JUDGE share conserved: running=%d allocated=%d", snap.Running.CPU, snap.Allocated.CPU)

	// new 结束后 long 必须能重启：不被历史消耗压着起不来。
	s.Tick(t0.Add(11*time.Minute), []string{"new/t"})
	mustSchedule(t, s)
	if st := s.GetJob("long").Tasks[0].State; st != TaskRunning {
		t.Fatalf("long must restart after new finishes, got %s", st)
	}
	t.Logf("JUDGE long restarted after winner finished; not starved by historical consumption")
}

// 反向：消耗更多的一方不能抢走消耗更少的一方。老作业被高优先级抢过、
// 背着大额历史消耗重新排队时，不得顶掉刚跑起来的同优先级新作业。
func TestSamePriorityMoreConsumedCannotPreemptLess(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})

	// old 先跑 10 分钟，攒下大额已消耗份额，然后被高优先级 hi 抢走。
	mustSubmit(t, s, JobSpec{ID: "old", User: "u1", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	mustSchedule(t, s)
	s.Tick(t0.Add(10*time.Minute), nil)
	mustSubmit(t, s, JobSpec{ID: "hi", User: "u9", Queue: "q1", Priority: 100,
		Tasks: []TaskSpec{mkTask("h", 4, 1)}})
	mustSchedule(t, s)
	if st := s.GetJob("old").Tasks[0].State; st != TaskPreempted {
		t.Fatalf("setup: old must be preempted by hi, got %s", st)
	}
	// hi 迅速结束；fresh 同优先级新作业先拿到资源跑 1 分钟。
	s.Tick(t0.Add(11*time.Minute), []string{"hi/h"})
	mustSubmit(t, s, JobSpec{ID: "fresh", User: "u2", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	mustSchedule(t, s)
	if st := s.GetJob("fresh").Tasks[0].State; st != TaskRunning {
		t.Fatalf("setup: fresh must be running, got %s", st)
	}
	s.Tick(t0.Add(12*time.Minute), nil)

	cold := s.consumedShare(s.jobs["old"], s.now)
	cfresh := s.consumedShare(s.jobs["fresh"], s.now)
	t.Logf("INPUT old(preempted, consumed=%.0f) pending vs fresh(running, consumed=%.0f)", cold, cfresh)
	if !(cold > cfresh) {
		t.Fatalf("setup: old consumed share must exceed fresh, got old=%v fresh=%v", cold, cfresh)
	}

	// old 重新参与调度：fresh 份额更小，绝不被 old 抢走；原因可区分。
	rpt := mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 0 {
		t.Fatalf("more-consumed old must not preempt less-consumed fresh, got %+v", rpt.PreemptedJobs)
	}
	u := unschedFor(rpt, "old")
	if u == nil || u.Reason != ReasonNoPreemptableVictim {
		t.Fatalf("want NO_PREEMPTABLE_VICTIM for old, got %+v", u)
	}
	if st := s.GetJob("fresh").Tasks[0].State; st != TaskRunning {
		t.Fatalf("fresh must keep running, got %s", st)
	}
	t.Logf("JUDGE old(share %.0f) cannot preempt fresh(share %.0f): %s", cold, cfresh, u.Reason)

	// fresh 结束后 old 自然重启（排队按持有份额，不被历史消耗永久压制）。
	s.Tick(t0.Add(13*time.Minute), []string{"fresh/t"})
	mustSchedule(t, s)
	if st := s.GetJob("old").Tasks[0].State; st != TaskRunning {
		t.Fatalf("old must restart once fresh finishes, got %s", st)
	}
	t.Logf("JUDGE old restarted after fresh finished; queued by held share, not suppressed")
}

// 同优先级多个候选受害者：按归一化已消耗份额降序让位，且只做最小化抢占。
func TestSamePriorityVictimOrderByConsumedShare(t *testing.T) {
	q := &QuotaConfig{UserWeights: map[string]float64{"big": 2}}
	s := newTestScheduler(t, nil, q)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 8, Memory: 8})

	// 三个同优先级作业同时在跑：a(4cpu,u1)、b(2cpu,u2)、c(2cpu,big 权重2)。
	mustSubmit(t, s, JobSpec{ID: "a", User: "u1", Queue: "q1", Priority: 3,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	mustSubmit(t, s, JobSpec{ID: "b", User: "u2", Queue: "q1", Priority: 3,
		Tasks: []TaskSpec{mkTask("t", 2, 1)}})
	mustSubmit(t, s, JobSpec{ID: "c", User: "big", Queue: "q1", Priority: 3,
		Tasks: []TaskSpec{mkTask("t", 2, 1)}})
	mustSchedule(t, s)
	t.Logf("INPUT a=4cpu/u1, b=2cpu/u2, c=2cpu/big(weight2); all prio3; run 10m")

	s.Tick(t0.Add(10*time.Minute), nil)
	ca, cb, cc := s.consumedShare(s.jobs["a"], s.now),
		s.consumedShare(s.jobs["b"], s.now), s.consumedShare(s.jobs["c"], s.now)
	t.Logf("JUDGE consumed shares: a=%.0f b=%.0f c=%.0f (a biggest; c halved by weight)", ca, cb, cc)
	if !(ca > cb && cb > cc) {
		t.Fatalf("want a>b>c consumed shares, got a=%v b=%v c=%v", ca, cb, cc)
	}

	// 新作业要 4cpu：只需让出份额最大的 a（4cpu 恰好够），b/c 不动。
	mustSubmit(t, s, JobSpec{ID: "new", User: "u3", Queue: "q1", Priority: 3,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	rpt := mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 1 || rpt.PreemptedJobs[0].PreemptedJobID != "a" {
		got := []string{}
		for _, p := range rpt.PreemptedJobs {
			got = append(got, p.PreemptedJobID)
		}
		t.Fatalf("want minimal victim set [a], got %v", got)
	}
	assertTaskStates(t, s, "b", map[string]TaskState{"t": TaskRunning})
	assertTaskStates(t, s, "c", map[string]TaskState{"t": TaskRunning})
	t.Logf("JUDGE minimal preemption: only biggest-share a yielded; b,c keep running")
}
