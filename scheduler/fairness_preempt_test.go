package scheduler

import (
	"testing"
	"time"
)

// 同优先级公平抢占：长跑、累计消耗多的作业先让出；消耗相同（都为 0）时不抢。
func TestSamePriorityPreemptionByConsumedShare(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 8})

	// long：4cpu，占满节点先跑 10 分钟；shortA 此时无法启动，保持排队。
	mustSubmit(t, s, JobSpec{ID: "long", User: "u-long", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	mustSubmit(t, s, JobSpec{ID: "shortA", User: "u-a", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	mustSchedule(t, s)
	s.Tick(t0.Add(10*time.Minute), nil)
	t.Logf("INPUT long ran 10m and stays running; shortA waits; new same-priority shortB arrives")

	mustSubmit(t, s, JobSpec{ID: "shortB", User: "u-b", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	rpt := mustSchedule(t, s)
	// shortA 与 shortB 消耗都为 0，long 消耗 40 cpu-min：两者都只能抢 long；
	// 一个周期内先到队首者抢到并启动，另一个继续等待。
	if len(rpt.PreemptedJobs) != 1 {
		t.Fatalf("want exactly 1 same-priority preemption, got %d", len(rpt.PreemptedJobs))
	}
	rec := rpt.PreemptedJobs[0]
	if rec.PreemptedJobID != "long" || !rec.SamePriority {
		t.Fatalf("want long preempted via same-priority fairness, got %+v", rec)
	}
	t.Logf("JUDGE victim=%s (consumed 4cpu*10m) preempted; samePrio=%v",
		rec.PreemptedJobID, rec.SamePriority)
	if rt := s.GetJob("long").Tasks[0].Runtime; rt != 10*time.Minute {
		t.Fatalf("long settled runtime = %s, want 10m", rt)
	}
	started := rpt.StartedJobs[0]
	if st := s.GetJob(started).Tasks[0].State; st != TaskRunning {
		t.Fatalf("%s must be running, got %s", started, st)
	}
	t.Logf("JUDGE zero-consumed newcomer %s started after fairness preemption", started)
}

// 同优先级但消耗相等（双方都没跑过）不得抢占，保持排队顺序语义。
func TestSamePriorityNoPreemptionWhenConsumedEqual(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "a", User: "u1", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	mustSubmit(t, s, JobSpec{ID: "b", User: "u2", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	mustSchedule(t, s)
	t.Logf("INPUT a and b same priority, a starts; third same-priority job c with zero consumed arrives")
	mustSubmit(t, s, JobSpec{ID: "c", User: "u3", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	// a 几乎没消耗（同一模拟时刻启动），c 已消耗为 0：不满足“严格更多”，不得抢占。
	rpt := mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 0 || len(rpt.StartedJobs) != 0 {
		t.Fatalf("equal consumed share must not preempt, got %+v", rpt)
	}
	if st := s.GetJob("a").Tasks[0].State; st != TaskRunning {
		t.Fatalf("a must remain running, got %s", st)
	}
	t.Logf("JUDGE equal consumed -> no same-priority preemption, a keeps running")
}

// 多轮同优先级抢占连续成立：同一作业被抢、重启、再被抢，
// 运行时间按段累加（10m + 5m），资源时间分桶不重不漏。
func TestSamePriorityRepeatedPreemptionChains(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})

	mustSubmit(t, s, JobSpec{ID: "long", User: "u-long", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	mustSchedule(t, s)

	// 第 1 轮：long 跑 10m 后被 s1 抢。
	mustSubmit(t, s, JobSpec{ID: "s1", User: "u1", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	s.Tick(t0.Add(10*time.Minute), nil)
	rpt := mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 1 || rpt.PreemptedJobs[0].PreemptedJobID != "long" {
		t.Fatalf("round1 want long preempted, got %+v", rpt.PreemptedJobs)
	}

	// s1 2 分钟后完成；long 在 12m 重启。
	s.Tick(t0.Add(12*time.Minute), []string{"s1/t"})
	mustSchedule(t, s)
	if st := s.GetJob("long").Tasks[0].State; st != TaskRunning {
		t.Fatalf("long should restart at 12m, got %s", st)
	}

	// 第 2 轮：long 从 12m 跑到 17m（5m 新段），s2 到达再抢。
	mustSubmit(t, s, JobSpec{ID: "s2", User: "u2", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	s.Tick(t0.Add(17*time.Minute), nil)
	rpt = mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 1 || rpt.PreemptedJobs[0].PreemptedJobID != "long" {
		t.Fatalf("round2 want long preempted again, got %+v", rpt.PreemptedJobs)
	}
	long := s.GetJob("long")
	if long.Tasks[0].Runtime != 15*time.Minute {
		t.Fatalf("long accumulated runtime = %s, want 15m (10m + 5m segments)", long.Tasks[0].Runtime)
	}
	// Preempted 桶应恰好为 4cpu*15m = 60 cpu-minutes，且 Running 为 0（当前被抢）。
	b := long.Tasks[0].ResourceTime
	if b.Preempted.CPU != 4*15*time.Minute {
		t.Fatalf("preempted cpu-time = %s, want 60m", b.Preempted.CPU)
	}
	if b.Running.CPU != 0 || b.Total.CPU != 4*15*time.Minute {
		t.Fatalf("want running=0 total=60m, got run=%s total=%s", b.Running.CPU, b.Total.CPU)
	}
	t.Logf("JUDGE repeated same-prio preemption: long runtime=15m in two segments; preempted cpu-time=%s",
		b.Preempted.CPU)
}

// 权重归一化：高权重用户同等消耗下归一化份额更低，不先被让；
// 低权重长跑用户归一化消耗最高，最先成为受害者。
func TestSamePriorityPreemptionWeightNormalized(t *testing.T) {
	s := newTestScheduler(t, nil, &QuotaConfig{
		UserWeight: map[string]float64{"u-heavy": 2.0, "u-light": 0.5},
	})
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 12, Memory: 12})
	mustSubmit(t, s, JobSpec{ID: "heavy", User: "u-heavy", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}}) // 权重 2
	mustSubmit(t, s, JobSpec{ID: "light", User: "u-light", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}}) // 权重 0.5
	mustSubmit(t, s, JobSpec{ID: "plain", User: "u-plain", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}}) // 权重 1
	mustSchedule(t, s)
	s.Tick(t0.Add(10*time.Minute), nil)
	t.Logf("INPUT all ran 10m; normalized consumed: light=80m > plain=40m > heavy=20m")

	mustSubmit(t, s, JobSpec{ID: "new", User: "u-new", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	rpt := mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 1 || rpt.PreemptedJobs[0].PreemptedJobID != "light" {
		t.Fatalf("want light (lowest weight -> highest normalized share) preempted first, got %+v",
			rpt.PreemptedJobs)
	}
	t.Logf("JUDGE weight-normalized victim selection picked %s", rpt.PreemptedJobs[0].PreemptedJobID)
}

// 反饥饿：作业被抢后带着已结算消耗重新排队；容量恢复后它能真正重新进入
// 运行（运行时段继续累加），不会因为“剩余份额少/被抢过”而永远停在
// Preempted，也不会与等待者形成死锁。
func TestPreemptedJobNotStarvedByNewcomers(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	// 初始只有 n1（4cpu），long 被 s1 抢占；随后 n2 上线提供恢复容量。
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 8})

	mustSubmit(t, s, JobSpec{ID: "long", User: "u-long", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	mustSchedule(t, s)
	s.Tick(t0.Add(10*time.Minute), nil)

	// s1 同优先级抢占 long（消耗 40 cpu-min vs 0）。
	mustSubmit(t, s, JobSpec{ID: "s1", User: "u1", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	rpt := mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 1 || rpt.PreemptedJobs[0].PreemptedJobID != "long" {
		t.Fatalf("want long preempted by s1, got %+v", rpt.PreemptedJobs)
	}

	// 零消耗新作业 s2 在 s1 运行期间到达并排队。
	mustSubmit(t, s, JobSpec{ID: "s2", User: "u2", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	mustSchedule(t, s)

	// n2 上线、s1 完成：容量恢复。long 提交更早，先恢复运行（不是被抢过就永久靠后）。
	mustAddNode(t, s, NodeSpec{Name: "n2", CPU: 4, Memory: 8})
	s.Tick(t0.Add(12*time.Minute), []string{"s1/t"})
	mustSchedule(t, s)
	long := s.GetJob("long")
	if long.Tasks[0].State != TaskRunning {
		t.Fatalf("preempted long must actually resume running once capacity returns, got %s",
			long.Tasks[0].State)
	}
	if long.Tasks[0].Runtime != 10*time.Minute {
		t.Fatalf("resumed long keeps prior settled runtime 10m, got %s", long.Tasks[0].Runtime)
	}
	t.Logf("JUDGE preempted long resumed running (node=%s) with accumulated runtime=%s",
		long.Tasks[0].Node, long.Tasks[0].Runtime)

	// long 恢复后又跑 3m 完成：被抢段 40 cpu-min + 恢复段 12 cpu-min，分桶不重不漏。
	s.Tick(t0.Add(15*time.Minute), []string{"long/t"})
	v := s.ResourceTimes()
	b := v.Jobs["long"].Breakdown
	if b.Preempted.CPU != 4*10*time.Minute || b.Finished.CPU != 4*3*time.Minute {
		t.Fatalf("long resource-time preempted=%s finished=%s, want 40m/12m",
			b.Preempted.CPU, b.Finished.CPU)
	}
	t.Logf("JUDGE long across preempt+restart: preempted=%s finished=%s total=%s",
		b.Preempted.CPU, b.Finished.CPU, b.Total.CPU)
}
