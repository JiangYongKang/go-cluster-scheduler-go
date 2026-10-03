package scheduler

import (
	"testing"
	"time"
)

// 同优先级抢占（用户主场景）：4 核节点上，长跑作业占满全部资源并运行 10 分钟，
// 之后提交的同优先级新作业归一化已消耗份额为 0，必须把长跑者顶下来启动；
// 被抢者结算 10 分钟运行时并持有等量预留，allocated 总量守恒。
func TestSamePriorityPreemptLongRunner(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})

	mustSubmit(t, s, JobSpec{ID: "old", User: "u1", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	mustSchedule(t, s)

	// old 独占跑 10 分钟。
	s.Tick(t0.Add(10*time.Minute), nil)
	mustSubmit(t, s, JobSpec{ID: "new", User: "u2", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	cOld := s.consumedShare(s.jobs["old"], s.now)
	cNew := s.consumedShare(s.jobs["new"], s.now)
	t.Logf("INPUT old runs 10m on 4cpu; new same-priority arrives now; consumed old=%.0f new=%.0f", cOld, cNew)
	if !(cOld > cNew) {
		t.Fatalf("old consumed share must exceed new, got old=%v new=%v", cOld, cNew)
	}

	rpt := mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 1 || rpt.PreemptedJobs[0].PreemptedJobID != "old" ||
		rpt.PreemptedJobs[0].PreemptingJobID != "new" {
		t.Fatalf("want old preempted by new, got %+v", rpt.PreemptedJobs)
	}
	if rt := s.GetJob("old").Tasks[0].Runtime; rt != 10*time.Minute {
		t.Fatalf("old settled runtime want 10m, got %s", rt)
	}
	if tk := s.GetJob("new").Tasks[0]; tk.State != TaskRunning || tk.Node != "n1" {
		t.Fatalf("new must be running on n1, got %+v", tk)
	}
	if tk := s.GetJob("old").Tasks[0]; tk.State != TaskPreempted {
		t.Fatalf("old must be preempted, got %s", tk.State)
	}
	// 守恒：运行份额 4（new）+ 预留 4（old）= 8，始终等于两个作业的总量。
	snap := s.Snapshot()
	if snap.Running.CPU != 4 || snap.Allocated.CPU != 8 {
		t.Fatalf("want running=4 allocated=8, got run=%d alloc=%d", snap.Running.CPU, snap.Allocated.CPU)
	}
	t.Logf("JUDGE same-priority long-runner yielded: old preempted (runtime=10m, share held), new running; run=%d alloc=%d",
		snap.Running.CPU, snap.Allocated.CPU)

	// new 结束后，old 在后续周期重新参与调度：不被历史消耗压着，能重新启动；
	// 预留转回运行，allocated 回落为 4；runtime 不重置（此后按段累加）。
	s.Tick(t0.Add(20*time.Minute), []string{"new/t"})
	rpt2 := mustSchedule(t, s)
	if len(rpt2.StartedJobs) != 1 || rpt2.StartedJobs[0] != "old" {
		t.Fatalf("old must restart after new completes, got started=%v", rpt2.StartedJobs)
	}
	if tk := s.GetJob("old").Tasks[0]; tk.State != TaskRunning {
		t.Fatalf("old must be running again, got %s", tk.State)
	}
	snap2 := s.Snapshot()
	if snap2.Running.CPU != 4 || snap2.Allocated.CPU != 4 {
		t.Fatalf("after restart want running=allocated=4, got run=%d alloc=%d",
			snap2.Running.CPU, snap2.Allocated.CPU)
	}
	t.Logf("JUDGE old restarted despite 10m historical consumption; hold converted, run=%d alloc=%d",
		snap2.Running.CPU, snap2.Allocated.CPU)
}

// 反向不得抢占：消耗更少的运行者不能被消耗更多的一方抢走。
// 构造：big 同优先级、带 10 分钟历史消耗（在 n1 上被高优先级作业抢占）；
// small 同优先级、只跑了 2 分钟，占着 n2。把 n1 置为 Drained 迫使 big
// 重排队时只能看 n2：big 归一化消耗更大，small 不是“严格更大”的一方，必须维持原状。
func TestSamePriorityHigherConsumerCannotPreemptLower(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustAddNode(t, s, NodeSpec{Name: "n2", CPU: 4, Memory: 4})

	// big 先提交到 n1 跑 10 分钟。
	mustSubmit(t, s, JobSpec{ID: "big", User: "u2", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("b", 4, 1)}})
	mustSchedule(t, s)
	if tk := s.GetJob("big").Tasks[0]; tk.Node != "n1" {
		t.Fatalf("setup: big should run on n1, got %s", tk.Node)
	}
	// 用 filler 占住 n2，使稍后提交的 urgent 无空闲节点、只能抢 n1 上的 big。
	// filler 优先级高于 urgent，urgent 抢不动它，只能去抢 n1 上的 big。
	mustSubmit(t, s, JobSpec{ID: "filler", User: "u9", Queue: "q1", Priority: 100,
		Tasks: []TaskSpec{mkTask("f", 4, 1)}})
	mustSchedule(t, s)
	if tk := s.GetJob("filler").Tasks[0]; tk.Node != "n2" {
		t.Fatalf("setup: filler should hold n2, got %s", tk.Node)
	}
	// urgent 在 10 分钟时抢占 big，big 被结算 10 分钟、持有预留。
	s.Tick(t0.Add(10*time.Minute), nil)
	mustSubmit(t, s, JobSpec{ID: "urgent", User: "u3", Queue: "q1", Priority: 50,
		Tasks: []TaskSpec{mkTask("u", 4, 1)}})
	rpt := mustSchedule(t, s)
	if rec := findRecord(rpt, "big"); rec == nil {
		t.Fatalf("setup: big should be preempted by urgent, got %+v", rpt.PreemptedJobs)
	}
	// urgent 结束释放 n1；small 此时在空闲的 n1/n2 中按节点名序落到 n1，
	// 为让 small 落在 n2，先把 n1 Drained，再提交 small，随后恢复 n2 独占视图。
	if err := s.SetNodeState("n1", NodeDrained); err != nil {
		t.Fatal(err)
	}
	// urgent 与 filler 同时结束，n2 腾出。
	s.Tick(t0.Add(11*time.Minute), []string{"urgent/u", "filler/f"})
	mustSubmit(t, s, JobSpec{ID: "small", User: "u1", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("s", 4, 1)}})
	mustSchedule(t, s) // 只有 n2 Ready，small 落 n2。
	if tk := s.GetJob("small").Tasks[0]; tk.Node != "n2" {
		t.Fatalf("setup: small should run on n2, got %s", tk.Node)
	}
	// 恢复 n1 为 Ready 但再立刻 Drain 掉：让 big 重排队时只能尝试 n2。
	// （n1 上有 big 的 10 分钟历史，恢复也不影响 small；这里直接保持 n1 不可用。）
	s.Tick(t0.Add(12*time.Minute), nil) // small 已跑约 1 分钟 live
	cBig := s.consumedShare(s.jobs["big"], s.now)
	cSmall := s.consumedShare(s.jobs["small"], s.now)
	t.Logf("INPUT big has 10m settled consumption and queues; small ran ~1m live; consumed big=%.0f small=%.0f",
		cBig, cSmall)
	if !(cBig > cSmall) {
		t.Fatalf("setup: big consumption must exceed small, got big=%v small=%v", cBig, cSmall)
	}

	// 若错误地允许“高消耗者抢低消耗者”，big 会顶掉 small；正确行为是等待。
	rpt2 := mustSchedule(t, s)
	if len(rpt2.PreemptedJobs) != 0 || len(rpt2.StartedJobs) != 0 {
		t.Fatalf("smaller-consumed runner must not be preempted by larger-consumed peer, got %+v", rpt2)
	}
	if rpt2.Unschedulable[0].Reason != ReasonNoYieldableShare {
		t.Fatalf("want NO_YIELDABLE_SHARE, got %s", rpt2.Unschedulable[0].Reason)
	}
	if tk := s.GetJob("small").Tasks[0]; tk.State != TaskRunning {
		t.Fatalf("small must keep running, got %s", tk.State)
	}
	if tk := s.GetJob("big").Tasks[0]; tk.State != TaskPreempted {
		t.Fatalf("big must stay preempted/waiting, got %s", tk.State)
	}
	t.Logf("JUDGE larger-consumed big cannot preempt smaller-consumed small; status quo kept: %s",
		rpt2.Unschedulable[0].Reason)

	// n1 恢复 Ready 后 big 回到 n1 重启（不被历史消耗压制），small 不受影响。
	if err := s.SetNodeState("n1", NodeReady); err != nil {
		t.Fatal(err)
	}
	rpt3 := mustSchedule(t, s)
	if len(rpt3.StartedJobs) != 1 || rpt3.StartedJobs[0] != "big" {
		t.Fatalf("big must restart on recovered n1, got started=%v", rpt3.StartedJobs)
	}
	if tk := s.GetJob("big").Tasks[0]; tk.Node != "n1" {
		t.Fatalf("big must restart on n1, got %s", tk.Node)
	}
	if tk := s.GetJob("small").Tasks[0]; tk.State != TaskRunning {
		t.Fatalf("small must keep running on n2, got %s", tk.State)
	}
	t.Logf("JUDGE big restarts on n1 once ready; historical consumption does not starve it; small untouched")
}

// 无可让出者时维持原状：同优先级的胜出者与运行者归一化已消耗份额严格相等
// （各跑 6 分钟，一个被抢占、一个节点失联待重试），没有任何一方“严格更大”，
// 给 NO_YIELDABLE_SHARE；节点恢复后等待者正常重启。
func TestSamePriorityEqualShareNoYield(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustAddNode(t, s, NodeSpec{Name: "n2", CPU: 4, Memory: 4})

	// a、b 同优先级同时启动，各占一个节点；b 允许失联重试一次。
	mustSubmit(t, s, JobSpec{ID: "a", User: "u1", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("a", 4, 1)}})
	mustSubmit(t, s, JobSpec{ID: "b", User: "u2", Queue: "q1", Priority: 5, MaxRetries: 1,
		Tasks: []TaskSpec{mkTask("b", 4, 1)}})
	mustSchedule(t, s)
	if tk := s.GetJob("a").Tasks[0]; tk.Node != "n1" || s.GetJob("b").Tasks[0].Node != "n2" {
		t.Fatalf("setup: a->n1 b->n2, got a=%s b=%s",
			s.GetJob("a").Tasks[0].Node, s.GetJob("b").Tasks[0].Node)
	}
	// 跑 6 分钟后 n2 失联：b 结算 6m（失败桶）、保留份额回到待重试。
	s.Tick(t0.Add(6*time.Minute), nil)
	if err := s.SetNodeState("n2", NodeLost); err != nil {
		t.Fatal(err)
	}
	// urgent 抢占 n1 上的 a：a 同样结算 6m（被抢占桶）、持有预留。
	mustSubmit(t, s, JobSpec{ID: "urgent", User: "u9", Queue: "q1", Priority: 50,
		Tasks: []TaskSpec{mkTask("u", 4, 1)}})
	rpt := mustSchedule(t, s)
	if rec := findRecord(rpt, "a"); rec == nil {
		t.Fatalf("setup: a should be preempted by urgent, got %+v", rpt.PreemptedJobs)
	}
	// urgent 结束，n2 仍失联：a 先回到 n1（提交序在前），b 只能尝试 n1 上的 a。
	s.Tick(t0.Add(7*time.Minute), []string{"urgent/u"})
	mustSchedule(t, s)
	if tk := s.GetJob("a").Tasks[0]; tk.State != TaskRunning {
		t.Fatalf("setup: a should be running again, got %s", tk.State)
	}
	if tk := s.GetJob("b").Tasks[0]; tk.State != TaskPending {
		t.Fatalf("setup: b should be pending retry, got %s", tk.State)
	}
	ca := s.consumedShare(s.jobs["a"], s.now)
	cb := s.consumedShare(s.jobs["b"], s.now)
	t.Logf("INPUT a settled 6m (preempted) running again; b settled 6m (node lost) queued; consumed a=%.0f b=%.0f",
		ca, cb)
	if ca != cb {
		t.Fatalf("setup: a,b consumed shares must be equal, got %v vs %v", ca, cb)
	}
	rpt2 := mustSchedule(t, s)
	if len(rpt2.PreemptedJobs) != 0 || len(rpt2.StartedJobs) != 0 {
		t.Fatalf("equal-share peers must not yield, got preempt=%v started=%v",
			rpt2.PreemptedJobs, rpt2.StartedJobs)
	}
	if rpt2.Unschedulable[0].Reason != ReasonNoYieldableShare {
		t.Fatalf("want NO_YIELDABLE_SHARE, got %s", rpt2.Unschedulable[0].Reason)
	}
	if tk := s.GetJob("a").Tasks[0]; tk.State != TaskRunning {
		t.Fatalf("a must keep running, got %s", tk.State)
	}
	t.Logf("JUDGE equal-consumed same-priority peers: no yield (%s); a keeps running, b keeps waiting",
		rpt2.Unschedulable[0].Reason)
	// n2 恢复：b 立即重启，a 不受影响；双方历史都不构成压制。
	if err := s.SetNodeState("n2", NodeReady); err != nil {
		t.Fatal(err)
	}
	rpt3 := mustSchedule(t, s)
	if len(rpt3.StartedJobs) != 1 || rpt3.StartedJobs[0] != "b" {
		t.Fatalf("b must restart on recovered n2, got started=%v", rpt3.StartedJobs)
	}
	if tk := s.GetJob("b").Tasks[0]; tk.Node != "n2" {
		t.Fatalf("b must restart on n2, got %s", tk.Node)
	}
	if tk := s.GetJob("a").Tasks[0]; tk.State != TaskRunning {
		t.Fatalf("a must keep running on n1, got %s", tk.State)
	}
	t.Logf("JUDGE b restarts on recovered n2; a untouched; equal history starves neither")
}

// 权重归一化影响同优先级让出方向：高权重（4）用户的长跑作业归一化份额更小，
// 低权重（1）、零消耗的新作业仍可顶掉它（10 分钟原始消耗 / 权重 4 = 10 标量 > 0）。
func TestSamePriorityWeightNormalizedYield(t *testing.T) {
	q := &QuotaConfig{
		UserWeights:  map[string]float64{"heavy": 4, "light": 1},
		QueueWeights: map[string]float64{"q1": 1},
	}
	s := newTestScheduler(t, nil, q)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})

	mustSubmit(t, s, JobSpec{ID: "heavyJob", User: "heavy", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	mustSchedule(t, s)
	s.Tick(t0.Add(10*time.Minute), nil)
	mustSubmit(t, s, JobSpec{ID: "lightNew", User: "light", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	cH := s.consumedShare(s.jobs["heavyJob"], s.now)
	cL := s.consumedShare(s.jobs["lightNew"], s.now)
	t.Logf("INPUT heavy(weight4) ran 10m; light(weight1) new job; normalized consumed heavy=%.1f light=%.1f", cH, cL)
	rpt := mustSchedule(t, s)
	if len(rpt.PreemptedJobs) != 1 || rpt.PreemptedJobs[0].PreemptedJobID != "heavyJob" {
		t.Fatalf("heavyJob must yield to smaller normalized-share newcomer, got %+v", rpt.PreemptedJobs)
	}
	t.Logf("JUDGE weight-normalized: heavyJob yielded (normalized %.1f > %.1f)", cH, cL)
}

// 权重反向：高权重让两个长跑者归一化份额变小，低权重的新来者份额并不更小时
// 不得抢占，维持 NO_YIELDABLE_SHARE。
func TestSamePriorityWeightNoYieldWhenNormalizedSmaller(t *testing.T) {
	q := &QuotaConfig{
		UserWeights:  map[string]float64{"heavy": 100, "light": 1},
		QueueWeights: map[string]float64{"q1": 1},
	}
	s := newTestScheduler(t, nil, q)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "heavyJob", User: "heavy", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("t", 4, 1)}})
	mustSchedule(t, s)
	// heavy 跑 1 分钟：归一化 = 4*1m/100 极小。
	s.Tick(t0.Add(time.Minute), nil)
	// light 的新作业虽然历史为 0，但它若要抢 heavy，需要 heavy 归一化份额
	// 严格大于自己（0）。极小值仍 > 0，因此本例仍会让出；
	// 为构造“不让”，让 light 作业自己也带历史：先在别处跑 10 分钟被抢占。
	mustAddNode(t, s, NodeSpec{Name: "n2", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "lightJob", User: "light", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("l", 4, 1)}})
	mustSchedule(t, s)
	s.Tick(t0.Add(11*time.Minute), nil)
	// urgent 在 n2 抢占 lightJob，使其带 10 分钟历史进入被抢占态。
	mustSubmit(t, s, JobSpec{ID: "urgent", User: "u3", Queue: "q1", Priority: 50,
		Tasks: []TaskSpec{mkTask("u", 4, 1)}})
	rpt := mustSchedule(t, s)
	if rec := findRecord(rpt, "lightJob"); rec == nil {
		t.Fatalf("setup: lightJob should be preempted, got %+v", rpt.PreemptedJobs)
	}
	s.Tick(t0.Add(12*time.Minute), []string{"urgent/u"})
	if err := s.SetNodeState("n2", NodeDrained); err != nil {
		t.Fatal(err)
	}
	cH := s.consumedShare(s.jobs["heavyJob"], s.now)
	cL := s.consumedShare(s.jobs["lightJob"], s.now)
	t.Logf("INPUT heavy(weight100) live ~12m normalized=%.2f; light(weight1) has 10m history normalized=%.2f",
		cH, cL)
	if !(cL > cH) {
		t.Fatalf("setup: light normalized consumption must exceed heavy, got light=%v heavy=%v", cL, cH)
	}
	rpt2 := mustSchedule(t, s)
	if len(rpt2.PreemptedJobs) != 0 || len(rpt2.StartedJobs) != 0 {
		t.Fatalf("smaller normalized-share heavyJob must not yield to lightJob, got %+v", rpt2)
	}
	if rpt2.Unschedulable[0].Reason != ReasonNoYieldableShare {
		t.Fatalf("want NO_YIELDABLE_SHARE, got %s", rpt2.Unschedulable[0].Reason)
	}
	if tk := s.GetJob("heavyJob").Tasks[0]; tk.State != TaskRunning {
		t.Fatalf("heavyJob must keep running, got %s", tk.State)
	}
	t.Logf("JUDGE weight-normalized: heavyJob's share (%.2f) < lightJob's (%.2f), no yield: %s",
		cH, cL, rpt2.Unschedulable[0].Reason)
}

func findRecord(rpt *ScheduleReport, jobID string) *PreemptionRecord {
	for i := range rpt.PreemptedJobs {
		if rpt.PreemptedJobs[i].PreemptedJobID == jobID {
			return &rpt.PreemptedJobs[i]
		}
	}
	return nil
}
