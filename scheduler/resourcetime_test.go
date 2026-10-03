package scheduler

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func rtNanos(cpu, mem, gpu int64, d time.Duration) ResourceTime {
	return ResourceTime{CPUNanos: cpu * int64(d), MemoryNanos: mem * int64(d), GPUNanos: gpu * int64(d)}
}

func sumFive(b ResourceTimeBuckets) ResourceTime {
	return b.Running.add(b.Finished).add(b.Canceled).add(b.Failed).add(b.Preempted)
}

// 守恒恒等式：五类（运行/完成/取消/失败/抢占）之和等于总量，逐维度成立。
func assertBucketsConserved(t *testing.T, where string, b ResourceTimeBuckets) {
	t.Helper()
	got := sumFive(b)
	if got != b.Total {
		t.Fatalf("%s: five buckets != total: sum=%+v total=%+v", where, got, b.Total)
	}
}

// 运行中：Running 桶随时钟增长；完成后整段转入 Finished，Running 归零。
func TestResourceTimeRunningThenFinished(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 8})
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("a", 2, 4)}})
	mustSchedule(t, s)
	s.Tick(t0.Add(time.Minute), nil)
	b := s.ResourceTimes().Jobs["j"].Buckets
	if b.Running != rtNanos(2, 4, 0, time.Minute) || b.Total != b.Running {
		t.Fatalf("running 1m want live 2cpu/4mem, got %+v", b)
	}
	t.Logf("JUDGE running 1m: running=%+v total=%+v (five=total)", b.Running, b.Total)

	s.Tick(t0.Add(3*time.Minute), []string{"j/a"})
	b = s.ResourceTimes().Jobs["j"].Buckets
	want := rtNanos(2, 4, 0, 3*time.Minute)
	if b.Finished != want || b.Running.CPUNanos != 0 || b.Total != want {
		t.Fatalf("finished want settled 3m in Finished, got %+v", b)
	}
	t.Logf("JUDGE finished at 3m: finished=%+v running=0 total=%+v", b.Finished, b.Total)
}

// 抢占 -> 重启 -> 完成：被抢段进 Preempted，重启后的段进 Finished，
// 两段之和等于总量，无重复无漏记。
func TestResourceTimePreemptRestartFinishConservation(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "vic", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("v", 4, 1)}})
	mustSchedule(t, s)
	mustSubmit(t, s, JobSpec{ID: "h1", User: "u2", Queue: "q1", Priority: 100,
		Tasks: []TaskSpec{mkTask("h", 4, 1)}})
	s.Tick(t0.Add(time.Minute), nil)
	mustSchedule(t, s) // 1m 段被抢

	s.Tick(t0.Add(2*time.Minute), []string{"h1/h"})
	mustSchedule(t, s) // vic 重启
	s.Tick(t0.Add(5*time.Minute), []string{"vic/v"})

	rpt := s.ResourceTimes()
	b := rpt.Jobs["vic"].Buckets
	assertBucketsConserved(t, "job vic", b)
	if b.Preempted != rtNanos(4, 1, 0, time.Minute) {
		t.Fatalf("preempted bucket want 4cpu*1m, got %+v", b.Preempted)
	}
	// 2m 重启、5m 完成：完成段 3m。
	if b.Finished != rtNanos(4, 1, 0, 3*time.Minute) {
		t.Fatalf("finished bucket want 4cpu*3m, got %+v", b.Finished)
	}
	if b.Total != rtNanos(4, 1, 0, 4*time.Minute) {
		t.Fatalf("total want 4cpu*4m, got %+v", b.Total)
	}
	// 用户/队列维度同样守恒且数值一致。
	assertBucketsConserved(t, "user u1", rpt.Users["u1"].Buckets)
	assertBucketsConserved(t, "queue q1", rpt.Queues["q1"].Buckets)
	t.Logf("JUDGE preempted=1m finished=3m total=4m; five=total on job/user/queue dimensions")
}

// 取消：运行中任务的段进 Canceled；被抢占持有预留的任务此前的被抢段仍在
// Preempted，取消不再追加运行时间；五桶之和守恒。
func TestResourceTimeCancelConservation(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 8, Memory: 8})
	mustSubmit(t, s, JobSpec{ID: "low", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("a", 4, 1)}})
	mustSchedule(t, s)
	mustSubmit(t, s, JobSpec{ID: "hi", User: "u2", Queue: "q1", Priority: 100,
		Tasks: []TaskSpec{mkTask("h", 8, 1)}})
	s.Tick(t0.Add(time.Minute), nil)
	mustSchedule(t, s) // low 在 1m 处被抢占
	if err := s.CancelJob("low"); err != nil {
		t.Fatal(err)
	}
	b := s.ResourceTimes().Jobs["low"].Buckets
	assertBucketsConserved(t, "canceled low", b)
	if b.Preempted != rtNanos(4, 1, 0, time.Minute) {
		t.Fatalf("preempted segment must remain, got %+v", b.Preempted)
	}
	if b.Canceled.CPUNanos != 0 {
		t.Fatalf("cancel after preempt adds no running time, got %+v", b.Canceled)
	}
	t.Logf("JUDGE canceled preempted task: preempted=1m preserved, canceled=0, total=preempted")

	// 另一个作业：hi 结束后 run 真正跑起来，再直接取消，段进 Canceled。
	s.Tick(t0.Add(2*time.Minute), []string{"hi/h"})
	mustSubmit(t, s, JobSpec{ID: "run", User: "u3", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("r", 4, 1)}})
	mustSchedule(t, s)
	if st := s.GetJob("run").Tasks[0].State; st != TaskRunning {
		t.Fatalf("run must be running before cancel, got %s", st)
	}
	s.Tick(t0.Add(4*time.Minute), nil)
	if err := s.CancelJob("run"); err != nil {
		t.Fatal(err)
	}
	b = s.ResourceTimes().Jobs["run"].Buckets
	assertBucketsConserved(t, "canceled run", b)
	if b.Canceled != rtNanos(4, 1, 0, 2*time.Minute) {
		t.Fatalf("canceled want 4cpu*2m, got %+v", b.Canceled)
	}
	t.Logf("JUDGE running task canceled: canceled=2m in Canceled bucket; five=total")
}

// 失败重试：节点失联段进 Failed；重试重启后正常完成段进 Finished；
// 重试用尽时最后一段也进 Failed；全程守恒。
func TestResourceTimeFailureRetryConservation(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustAddNode(t, s, NodeSpec{Name: "n2", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1, MaxRetries: 1,
		Tasks: []TaskSpec{mkTask("a", 4, 1)}})
	mustSchedule(t, s)
	s.Tick(t0.Add(time.Minute), nil)
	if err := s.SetNodeState("n1", NodeLost); err != nil { // 1m 失败段
		t.Fatal(err)
	}
	mustSchedule(t, s)                             // 在 n2 重启
	s.Tick(t0.Add(3*time.Minute), []string{"j/a"}) // 1m..3m 运行 2m 后正常完成
	b := s.ResourceTimes().Jobs["j"].Buckets
	assertBucketsConserved(t, "retry-then-finish", b)
	if b.Failed != rtNanos(4, 1, 0, time.Minute) {
		t.Fatalf("failed bucket want 4cpu*1m (lost segment), got %+v", b.Failed)
	}
	if b.Finished != rtNanos(4, 1, 0, 2*time.Minute) {
		t.Fatalf("finished bucket want 4cpu*2m, got %+v", b.Finished)
	}
	if b.Total != rtNanos(4, 1, 0, 3*time.Minute) {
		t.Fatalf("total want 4cpu*3m, got %+v", b.Total)
	}
	t.Logf("JUDGE lost seg -> Failed=1m, retry run -> Finished=2m, total=3m; five=total")

	// 重试用尽场景。
	s2 := newTestScheduler(t, nil, nil)
	mustAddNode(t, s2, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustAddNode(t, s2, NodeSpec{Name: "n2", CPU: 4, Memory: 4})
	mustSubmit(t, s2, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1, MaxRetries: 1,
		Tasks: []TaskSpec{mkTask("a", 4, 1)}})
	mustSchedule(t, s2)
	s2.Tick(t0.Add(time.Minute), nil)
	_ = s2.SetNodeState("n1", NodeLost)
	mustSchedule(t, s2)
	s2.Tick(t0.Add(2*time.Minute), nil)
	if err := s2.SetNodeState("n2", NodeLost); err != nil { // 用尽：第二段也 Failed
		t.Fatal(err)
	}
	b2 := s2.ResourceTimes().Jobs["j"].Buckets
	assertBucketsConserved(t, "retries-exhausted", b2)
	if b2.Failed != rtNanos(4, 1, 0, 2*time.Minute) || b2.Finished.CPUNanos != 0 {
		t.Fatalf("want both segments (1m+1m) in Failed, got %+v", b2)
	}
	t.Logf("JUDGE retries exhausted: failed=2m total=2m; five=total")
}

// 跨阶段链式场景：被抢占 -> 重启 -> 运行中被取消。被抢段留在 Preempted，
// 重启后的第二段在取消时进 Canceled，两段相加等于总量，不重不漏；
// 同时校验份额回补（allocated 归零）与五桶恒等式。
func TestResourceTimePreemptRestartCancelChain(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "vic", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("v", 4, 1)}})
	mustSchedule(t, s)
	t.Logf("INPUT vic 4cpu: run 1m -> preempted by hi -> restart at 2m -> canceled at 5m")

	// 第 1 段：跑 1 分钟后被高优先级 hi 抢占。
	mustSubmit(t, s, JobSpec{ID: "hi", User: "u2", Queue: "q1", Priority: 100,
		Tasks: []TaskSpec{mkTask("h", 4, 1)}})
	s.Tick(t0.Add(time.Minute), nil)
	mustSchedule(t, s)
	// hi 结束，vic 重启；再跑 3 分钟后被取消。
	s.Tick(t0.Add(2*time.Minute), []string{"hi/h"})
	mustSchedule(t, s)
	if st := s.GetJob("vic").Tasks[0].State; st != TaskRunning {
		t.Fatalf("vic must restart, got %s", st)
	}
	s.Tick(t0.Add(5*time.Minute), nil)
	if err := s.CancelJob("vic"); err != nil {
		t.Fatal(err)
	}

	b := s.ResourceTimes().Jobs["vic"].Buckets
	assertBucketsConserved(t, "preempt-restart-cancel vic", b)
	if b.Preempted != rtNanos(4, 1, 0, time.Minute) {
		t.Fatalf("preempted bucket want 4cpu*1m, got %+v", b.Preempted)
	}
	if b.Canceled != rtNanos(4, 1, 0, 3*time.Minute) {
		t.Fatalf("canceled bucket want 4cpu*3m (restarted segment), got %+v", b.Canceled)
	}
	if b.Total != rtNanos(4, 1, 0, 4*time.Minute) {
		t.Fatalf("total want 4cpu*4m (1m preempted + 3m canceled), got %+v", b.Total)
	}
	if b.Running.CPUNanos != 0 || b.Finished.CPUNanos != 0 || b.Failed.CPUNanos != 0 {
		t.Fatalf("no other bucket may hold time, got %+v", b)
	}
	if rt := s.GetJob("vic").Tasks[0].Runtime; rt != 4*time.Minute {
		t.Fatalf("task runtime want 4m across two segments, got %s", rt)
	}
	// 份额回补：取消后运行份额与预留份额全部释放。
	if snap := s.Snapshot(); snap.Running.CPU != 0 || snap.Allocated.CPU != 0 {
		t.Fatalf("after cancel want running=allocated=0, got run=%d alloc=%d",
			snap.Running.CPU, snap.Allocated.CPU)
	}
	t.Logf("JUDGE chain preempt(1m)->restart->cancel(3m): preempted+canceled=total=4m; shares fully returned")
}

// 多作业混合终局：运行中、正常结束、被取消、失败、被抢占五类同时在场时，
// 用户/队列/作业三个维度各自的五桶之和都等于总量，且与快照读数一致。
func TestResourceTimeMixedTerminalStatesConservation(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 8, Memory: 8})
	mustAddNode(t, s, NodeSpec{Name: "n2", CPU: 8, Memory: 8})

	// vic 先单独跑 1 分钟，使其已消耗份额大于稍后同优先级的 run，
	// 保证后面 hi 抢占时受害者确定是 vic。
	mustSubmit(t, s, JobSpec{ID: "vic", User: "u3", Queue: "q2", Priority: 1,
		Tasks: []TaskSpec{mkTask("t", 2, 1)}})
	mustSchedule(t, s)
	s.Tick(t0.Add(time.Minute), nil)

	mustSubmit(t, s, JobSpec{ID: "fin", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("t", 2, 1)}})
	mustSubmit(t, s, JobSpec{ID: "run", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("t", 2, 1)}})
	mustSubmit(t, s, JobSpec{ID: "can", User: "u2", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("t", 2, 1)}})
	// lose 8cpu 独占 n2（n1 已有 vic2+fin2+run2+can2=8，放不下）。
	mustSubmit(t, s, JobSpec{ID: "lose", User: "u2", Queue: "q2", Priority: 1,
		Tasks: []TaskSpec{mkTask("t", 8, 1)}})
	mustSchedule(t, s)
	if got := s.GetJob("lose").Tasks[0].Node; got != "n2" {
		t.Fatalf("setup: lose must be alone on n2, got %s", got)
	}
	t.Logf("INPUT n1: vic(2,started t0)+fin+run+can(2 each,started t0+1m); n2: lose(8); " +
		"at t0+3m: fin completes, can canceled, n2 lost, hi(6cpu,prio100) preempts vic")

	// 第二段跑 2 分钟到 t0+3m：fin 完成、can 取消、n2 失联（lose 无重试额度）、
	// hi 需要 6cpu，只能抢 vic（n1 上 run2+vic2=4 占用，free 4 < 6）。
	s.Tick(t0.Add(3*time.Minute), []string{"fin/t"})
	if err := s.CancelJob("can"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetNodeState("n2", NodeLost); err != nil {
		t.Fatal(err)
	}
	mustSubmit(t, s, JobSpec{ID: "hi", User: "u9", Queue: "q2", Priority: 100,
		Tasks: []TaskSpec{mkTask("h", 6, 1)}})
	mustSchedule(t, s)
	if st := s.GetJob("vic").Tasks[0].State; st != TaskPreempted {
		t.Fatalf("vic must be preempted by hi, got %s", st)
	}
	if st := s.GetJob("run").Tasks[0].State; st != TaskRunning {
		t.Fatalf("run must keep running, got %s", st)
	}
	s.Tick(t0.Add(5*time.Minute), nil) // run 累计跑 4m；hi 跑 2m

	rpt := s.ResourceTimes()
	rt := func(cpu int64, d time.Duration) ResourceTime { return rtNanos(cpu, 1, 0, d) }
	checks := []struct {
		job    string
		bucket func(b ResourceTimeBuckets) ResourceTime
		want   ResourceTime
		name   string
	}{
		{"fin", func(b ResourceTimeBuckets) ResourceTime { return b.Finished }, rt(2, 2*time.Minute), "finished"},
		{"can", func(b ResourceTimeBuckets) ResourceTime { return b.Canceled }, rt(2, 2*time.Minute), "canceled"},
		{"lose", func(b ResourceTimeBuckets) ResourceTime { return b.Failed }, rt(8, 2*time.Minute), "failed"},
		{"vic", func(b ResourceTimeBuckets) ResourceTime { return b.Preempted }, rt(2, 3*time.Minute), "preempted"},
		{"run", func(b ResourceTimeBuckets) ResourceTime { return b.Running }, rt(2, 4*time.Minute), "running"},
		{"hi", func(b ResourceTimeBuckets) ResourceTime { return b.Running }, rt(6, 2*time.Minute), "running"},
	}
	for _, c := range checks {
		b := rpt.Jobs[c.job].Buckets
		assertBucketsConserved(t, "mixed job "+c.job, b)
		if got := c.bucket(b); got != c.want {
			t.Fatalf("job %s %s bucket want %+v, got %+v (all=%+v)", c.job, c.name, c.want, got, b)
		}
		if b.Total != c.want {
			t.Fatalf("job %s total want %+v, got %+v", c.job, c.want, b.Total)
		}
	}
	// 每个维度都满足五桶=总量。
	for id, v := range rpt.Jobs {
		assertBucketsConserved(t, "job "+id, v.Buckets)
	}
	for u, v := range rpt.Users {
		assertBucketsConserved(t, "user "+u, v.Buckets)
	}
	for q, v := range rpt.Queues {
		assertBucketsConserved(t, "queue "+q, v.Buckets)
	}
	// 用户维度聚合 = 各作业之和（u1 = fin 2m + run 4m）。
	if got, want := rpt.Users["u1"].Buckets.Total, rt(2, 2*time.Minute).add(rt(2, 4*time.Minute)); got != want {
		t.Fatalf("user u1 total want fin+run %+v, got %+v", want, got)
	}
	t.Logf("JUDGE five terminal classes coexist; five=total on job/user/queue; user aggregation exact")
}

// 并发：资源时间查询与调度/抢占/取消/失联并发时，每次读到的视图都自洽，
// 且最终五桶之和等于总量。
func TestResourceTimeConcurrentConsistentReads(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	for i := 0; i < 3; i++ {
		mustAddNode(t, s, NodeSpec{Name: fmt.Sprintf("n%d", i), CPU: 16, Memory: 32})
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	// 写侧：不断提交/调度/完成。
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for k := 0; k < 40; k++ {
				id := fmt.Sprintf("j-%d-%d", w, k)
				if err := s.Submit(JobSpec{ID: id, User: fmt.Sprintf("u%d", w%2), Queue: "q1",
					Priority: (k % 5), Tasks: []TaskSpec{mkTask("a", 2, 2)}}); err == nil {
					_, _ = s.Schedule()
				}
				if k%4 == 0 {
					s.Tick(t0.Add(time.Duration(k)*time.Second), []string{fmt.Sprintf("%s/a", id)})
				}
			}
		}(w)
	}
	// 节点抖动制造失败/重试段。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for k := 0; k < 20; k++ {
			_ = s.SetNodeState("n1", NodeLost)
			_ = s.SetNodeState("n1", NodeReady)
		}
	}()
	// 读侧：每次报表内每个视图都必须满足五桶=总量。不计入 wg，
	// 由 stop 通道在写侧结束后通知退出。
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stop:
				return
			default:
				rpt := s.ResourceTimes()
				for _, v := range rpt.Jobs {
					if sumFive(v.Buckets) != v.Buckets.Total {
						panic(fmt.Sprintf("inconsistent job %s view: %+v", v.JobID, v.Buckets))
					}
				}
				time.Sleep(time.Microsecond) // 退让，避免忙轮询饿死写侧
			}
		}
	}()
	wg.Wait()
	close(stop)
	<-readerDone

	rpt := s.ResourceTimes()
	for _, v := range rpt.Jobs {
		assertBucketsConserved(t, "final job "+v.JobID, v.Buckets)
	}
	for _, v := range rpt.Users {
		assertBucketsConserved(t, "final user "+v.User, v.Buckets)
	}
	t.Logf("JUDGE concurrent reads always self-consistent; final users=%d jobs=%d conserved",
		len(rpt.Users), len(rpt.Jobs))
}
