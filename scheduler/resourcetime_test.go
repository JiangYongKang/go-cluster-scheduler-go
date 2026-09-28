package scheduler

import (
	"sync"
	"testing"
	"time"
)

func rtEqual(t *testing.T, got, want ResourceTime, label string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %+v, want %+v", label, got, want)
	}
}

// 验证五桶之和恒等于总量：完成、被抢占、取消、失败四类终态各走一遍。
func TestResourceTimeFiveBucketsConservation(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 16})

	// jfin：正常完成，跑 5m。
	mustSubmit(t, s, JobSpec{ID: "jfin", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("t", 1, 1)}})
	// jpre：先跑 3m 被高优先级抢占，之后重启再跑 2m 完成。
	mustSubmit(t, s, JobSpec{ID: "jpre", User: "u2", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("t", 2, 1)}})
	// jcan：跑 4m 被取消。
	mustSubmit(t, s, JobSpec{ID: "jcan", User: "u3", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("t", 1, 1)}})
	// jfail：节点失联、重试 0 次 -> 失败，跑 6m。
	mustAddNode(t, s, NodeSpec{Name: "n2", CPU: 1, Memory: 8})
	mustSubmit(t, s, JobSpec{ID: "jfail", User: "u4", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("t", 1, 1)}})
	mustSchedule(t, s)
	t.Logf("INPUT four jobs start together at t0 (jfin/jpre/jcan on n1, jfail on n2)")

	s.Tick(t0.Add(3*time.Minute), nil)
	// 高优先级作业抢占 jpre（3m 段进 Preempted 桶）。
	mustSubmit(t, s, JobSpec{ID: "hp", User: "u5", Queue: "q1", Priority: 100,
		Tasks: []TaskSpec{mkTask("t", 2, 1)}})
	mustSchedule(t, s)
	t.Logf("JUDGE jpre preempted after 3m; its 3m segment billed to Preempted")

	s.Tick(t0.Add(4*time.Minute), []string{})
	if err := s.CancelJob("jcan"); err != nil { // 4m 段进 Canceled 桶
		t.Fatalf("cancel: %v", err)
	}
	t.Logf("JUDGE jcan canceled after 4m; billed to Canceled")

	s.Tick(t0.Add(5*time.Minute), []string{"jfin/t"}) // jfin 完成（5m -> Finished）
	t.Logf("JUDGE jfin finished after 5m; billed to Finished")

	s.Tick(t0.Add(6*time.Minute), nil)
	if err := s.SetNodeState("n2", NodeLost); err != nil { // jfail 失联失败（6m -> Failed）
		t.Fatalf("node lost: %v", err)
	}
	t.Logf("JUDGE jfail failed via node loss after 6m; billed to Failed")

	// hp 完成，jpre 重启。
	s.Tick(t0.Add(8*time.Minute), []string{"hp/t"})
	mustSchedule(t, s)
	t.Logf("JUDGE jpre restarts at 8m (3m already settled, new segment starts)")
	s.Tick(t0.Add(10*time.Minute), []string{"jpre/t"}) // jpre 再跑 2m 完成

	v := s.ResourceTimes()
	m := time.Minute
	want := ResourceTimeBreakdown{
		Running:   ResourceTime{},
		Finished:  ResourceTime{CPU: 5*m + 4*m + 10*m, Memory: 5*m + 2*m + 5*m},
		Canceled:  ResourceTime{CPU: 4 * m, Memory: 4 * m},
		Failed:    ResourceTime{CPU: 6 * m, Memory: 6 * m},
		Preempted: ResourceTime{CPU: 2 * 3 * m, Memory: 3 * m},
	}
	want.Total = want.Running.add(want.Finished).add(want.Canceled).add(want.Failed).add(want.Preempted)

	rtEqual(t, v.Jobs["jfin"].Breakdown.Finished, ResourceTime{CPU: 5 * m, Memory: 5 * m}, "jfin finished")
	rtEqual(t, v.Jobs["jpre"].Breakdown.Preempted, ResourceTime{CPU: 2 * 3 * m, Memory: 3 * m}, "jpre preempted")
	rtEqual(t, v.Jobs["jpre"].Breakdown.Finished, ResourceTime{CPU: 2 * 2 * m, Memory: 2 * m}, "jpre final segment")
	rtEqual(t, v.Jobs["jcan"].Breakdown.Canceled, ResourceTime{CPU: 4 * m, Memory: 4 * m}, "jcan canceled")
	rtEqual(t, v.Jobs["jfail"].Breakdown.Failed, ResourceTime{CPU: 6 * m, Memory: 6 * m}, "jfail failed")
	rtEqual(t, v.Total.Total, want.Total, "total five-bucket sum")
	t.Logf("JUDGE five buckets: fin=%s can=%s fail=%s pre=%s total=%s",
		want.Finished.CPU, want.Canceled.CPU, want.Failed.CPU, want.Preempted.CPU, want.Total.CPU)

	// 用户/队列维度的五桶之和也必须等于总量。
	var sumUsers, sumQueues ResourceTimeBreakdown
	for _, b := range v.Users {
		sumUsers = addBreakdown(sumUsers, b)
	}
	for _, b := range v.Queues {
		sumQueues = addBreakdown(sumQueues, b)
	}
	rtEqual(t, sumUsers.Total, v.Total.Total, "sum(user totals)")
	rtEqual(t, sumQueues.Total, v.Total.Total, "sum(queue totals)")
	rtEqual(t, v.Queues["q1"].Total, v.Total.Total, "single queue equals total")
}

// 失败重试场景：失联摘出段计 Failed，重启后完成段计 Finished；两段不重不漏。
func TestResourceTimeRetryThenFinish(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustAddNode(t, s, NodeSpec{Name: "n2", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1, MaxRetries: 1,
		Tasks: []TaskSpec{mkTask("t", 2, 1)}})
	mustSchedule(t, s)
	s.Tick(t0.Add(2*time.Minute), nil)
	if err := s.SetNodeState("n1", NodeLost); err != nil {
		t.Fatalf("node lost: %v", err)
	}
	t.Logf("JUDGE j detached at 2m (Failed bucket); retries on other node")
	mustSchedule(t, s)
	s.Tick(t0.Add(5*time.Minute), []string{"j/t"})
	v := s.ResourceTimes()
	b := v.Jobs["j"].Breakdown
	m := time.Minute
	rtEqual(t, b.Failed, ResourceTime{CPU: 2 * 2 * m, Memory: 2 * m}, "failed segment")
	rtEqual(t, b.Finished, ResourceTime{CPU: 2 * 3 * m, Memory: 3 * m}, "finished after retry")
	rtEqual(t, b.Total, b.Failed.add(b.Finished), "total = failed + finished")
	t.Logf("JUDGE retry conservation: failed=%s finished=%s total=%s", b.Failed.CPU, b.Finished.CPU, b.Total.CPU)
}

// Running 桶实时反映当前在跑段；抢占/完成的同一时刻不出现重复记账。
func TestResourceTimeRunningBucketLive(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 4, Memory: 4})
	mustSubmit(t, s, JobSpec{ID: "j", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("t", 2, 1)}})
	mustSchedule(t, s)
	s.Tick(t0.Add(time.Minute), nil)
	v := s.ResourceTimes()
	m := time.Minute
	rtEqual(t, v.Total.Running, ResourceTime{CPU: 2 * m, Memory: m}, "live running at 1m")
	rtEqual(t, v.Total.Total, v.Total.Running, "total equals running while active")
	s.Tick(t0.Add(3*time.Minute), []string{"j/t"})
	v = s.ResourceTimes()
	rtEqual(t, v.Total.Running, ResourceTime{}, "running drained after finish")
	rtEqual(t, v.Total.Finished, ResourceTime{CPU: 2 * 3 * m, Memory: 3 * m}, "finished 3m")
	rtEqual(t, v.Total.Total, v.Total.Finished, "total equals finished")
	t.Logf("JUDGE running bucket live then drains into finished without duplication")
}

// 与调度/抢占/取消/完成并发读取：任何时刻读到的快照五桶都自洽。
func TestResourceTimeConcurrentReads(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 32, Memory: 32})
	for i := 0; i < 8; i++ {
		mustSubmit(t, s, JobSpec{ID: "j" + itoa(i), User: "u" + itoa(i), Queue: "q1", Priority: 1,
			Tasks: []TaskSpec{mkTask("t", 2, 1)}})
	}
	mustSchedule(t, s)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // 并发读：校验五桶之和与逐桶相加一致。
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			v := s.ResourceTimes()
			want := v.Total.Running.add(v.Total.Finished).add(v.Total.Canceled).
				add(v.Total.Failed).add(v.Total.Preempted)
			if v.Total.Total != want {
				t.Errorf("inconsistent resource-time snapshot: total=%+v recomputed=%+v",
					v.Total.Total, want)
				return
			}
		}
	}()
	// 写侧：时钟推进、取消、抢占交替发生。
	for k := 0; k < 6; k++ {
		s.Tick(t0.Add(time.Duration(k+1)*time.Minute), nil)
		_ = s.CancelJob("j" + itoa(k))
		mustSubmit(t, s, JobSpec{ID: "hp" + itoa(k), User: "h" + itoa(k), Queue: "q1", Priority: 100,
			Tasks: []TaskSpec{mkTask("t", 2, 1)}})
		mustSchedule(t, s)
	}
	close(stop)
	wg.Wait()
	v := s.ResourceTimes()
	want := v.Total.Running.add(v.Total.Finished).add(v.Total.Canceled).
		add(v.Total.Failed).add(v.Total.Preempted)
	rtEqual(t, v.Total.Total, want, "final consistent snapshot")
	t.Logf("JUDGE concurrent reads during preempt/cancel/advance always self-consistent; total cpu=%s",
		v.Total.Total.CPU)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
