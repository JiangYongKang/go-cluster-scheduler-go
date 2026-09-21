package scheduler

import (
	"fmt"
	"sync"
	"testing"
)

// 并发提交/调度/取消/节点更新不得出现重复调度、半更新或 panic。
func TestConcurrentOperationsSafe(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	for i := 0; i < 4; i++ {
		mustAddNode(t, s, NodeSpec{Name: fmt.Sprintf("n%d", i), CPU: 16, Memory: 32})
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for k := 0; k < 25; k++ {
				id := fmt.Sprintf("j-%d-%d", w, k)
				err := s.Submit(JobSpec{ID: id, User: fmt.Sprintf("u%d", w%3), Queue: "q1",
					Priority: (w + k) % 10,
					Tasks:    []TaskSpec{mkTask("a", 1, 1), mkTask("b", 1, 1)}})
				if err != nil {
					continue
				}
				_, _ = s.Schedule()
				if k%5 == 0 {
					_ = s.CancelJob(id)
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for k := 0; k < 20; k++ {
			_ = s.SetNodeState("n1", NodeLost)
			_ = s.SetNodeState("n1", NodeReady)
		}
	}()
	wg.Wait()

	// 记账不变量：任意节点账本与其 running 集合严格一致；无重复任务。
	for name, nd := range s.nodes {
		var cpu, mem int64
		for key, tk := range nd.running {
			cpu += tk.spec.Resources.CPU
			mem += tk.spec.Resources.Memory
			// 每个 running key 唯一（map 保证）；任务自报节点必须等于所在节点。
			if tk.node != name {
				t.Fatalf("task %s reports node %s while booked on %s", key, tk.node, name)
			}
		}
		if cpu != nd.usedCPU || mem != nd.usedMem {
			t.Fatalf("node %s ledger mismatch: summed cpu=%d mem=%d vs books cpu=%d mem=%d",
				name, cpu, mem, nd.usedCPU, nd.usedMem)
		}
		if nd.taskCount != len(nd.running) {
			t.Fatalf("node %s taskCount=%d but running=%d", name, nd.taskCount, len(nd.running))
		}
	}
	// 用户/队列运行份额必须等于全部节点 running 之和。
	snap := s.Snapshot()
	var total int64
	for _, r := range snap.Users {
		total += r.CPU
	}
	var runningTotal int64
	for _, nd := range s.nodes {
		for _, tk := range nd.running {
			runningTotal += tk.spec.Resources.CPU
		}
	}
	var heldTotal int64
	for _, h := range s.preemptHold {
		heldTotal += h.CPU
	}
	if total != runningTotal+heldTotal {
		t.Fatalf("allocation conservation broken: users=%d running=%d held=%d",
			total, runningTotal, heldTotal)
	}
	t.Logf("JUDGE concurrent ops consistent: running=%d held=%d user-allocated=%d",
		runningTotal, heldTotal, total)
}

// 同一输入序列两次独立运行的调度结果（任务->节点）必须一致。
func TestDeterministicReproducible(t *testing.T) {
	run := func() map[string]string {
		s := newTestScheduler(t, nil, nil)
		mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 6, Memory: 8})
		mustAddNode(t, s, NodeSpec{Name: "n2", CPU: 6, Memory: 8})
		jobs := []struct {
			id string
			p  int
			c  int64
		}{
			{"a", 1, 4}, {"b", 2, 4}, {"c", 1, 2}, {"d", 3, 6}, {"e", 2, 2},
		}
		for _, j := range jobs {
			mustSubmit(t, s, JobSpec{ID: j.id, User: "u" + j.id, Queue: "q1", Priority: j.p,
				Tasks: []TaskSpec{mkTask("t", j.c, 1)}})
			_, _ = s.Schedule()
		}
		out := map[string]string{}
		for _, j := range jobs {
			if info := s.GetJob(j.id); info != nil {
				out[j.id] = info.Tasks[0].Node + ":" + info.Tasks[0].State.String()
			}
		}
		return out
	}
	first := run()
	second := run()
	if len(first) != len(second) {
		t.Fatalf("result size differs: %v vs %v", first, second)
	}
	for k, v := range first {
		if second[k] != v {
			t.Fatalf("non-reproducible placement for %s: %s vs %s\nfirst=%v\nsecond=%v",
				k, v, second[k], first, second)
		}
	}
	t.Logf("JUDGE deterministic result: %v", first)
}

// 决策日志有界：超过上限不无限增长。
func TestDecisionLogBounded(t *testing.T) {
	s := newTestScheduler(t, &Config{MaxDecisionLog: 50}, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 1, Memory: 1})
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("j%d", i)
		_ = s.Submit(JobSpec{ID: id, User: "u", Queue: "q1", Priority: 1,
			Tasks: []TaskSpec{mkTask("a", 8, 1)}})
		_, _ = s.Schedule()
	}
	if n := len(s.Decisions()); n > 50 {
		t.Fatalf("decision log unbounded: %d entries", n)
	}
	t.Logf("JUDGE decision log bounded at %d entries", len(s.Decisions()))
}

// 重复提交同一作业被幂等拒绝，不产生双份状态。
func TestDuplicateSubmitRejected(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	spec := JobSpec{ID: "d", User: "u", Queue: "q1", Tasks: []TaskSpec{mkTask("a", 1, 1)}}
	if err := s.Submit(spec); err != nil {
		t.Fatal(err)
	}
	if err := s.Submit(spec); !IsReason(err, ReasonDuplicateJob) {
		t.Fatalf("want DUPLICATE_JOB, got %v", err)
	}
	if len(s.jobs) != 1 || len(s.order) != 1 {
		t.Fatalf("duplicate submit left state: jobs=%d order=%d", len(s.jobs), len(s.order))
	}
}
