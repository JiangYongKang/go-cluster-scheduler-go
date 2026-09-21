package scheduler

import (
	"testing"
	"time"
)

// 场景1：资源充足时作业数组整体启动；资源不足时不得部分启动。
func TestGangSchedulingAllOrNothing(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 8, Memory: 16})

	// 两个任务合计 cpu=6 <= 8：整体启动。
	j1 := JobSpec{ID: "j1", User: "u1", Queue: "q1", Priority: 10,
		Tasks: []TaskSpec{mkTask("a", 4, 4), mkTask("b", 2, 2)}}
	mustSubmit(t, s, j1)
	rpt := mustSchedule(t, s)
	if len(rpt.StartedJobs) != 1 {
		t.Fatalf("want 1 started job, got %d", len(rpt.StartedJobs))
	}
	assertTaskStates(t, s, "j1", map[string]TaskState{"a": TaskRunning, "b": TaskRunning})

	// 剩余 cpu=2：j2 的两个任务合计 cpu=4，无法整体放下，必须一个都不启动。
	j2 := JobSpec{ID: "j2", User: "u2", Queue: "q1", Priority: 5,
		Tasks: []TaskSpec{mkTask("c", 2, 1), mkTask("d", 2, 1)}}
	mustSubmit(t, s, j2)
	rpt = mustSchedule(t, s)
	if len(rpt.StartedJobs) != 0 {
		t.Fatalf("want no started job, got %v", rpt.StartedJobs)
	}
	if len(rpt.Unschedulable) != 1 || rpt.Unschedulable[0].JobID != "j2" {
		t.Fatalf("want j2 unschedulable, got %+v", rpt.Unschedulable)
	}
	t.Logf("JUDGE j2 rejected reason=%s; neither task partially started", rpt.Unschedulable[0].Reason)
	assertTaskStates(t, s, "j2", map[string]TaskState{"c": TaskPending, "d": TaskPending})

	// 完成 j1 全部任务释放份额后再调度，j2 应整体启动。
	s.Tick(t0.Add(time.Minute), []string{"j1/a", "j1/b"})
	rpt = mustSchedule(t, s)
	if len(rpt.StartedJobs) != 1 || rpt.StartedJobs[0] != "j2" {
		t.Fatalf("want j2 started after freeing, got %v", rpt.StartedJobs)
	}
	assertTaskStates(t, s, "j2", map[string]TaskState{"c": TaskRunning, "d": TaskRunning})
}

// 场景2：成组任务跨节点装箱：单机放不下全部，但集群总量满足时整体启动。
func TestGangSchedulingSpansNodes(t *testing.T) {
	s := newTestScheduler(t, nil, nil)
	mustAddNode(t, s, NodeSpec{Name: "n1", CPU: 3, Memory: 4})
	mustAddNode(t, s, NodeSpec{Name: "n2", CPU: 3, Memory: 4})
	j := JobSpec{ID: "jspan", User: "u1", Queue: "q1", Priority: 1,
		Tasks: []TaskSpec{mkTask("a", 3, 1), mkTask("b", 3, 1)}}
	mustSubmit(t, s, j)
	rpt := mustSchedule(t, s)
	if len(rpt.StartedJobs) != 1 {
		t.Fatalf("want gang scheduled across nodes, got unsched=%+v", rpt.Unschedulable)
	}
	info := s.GetJob("jspan")
	nodes := map[string]int{}
	for _, tk := range info.Tasks {
		nodes[tk.Node]++
	}
	if nodes["n1"] != 1 || nodes["n2"] != 1 {
		t.Fatalf("want one task per node, got %v", nodes)
	}
	t.Logf("JUDGE gang tasks placed on separate nodes: %v", nodes)
}
