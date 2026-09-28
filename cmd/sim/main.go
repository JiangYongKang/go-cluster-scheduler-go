// Command sim 在本地以模拟节点运行一个端到端调度场景并打印决策日志。
// 它不依赖任何真实集群、外部服务或凭据：
//
//	go run ./cmd/sim
//
// 场景覆盖：成组调度、GPU/NUMA 约束、优先级抢占与份额回补、
// 节点失联重试、取消、配额与决策日志。
package main

import (
	"fmt"
	"time"

	"github.com/highcumontoa/go-cluster-scheduler-go/scheduler"
)

func main() {
	base := time.Unix(1_700_000_000, 0)
	cfg := &scheduler.Config{MaxDecisionLog: 400}
	quota := &scheduler.QuotaConfig{
		UserQuota: map[string]scheduler.Resources{
			"alice": {CPU: 12, Memory: 64, GPU: 4},
			"bob":   {CPU: 12, Memory: 32, GPU: 2},
		},
		QueueQuota: map[string]scheduler.Resources{
			"batch": {CPU: 24, Memory: 128, GPU: 8},
		},
	}
	s := scheduler.New(cfg, quota)
	s.SetClock(base)

	// 两台模拟节点：一台带 2 NUMA 域 + 2 GPU，一台普通节点。
	must(s.AddNode(scheduler.NodeSpec{
		Name: "gpu-node", CPU: 8, Memory: 32,
		GPUs: []scheduler.GPUDevice{{ID: "gpu0", NUMAID: 0}, {ID: "gpu1", NUMAID: 1}},
		NUMA: []scheduler.NUMAZone{
			{ID: 0, CPUs: 4, Mem: 16, GPUs: []string{"gpu0"}},
			{ID: 1, CPUs: 4, Mem: 16, GPUs: []string{"gpu1"}},
		},
		Labels: map[string]string{"accel": "gpu"},
	}))
	must(s.AddNode(scheduler.NodeSpec{Name: "cpu-node", CPU: 8, Memory: 32,
		Labels: map[string]string{"accel": "cpu"}}))

	fmt.Println("== 1. 提交 alice 的 GPU 作业数组（2 任务必须成组，分属两个 NUMA 域） ==")
	must(s.Submit(scheduler.JobSpec{
		ID: "job-gpu", User: "alice", Queue: "batch", Priority: 10, MaxRetries: 1,
		Tasks: []scheduler.TaskSpec{
			{Name: "g0", NUMAID: 0, Resources: scheduler.Resources{CPU: 2, Memory: 4, GPU: 1}},
			{Name: "g1", NUMAID: 1, Resources: scheduler.Resources{CPU: 2, Memory: 4, GPU: 1}},
		},
	}))
	printReport(schedule(s), s)

	fmt.Println("== 2. 提交 bob 的普通作业，占满 cpu-node 与 gpu-node 剩余 CPU ==")
	must(s.Submit(scheduler.JobSpec{
		ID: "job-bulk", User: "bob", Queue: "batch", Priority: 5, MaxRetries: 1,
		Tasks: []scheduler.TaskSpec{
			{Name: "b0", NUMAID: -1, Resources: scheduler.Resources{CPU: 4, Memory: 4}},
			{Name: "b1", NUMAID: -1, Resources: scheduler.Resources{CPU: 4, Memory: 4}},
			{Name: "b2", NUMAID: -1, Resources: scheduler.Resources{CPU: 4, Memory: 4}},
		},
	}))
	printReport(schedule(s), s)

	fmt.Println("== 3. 高优先级紧急作业到达，触发抢占 ==")
	s.Tick(base.Add(time.Minute), nil)
	must(s.Submit(scheduler.JobSpec{
		ID: "job-urgent", User: "alice", Queue: "batch", Priority: 100,
		Affinity: scheduler.Affinity{Required: &scheduler.LabelSelector{
			MatchLabels: map[string]string{"accel": "cpu"}}},
		Tasks: []scheduler.TaskSpec{
			{Name: "u0", NUMAID: -1, Resources: scheduler.Resources{CPU: 4, Memory: 4}},
		},
	}))
	printReport(schedule(s), s)

	fmt.Println("== 4. 模拟 gpu-node 失联：其上任务按重试额度重新调度 ==")
	s.Tick(base.Add(2*time.Minute), nil)
	must(s.SetNodeState("gpu-node", scheduler.NodeLost))
	printReport(schedule(s), s)

	fmt.Println("== 5. gpu-node 恢复，紧急作业完成；被抢占/待重试任务重新成组调度 ==")
	must(s.SetNodeState("gpu-node", scheduler.NodeReady))
	s.Tick(base.Add(3*time.Minute), []string{"job-urgent/u0"})
	printReport(schedule(s), s)

	fmt.Println("== 6. 取消 bob 的批量作业，份额全部回补 ==")
	must(s.CancelJob("job-bulk"))
	printSnapshot(s)

	fmt.Println("\n== 7. 资源时间视角（运行/完成/取消/失败/被抢占 五类，五桶之和=总量） ==")
	rt := s.ResourceTimes()
	fmt.Printf("  RT total    run=%s fin=%s cancel=%s fail=%s preempt=%s\n",
		rt.Total.Running.CPU, rt.Total.Finished.CPU, rt.Total.Canceled.CPU,
		rt.Total.Failed.CPU, rt.Total.Preempted.CPU)
	for _, id := range []string{"job-gpu", "job-bulk", "job-urgent"} {
		if j, ok := rt.Jobs[id]; ok {
			b := j.Breakdown
			fmt.Printf("  RT job=%-10s run=%s fin=%s cancel=%s fail=%s preempt=%s (total=%s)\n",
				id, b.Running.CPU, b.Finished.CPU, b.Canceled.CPU,
				b.Failed.CPU, b.Preempted.CPU, b.Total.CPU)
		}
	}

	fmt.Println("\n== 决策日志（最后 25 条） ==")
	d := s.Decisions()
	start := 0
	if len(d) > 25 {
		start = len(d) - 25
	}
	for _, x := range d[start:] {
		task := x.TaskID
		if task != "" {
			task = "/" + task
		}
		fmt.Printf("[%s] %-18s job=%s%s node=%-9s %s %s\n",
			x.At.Format("15:04:05"), x.Kind, x.JobID, task, x.Node, reasonTag(x.Reason), x.Detail)
	}
}

func schedule(s *scheduler.Scheduler) *scheduler.ScheduleReport {
	r, err := s.Schedule()
	if err != nil {
		panic(err)
	}
	return r
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func reasonTag(r string) string {
	if r == "" {
		return ""
	}
	return "(" + r + ")"
}

func printReport(r *scheduler.ScheduleReport, s *scheduler.Scheduler) {
	for _, id := range r.StartedJobs {
		fmt.Printf("  START   %s tasks=%v\n", id, r.StartedTasks[id])
	}
	for _, p := range r.PreemptedJobs {
		fmt.Printf("  PREEMPT %s -> by %s tasks=%v settled=%s returned=cpu%d\n",
			p.PreemptedJobID, p.PreemptingJobID, p.PreemptedTasks,
			p.SettledRuntime, p.ReturnedShare.CPU)
	}
	for _, u := range r.Unschedulable {
		fmt.Printf("  WAIT    %s reason=%s detail=%s\n", u.JobID, u.Reason, u.Detail)
	}
	printSnapshot(s)
}

func printSnapshot(s *scheduler.Scheduler) {
	snap := s.Snapshot()
	fmt.Printf("  SNAP    running=cpu%d allocated=cpu%d users=%v queues=%v\n",
		snap.Running.CPU, snap.Allocated.CPU, snap.Users, snap.Queues)
}
