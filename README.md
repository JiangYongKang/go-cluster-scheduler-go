# go-cluster-scheduler（本地批处理集群调度模拟组件）

一个**纯本地、零外部依赖**（仅 Go 标准库）的批处理集群调度器。它用内存中的
模拟节点刻画作业从提交、成组调度、运行、被抢占、节点故障重试到完成/取消的
完整生命周期，使资源约束、抢占、故障与取消等场景下的调度结论**一致、守恒、
可复现、可解释**。

不连接任何真实集群、外部服务或凭据；所有行为都可在本机用 `go test` /
`go run` 验证。

## 能力概览

- **成组调度（gang）**：同一作业（含作业数组）的全部待启动任务必须同时满足
  资源与约束，要么整体启动，要么一个都不启动；资源不足时不会出现部分启动。
- **拓扑匹配与设备/资源域强绑定**：GPU 设备钉选、NUMA 域局部资源
  （CPU/Mem/GPU 同域）、节点标签硬亲和与软偏好、作业间反亲和。**无论节点
  是否配置资源域（NUMA），钉设备都必须真落在该设备上**：设备不存在、已被
  占用、设备不在指定域分别给出 `GPU_ABSENT`/`GPU_BUSY`/`GPU_ZONE_MISMATCH`
  （无资源域节点只有前两种），绝不静默改选同节点编号更靠前的空闲卡或借域；
  钉设备但未指定域时 CPU/内存也必须同设备归属域。
- **优先级 + 公平共享抢占**：严格高优先级可抢占低优先级（不变）；同优先级
  之间按“已消耗资源份额”单向让出——累计消耗越多、按用户/队列权重
  （`UserWeights`/`QueueWeights`）归一化后份额**严格更大**的作业先让位，
  零消耗的新来者可以顶掉长跑多耗者；份额相等或更小的一方不能被抢，无人可让
  时维持原状并给出可区分的 `NO_YIELDABLE_SHARE`。被抢占作业重启时按持有
  份额重新排队，不会被历史消耗永久压制。抢占按整作业粒度贪心并做最小化
  剪枝；被抢占任务的已耗运行时按段结算，份额“回补 + 等量预留”，重新启动时
  预留转回运行；多轮抢占各段累加，不会重复计入或丢失。
- **累计资源时间视角**：`ResourceTimes()` 提供用户/队列/作业三维的累计
  资源时间，分为运行中、正常结束、被取消、失败、被抢占五类，五者之和恒等于
  总量；跨调度、抢占、取消、失败重试均守恒，可与这些操作并发地一致读取。
- **节点故障与恢复**：节点失联时其上任务被原子摘除、节点账本清零；按任务重试
  额度转为待重试（保留份额）或进入可区分的不可恢复终态
  （`RETRIES_EXHAUSTED`）。任务在任一时刻至多记账在一个节点上。
- **用户/队列/作业三维核算与配额**：运行份额与预留份额分账，
  `allocated = running + held`；跨调度、抢占、取消、失败重试累计使用量守恒；
  超额提交/扩容被明确拒绝，失败不残留部分调度状态。
- **并发安全与可复现**：所有状态在单一互斥锁下变更，`go test -race` 下验证；
  同一输入序列的任务→节点放置结果确定一致。
- **规模上限**：待调度作业数、队列待调度数、全局运行任务数、决策日志条数均可
  配置；超限按策略拒绝或延后，决策日志环形有界，不泄漏内存。

## 快速开始

```bash
go run ./cmd/sim        # 端到端场景演示：提交→成组→抢占→失联→恢复→取消
go test ./...           # 全部单元测试
go test -race ./...     # 含竞态检测
go test ./scheduler/ -v # 查看每个用例打印的“调度输入 + 判定依据”
```

## 最小用法

```go
import "fmt"

s := scheduler.New(
    &scheduler.Config{MaxDecisionLog: 1000},
    &scheduler.QuotaConfig{
        UserQuota:  map[string]scheduler.Resources{"alice": {CPU: 8, Memory: 64}},
        QueueQuota: map[string]scheduler.Resources{"batch": {CPU: 32, Memory: 256}},
    },
)
s.AddNode(scheduler.NodeSpec{Name: "n1", CPU: 8, Memory: 64})

s.Submit(scheduler.JobSpec{
    ID: "j", User: "alice", Queue: "batch", Priority: 10,
    Tasks: []scheduler.TaskSpec{
        {Name: "a", NUMAID: -1, Resources: scheduler.Resources{CPU: 4, Memory: 8}},
        {Name: "b", NUMAID: -1, Resources: scheduler.Resources{CPU: 4, Memory: 8}},
    },
})
report, _ := s.Schedule() // 两任务要么一起启动，要么一起等待
s.Tick(time.Now(), nil)

// 累计资源时间：五桶（运行/完成/取消/失败/抢占）之和等于总量。
rt := s.ResourceTimes()
for id, v := range rt.Jobs {
	fmt.Println(id, v.Buckets.Running, v.Buckets.Finished, v.Buckets.Canceled,
		v.Buckets.Failed, v.Buckets.Preempted, v.Buckets.Total)
}
```

## 文档

- [docs/DESIGN.md](docs/DESIGN.md)：调度语义、状态机、资源核算与公平性规则、
  错误归类、可复现性与本地验证方法。

## 目录结构

```
scheduler/      调度器库（类型、放置、公平抢占、核算、生命周期、错误归类）
cmd/sim/        本地端到端模拟演示
docs/           设计与语义说明
```
