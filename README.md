# go-cluster-scheduler

一个**纯内存、可本地运行**的批处理集群调度组件。用模拟节点与本地状态刻画作业从
提交、排队、成组放置、抢占、故障恢复到结束/取消/丢失的完整生命周期。不依赖任何
真实集群、外部服务或凭据。

## 目标与范围

* **作业与作业数组（gang scheduling，全有全无）**：同一作业的多个任务只有在整组
  资源与硬约束都满足时才一起启动；资源不足时**不会**部分启动。数组作为整体参与
  配额校验与状态汇总。
* **GPU / NUMA / 节点亲和-反亲和拓扑匹配（硬约束）**：不满足约束的节点被排除并
  附带可区分的原因（GPU 型号不符、GPU 耗尽、NUMA 本地性不满足、亲和不符、反亲和
  冲突、资源不足、节点不可用）。**绝不静默降级**到违反约束的节点。
* **优先级 + 公平共享抢占**：严格高优先级才能抢占；同优先级互不抢占。在所有合格
  （低优先级）受害者中，按"累计资源时间/权重"从高到低选择更该让出的用户。被抢占
  任务已消耗的份额一次性结算，已运行时长不丢失，剩余运行时回补，多次抢占既不重复
  计入也不会丢失。
* **节点故障与作业恢复**：节点失联后其上任务被明确标记（`Preempted` 等待重新调度，
  或永久故障下超出重启预算的 `Lost` 终态）。任务在任意时刻至多有一个放置记录，不
  可能在两个节点上同时记账；无法恢复的任务有可区分终态与失败说明。
* **用户 / 队列 / 作业三维核算与配额**：提交时对**整个数组**做配额预检，配额耗尽
  或超额扩容被明确拒绝（错误可区分为用户配额 / 队列配额 / 决策规模上限），失败不
  残留任何部分调度状态。资源时间在 运行中/完成/取消/失败/被抢占 五类之间守恒。
* **并发安全与可复现**：提交、取消、节点状态更新、Tick 全部由单一互斥锁串行化；
  作业、任务、节点、受害者的遍历次序全部确定，相同输入序列产生完全一致的调度结果。
* **决策规模上限**：可配置存活作业数、单作业任务数、每 tick 放置数/抢占数、决策日
  志保留条数；超限按策略拒绝（提交期）或延后（调度期），不会无界占用内存。

## 目录结构

```
pkg/api/          领域类型：资源、节点、GPU/NUMA、作业/任务/状态、错误分类、决策记录
pkg/topology/     节点本地资源视图：NUMA 分区 + 离散 GPU，硬约束判定与确定性放置
pkg/accounting/   用户/队列配额、瞬时分配、守恒的资源时间账本、加权公平份额
pkg/preemption/   确定性受害者选择（优先级门控 + 公平份额排序）
pkg/scheduler/    调度核心：提交校验、gang 试算-提交、抢占、故障恢复、Tick、决策日志
pkg/simulation/   确定性逻辑时钟与事件时间线驱动
cmd/scheduler-demo/  本地端到端演示
```

## 核心调度语义

### 逻辑时钟（Tick）

一个 `Tick()` 是一个确定性调度周期，内部顺序固定：

1. **执行与完成**：对运行中的任务累计一个 tick 的资源时间；达到 `Runtime` 的任务
   当场完成并释放资源。
2. **调度**：候选作业按 `优先级降序、提交序号升序`排序，逐作业做 gang 试算。
3. **汇总**：重算每个作业的聚合状态。
4. **裁剪**：按 `MaxDecisionLog` 保留最近的决策日志。

任务在 tick `t` 被放置，**从 tick `t+1` 起**累计执行；抢占/失联发生的那个 tick，
受害者先完成该 tick 的执行再被结算，因此账本与运行时长严格一致。

### Gang（全有全无）放置与"试算-提交"

对每个作业，调度器在拓扑视图的**深拷贝**上模拟整组放置：

* 先基于**忽略占用的原始容量**判断：即使所有节点全空，集群是否放得下这个数组。
  放不下 = 硬约束/容量永久不可满足，作业进入 `Failed` 终态，决策中给出每个节点的
  拒绝原因；放得下但当前被占用 = 暂时不足，可等待或抢占（`Deferred`）。
* 对数组逐任务选节点（最空闲优先，均衡摊布），节点不足时只抢占**差额**资源。每次
  尝试都在隔离的临时视图上进行，只有成功才合并；失败的尝试不会把无关受害者带入计
  划，也不会在真实状态上留下半更新。
* 整组可行后，才在真实视图上**先提交抢占、再一次性提交全部放置与配额分配**。

### 拓扑硬约束

`api.TopologyReq` 全部为硬约束：

* `GPUModel`：节点必须有该型号 GPU，且有足够空闲设备；
* `NumaLocal`：一个任务的 CPU/内存与 GPU 必须落在同一个 NUMA 节点内；
* `NodeAffinity`：节点标签必须全部匹配；
* `AntiAffinity`：节点上不得运行指定作业的任务。

GPU 以离散设备记账（按索引占用/释放），NUMA 的 CPU/内存独立计量。放置选择（NUMA、
GPU、节点）均取最小索引/最空闲者，保证可复现。

### 抢占规则

* **资格**：仅当受害者优先级**严格低于**抢占者且属于不同用户时才可抢占；同优先级、
  同用户、自身都不会被抢占。
* **顺序**：优先级升序 → `累计资源时间 ÷ 权重`降序 → 起始 tick 升序 → 任务 ID。
* **规模**：单 tick 抢占数受 `MaxPreemptPerTick` 约束；只抢占补足节点差额所需数量；
  抢占不足以放下整个 gang 时，本周期整组延后（不会部分启动）。
* **结算**：受害者释放瞬时配额与拓扑放置，其本段已累计资源时间一次性移入
  `Preempted` 桶；`RunTicks` 保留，重调度后只执行 `Runtime - RunTicks`。

### 节点故障

* `SetNodeState(name, NotReady)`（软失联）：驱逐其上任务 → `Preempted`（可重调度），
  运行时与份额保留；节点恢复后下一周期重新放置。
* `FailNode(name, maxRestarts)`（永久故障）：驱逐任务；累计重启次数达到
  `maxRestarts` 的任务进入 **`Lost`** 终态（带失败说明），其余按 `Preempted` 处理。
* 驱逐通过 `View.FreeOnNode` 原子释放该节点全部 GPU/NUMA/CPU 预留；任务从放置表移
  除，保证"一个任务至多一个节点"的记账不变量。

### 作业/任务状态

任务：`Pending → Running → Finished`，可经 `Preempted → Running` 反复重入；终态为
`Finished / Failed / Canceled / Lost`。作业状态由任务聚合：全部 Finished →
`Succeeded`；全部终态但非全成功 → `Failed`；有运行中 → `Active`；否则 `Queued`。

## 资源核算与公平性规则

数量使用 milli 单位整数（1000 = 1），全程整数运算，无浮点误差。

* **瞬时分配**（`Allocated`）：任务运行占用的 CPU/内存/GPU，放置时加、停止时减。
* **资源时间**（资源量 × tick）：运行期间逐 tick 计入 `Running` 桶；每段运行结束时
  **恰好一次** `Settle` 转入 `Finished/Canceled/Failed/Preempted` 之一。
* **守恒式**：

  ```
  Total = Running + Finished + Canceled + Failed + Preempted
  ```

  该总量在调度、多次抢占、取消、失败重试之间保持不变：被抢占的工作不会重跑，也不
  会从账本消失。静止时 `Running` 桶必为 0（测试断言）。
* **配额**：用户与队列两层上限，0 表示该维度不限。`Submit` 对 `TaskRes × ArraySize`
  的整组需求做预检；用户与队列同时满足才受理。超额扩容返回
  `ErrQuotaExceeded`/`ErrQueueQuotaExceeded`，且不创建任何作业状态。
* **公平份额**：`Ledger.FairShare` 按权重给出目标份额；受害者排序使用
  `累计资源时间 / 权重`，使"超额使用"的用户先让出。权重经 `SetWeight` 配置，缺省 1。

## 并发与可复现性

`Scheduler` 内所有可变状态由一把 `sync.Mutex` 保护；`Ledger` 自带锁并仅在持锁期间
访问其 map。所有集合遍历（节点名、作业顺序、候选任务、受害者）均使用固定/排序后的
次序。`go test -race` 下并发压测（提交 / 取消 / Tick / 节点抖动同时进行）通过。

## 本地验证

仅需 Go（模块声明 go 1.25），无需任何外部依赖或网络：

```bash
# 构建
go build ./...

# 全部单元测试（资源不足、约束冲突、抢占守恒、节点故障、配额、取消、并发）
go test ./...

# 竞态检测
go test -race ./...

# 端到端演示：gang GPU/NUMA/亲和、硬拒绝、优先级抢占、故障恢复、配额
go run ./cmd/scheduler-demo
```

测试中通过 `scheduler.SetTrace(io.Writer)` 打开调度输入与判定依据日志，单测以
`t.Logf` 打印每个决策（放置节点、NUMA/GPU、抢占受害者、拒绝原因等）。

### 测试场景索引

| 场景 | 测试 |
| --- | --- |
| 资源不足时 gang 不部分启动、硬拒绝原因 | `TestGangAllOrNothingUnderShortage` |
| GPU 型号/NUMA/亲和/反亲和可区分拒绝、不降级 | `topology.TestEvaluateReasons` |
| 高优先级抢占、份额一次结算、剩余回补 | `TestPriorityPreemptionSettlement` |
| 同优先级/低优先级不得抢占 | `TestNoPreemptionOfEqualOrHigher` |
| 多次抢占不重复计入、总量守恒 | `TestRepeatedPreemptionNoDoubleCount` |
| 受害者确定性排序 | `preemption.TestSelectVictimsOrdering` |
| 软失联重调度、无双节点并行记账 | `TestSoftNodeFailureReschedulesOnce` |
| 永久故障 → Lost 终态、作业 Failed | `TestPermanentNodeFailureLostTerminal` |
| 用户/队列配额拒绝、超额扩容不受理 | `TestQuotasRejectOverExpansion` |
| 取消释放与守恒、终态幂等 | `TestCancelConservationNoResidue` |
| 决策规模上限（作业/数组/每 tick/日志） | `TestDecisionLimits` |
| 并发安全 + 分配/运行一致 | `TestConcurrentAPISafety` |
| 相同输入序列可复现 | `TestDeterministicSameInput` |
| 时间线：数组成组、故障、抢占、完成 | `simulation.TestTimeline*` |
