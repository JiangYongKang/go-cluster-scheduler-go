// Package scheduler 实现一个纯本地、可模拟的批处理集群调度器。
//
// 它以模拟节点与内存状态刻画作业从提交到结束的完整生命周期，
// 支持成组调度（gang）、GPU/NUMA/亲和性拓扑匹配、优先级与公平共享
// 抢占、节点故障恢复以及多维资源配额核算。
package scheduler

import "time"

// Resources 是可比较的标量资源向量。
type Resources struct {
	CPU    int64
	Memory int64
	GPU    int64
}

// LabelSelector 是简单的相等/不相等标签选择器。
type LabelSelector struct {
	MatchLabels    map[string]string // 候选节点必须全部相等
	NoScheduleKeys []string          // 候选节点上这些 key 不得存在
}

// Affinity 描述节点亲和/反亲和。
type Affinity struct {
	Required         *LabelSelector // 硬性要求：不满足则排除
	Preferred        *LabelSelector // 软性偏好：仅影响候选排序
	AntiAffinityWith []string       // 反亲和作业 ID：其运行任务所在节点必须排除
}

// GPUDevice 描述一块 GPU 设备。
type GPUDevice struct {
	ID     string
	NUMAID int
}

// NUMAZone 描述一个 NUMA 域上的局部资源。
type NUMAZone struct {
	ID   int
	CPUs int64
	Mem  int64
	GPUs []string // 该域上的 GPU 设备 ID
}

// NodeSpec 描述一个模拟节点。
type NodeSpec struct {
	Name     string
	CPU      int64
	Memory   int64
	GPUs     []GPUDevice
	NUMA     []NUMAZone // 为空表示不感知 NUMA
	Labels   map[string]string
	MaxTasks int // 0 表示不限
}

// NodeState 是节点生命周期状态。
type NodeState int

const (
	NodeReady NodeState = iota
	NodeDrained
	NodeLost
)

func (n NodeState) String() string {
	switch n {
	case NodeReady:
		return "Ready"
	case NodeDrained:
		return "Drained"
	case NodeLost:
		return "Lost"
	default:
		return "Unknown"
	}
}

// TaskSpec 描述作业中的单个任务。
type TaskSpec struct {
	Name      string
	Resources Resources
	NUMAID    int    // >=0 时要求任务资源落在指定 NUMA 域；GPU 与其同域
	GPUDevice string // 非空时要求指定 GPU 设备；为空且申请 GPU 时任意匹配
}

// JobSpec 描述一个作业（或作业数组）。
type JobSpec struct {
	ID         string
	User       string
	Queue      string
	Priority   int
	Tasks      []TaskSpec // 作业数组：全部任务成组调度
	MaxRetries int        // 节点失联导致的任务级重试上限
	Affinity   Affinity
}

// TaskState 是任务状态。
type TaskState int

const (
	TaskPending TaskState = iota
	TaskRunning
	TaskPreempted
	TaskFailed
	TaskFinished
)

func (t TaskState) String() string {
	switch t {
	case TaskPending:
		return "Pending"
	case TaskRunning:
		return "Running"
	case TaskPreempted:
		return "Preempted"
	case TaskFailed:
		return "Failed"
	case TaskFinished:
		return "Finished"
	default:
		return "Unknown"
	}
}

// JobState 是作业聚合状态。
type JobState int

const (
	JobPending JobState = iota
	JobRunning
	JobPartialFailure
	JobFinished
	JobCanceled
)

func (j JobState) String() string {
	switch j {
	case JobPending:
		return "Pending"
	case JobRunning:
		return "Running"
	case JobPartialFailure:
		return "PartialFailure"
	case JobFinished:
		return "Finished"
	case JobCanceled:
		return "Canceled"
	default:
		return "Unknown"
	}
}

// Reason 是调度/拒绝/故障原因的可区分归类。
type Reason string

// RejectReason 是提交/调度拒绝原因码。
type RejectReason string

const (
	ReasonOK RejectReason = "OK"
)

// DecisionKind 标识调度决策类型。
type DecisionKind string

// Decision 记录一次可解释的调度判定。
type Decision struct {
	At     time.Time
	Kind   DecisionKind
	JobID  string
	TaskID string
	Node   string
	Reason string
	Detail string
}

// QuotaConfig 描述用户/队列维度配额与公平权重。
type QuotaConfig struct {
	UserQueues   map[string][]string // user -> 允许的队列
	UserQuota    map[string]Resources
	QueueQuota   map[string]Resources
	UserWeight   map[string]float64 // 用户公平权重，缺省/<=0 视为 1
	QueueWeight  map[string]float64 // 队列公平权重，缺省/<=0 视为 1
	MaxJobsTotal int                // <=0 表示不限
}

// ResourceTime 是资源 × 时间的累计量（某一资源量持续了多久）。
// 例如 2 CPU 运行 3s，则 CPU 字段为 6s。
type ResourceTime struct {
	CPU    time.Duration
	Memory time.Duration
	GPU    time.Duration
}

// addSegment 累加“r 持续 d”的一段资源时间。
func (rt ResourceTime) addSegment(r Resources, d time.Duration) ResourceTime {
	return ResourceTime{
		CPU:    rt.CPU + time.Duration(r.CPU)*d,
		Memory: rt.Memory + time.Duration(r.Memory)*d,
		GPU:    rt.GPU + time.Duration(r.GPU)*d,
	}
}

func (rt ResourceTime) add(o ResourceTime) ResourceTime {
	return ResourceTime{CPU: rt.CPU + o.CPU, Memory: rt.Memory + o.Memory, GPU: rt.GPU + o.GPU}
}

// ResourceTimeBreakdown 按任务离场原因分桶的资源时间。
// Running 为查询时刻仍在运行的未结算段；其余四类为已结算段。
// Total = Running + Finished + Canceled + Failed + Preempted 恒成立。
type ResourceTimeBreakdown struct {
	Running   ResourceTime
	Finished  ResourceTime
	Canceled  ResourceTime
	Failed    ResourceTime
	Preempted ResourceTime
	Total     ResourceTime
}

// JobResourceTime 是单个作业的资源时间视图。
type JobResourceTime struct {
	ID        string
	User      string
	Queue     string
	Breakdown ResourceTimeBreakdown
}

// ResourceTimeView 是某一时刻按用户/队列/作业维度的资源时间一致快照。
// 每个维度的五类分桶之和恒等于 Total；同一任务的同一段时间在任何维度
// 都只计入一个桶，跨调度、抢占、取消、失败重试后守恒。
type ResourceTimeView struct {
	At     time.Time
	Users  map[string]ResourceTimeBreakdown
	Queues map[string]ResourceTimeBreakdown
	Jobs   map[string]JobResourceTime
	Total  ResourceTimeBreakdown
}

// Config 描述调度器上限与公平性参数。
type Config struct {
	MaxPendingJobs  int            // 待调度作业总数上限，<=0 不限
	MaxRunningTasks int            // 全局运行任务数上限，<=0 不限
	MaxDecisionLog  int            // 决策日志保留条数，<=0 不保留
	QueueMaxPending map[string]int // 队列待调度作业上限
}

// UsageSnapshot 是某一时刻的资源核算快照。
type UsageSnapshot struct {
	At        time.Time
	Running   Resources // 当前运行中占用
	Allocated Resources // 运行中 + 被抢占/待重试任务的预留份额
	Users     map[string]Resources
	Queues    map[string]Resources
	UserJobs  map[string]int
}

func (r Resources) add(o Resources) Resources {
	return Resources{CPU: r.CPU + o.CPU, Memory: r.Memory + o.Memory, GPU: r.GPU + o.GPU}
}

func (r Resources) sub(o Resources) Resources {
	return Resources{CPU: r.CPU - o.CPU, Memory: r.Memory - o.Memory, GPU: r.GPU - o.GPU}
}

// fitsIn 判断 r 是否可被 o 容纳（各维度均不超过）。
func (r Resources) fitsIn(o Resources) bool {
	return r.CPU <= o.CPU && r.Memory <= o.Memory && r.GPU <= o.GPU
}

func (r Resources) nonZero() bool {
	return r.CPU != 0 || r.Memory != 0 || r.GPU != 0
}
