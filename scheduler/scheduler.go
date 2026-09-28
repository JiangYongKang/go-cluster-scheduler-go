package scheduler

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// 决策类型。
const (
	DecisionSubmit        DecisionKind = "SUBMIT"
	DecisionReject        DecisionKind = "REJECT"
	DecisionSchedule      DecisionKind = "SCHEDULE"
	DecisionTaskStart     DecisionKind = "TASK_START"
	DecisionUnschedulable DecisionKind = "UNSCHEDULABLE"
	DecisionPreempt       DecisionKind = "PREEMPT"
	DecisionPreemptFail   DecisionKind = "PREEMPT_FAILED"
	DecisionComplete      DecisionKind = "TASK_COMPLETE"
	DecisionCancel        DecisionKind = "CANCEL"
	DecisionNodeLost      DecisionKind = "NODE_LOST"
	DecisionRetry         DecisionKind = "TASK_RETRY"
	DecisionRetryFail     DecisionKind = "RETRY_EXHAUSTED"
)

// ScheduleReport 汇总一个调度周期的结果。
type ScheduleReport struct {
	At            time.Time
	StartedJobs   []string
	StartedTasks  map[string][]string // jobID -> 本次启动的任务名
	PreemptedJobs []PreemptionRecord
	Unschedulable []UnschedulableJob
}

// PreemptionRecord 描述一次抢占结算（被抢占任务的已耗运行时与回补份额）。
type PreemptionRecord struct {
	PreemptedJobID  string
	PreemptedTasks  []string
	PreemptingJobID string
	Node            string
	SettledRuntime  time.Duration
	ReturnedShare   Resources
}

// UnschedulableJob 描述本周期无法启动的作业及可区分原因。
type UnschedulableJob struct {
	JobID       string
	Reason      RejectReason
	NodeReasons map[string][]FilterReason
	Detail      string
}

// TaskInfo 是任务的只读视图。
type TaskInfo struct {
	Name       string
	State      TaskState
	Node       string
	NUMAID     int
	GPU        string
	Retries    int
	TermReason TerminalReason
	Runtime    time.Duration // 已结算累计运行时长
}

// JobInfo 是作业的只读视图（数组维度汇总）。
type JobInfo struct {
	ID           string
	User         string
	Queue        string
	Priority     int
	State        JobState
	Tasks        []TaskInfo
	NumPending   int
	NumPreempted int
	NumRunning   int
	NumFinished  int
	NumFailed    int
}

// task 是任务的内部可变状态。
type task struct {
	spec      TaskSpec
	state     TaskState
	node      string
	numaID    int
	gpu       string
	retries   int
	startedAt time.Time
	runtime   time.Duration // 已结算（完成/被抢占/失联）的累计运行时长
	// rt 为按终局原因分桶的已结运行时段资源时间；当前运行时段在查询时
	// 按 startedAt..now 即时计算，不提前入账，避免重复或漏记。
	rt   ResourceTimeBuckets
	term TerminalReason
}

// job 是作业的内部可变状态。
type job struct {
	spec      JobSpec
	tasks     []*task
	state     JobState
	submitSeq int
	firstRun  time.Time // 首次启动时间，用于抢占受害者排序
}

// node 是模拟节点的内部可变状态。
type node struct {
	spec      NodeSpec
	state     NodeState
	usedCPU   int64
	usedMem   int64
	busyGPUs  map[string]bool
	taskCount int
	numaCPU   map[int]int64
	numaMem   map[int]int64
	numaGPU   map[int]map[string]bool
	running   map[string]*task // key = jobID/taskName -> 运行中任务
}

// Scheduler 是批处理集群调度器。所有状态访问均在 mu 保护之下，
// 因此同一输入序列在单进程内可复现。
type Scheduler struct {
	mu    sync.Mutex
	cfg   Config
	quota QuotaConfig

	nodes map[string]*node
	jobs  map[string]*job
	order []string // 提交顺序

	// 记账：当前运行中份额（启动计入、终止/被抢占/失联立即回补）。
	userUsed  map[string]Resources
	queueUsed map[string]Resources
	// 被抢占/等待重试任务的预留份额（同一 key 只保留一份，重复抢占不叠加）。
	preemptHold map[string]Resources

	submitSeq int
	now       time.Time
	decisions []Decision
}

// New 创建调度器。nil 参数取默认空配置（不设上限/配额）。
func New(cfg *Config, quota *QuotaConfig) *Scheduler {
	s := &Scheduler{
		nodes:       map[string]*node{},
		jobs:        map[string]*job{},
		userUsed:    map[string]Resources{},
		queueUsed:   map[string]Resources{},
		preemptHold: map[string]Resources{},
		now:         time.Unix(0, 0),
	}
	if cfg != nil {
		s.cfg = *cfg
	}
	if quota != nil {
		s.quota = *quota
	}
	return s
}

// AddNode 注册一个模拟节点。
func (s *Scheduler) AddNode(spec NodeSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateNode(spec); err != nil {
		return err
	}
	if _, ok := s.nodes[spec.Name]; ok {
		return schedError("AddNode", ReasonInvalidJob, "node %s already exists", spec.Name)
	}
	n := &node{
		spec:     cloneNodeSpec(spec),
		state:    NodeReady,
		busyGPUs: map[string]bool{},
		numaCPU:  map[int]int64{},
		numaMem:  map[int]int64{},
		numaGPU:  map[int]map[string]bool{},
		running:  map[string]*task{},
	}
	for _, z := range spec.NUMA {
		n.numaCPU[z.ID] = 0
		n.numaMem[z.ID] = 0
		n.numaGPU[z.ID] = map[string]bool{}
	}
	s.nodes[spec.Name] = n
	return nil
}

// NodeStateView 返回节点状态；节点不存在时 ok=false。
func (s *Scheduler) NodeStateView(name string) (NodeState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[name]
	if !ok {
		return NodeReady, false, schedError("NodeStateView", ReasonNodeNotFound, "node %s not found", name)
	}
	return n.state, true, nil
}

// SetNodeState 更新节点状态；置为 NodeLost 会立刻原子地触发其上任务的故障处理。
func (s *Scheduler) SetNodeState(name string, st NodeState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[name]
	if !ok {
		return schedError("SetNodeState", ReasonNodeNotFound, "node %s not found", name)
	}
	old := n.state
	n.state = st
	if st == NodeLost && old != NodeLost {
		s.handleNodeLost(n, s.now)
	}
	return nil
}

// Submit 提交一个作业（数组）；不满足配额/上限则整体拒绝，不残留任何状态。
func (s *Scheduler) Submit(spec JobSpec) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validate(spec); err != nil {
		s.logLocked(Decision{At: s.now, Kind: DecisionReject, JobID: spec.ID,
			Reason: string(ReasonInvalidJob), Detail: err.Error()})
		return err
	}
	if _, dup := s.jobs[spec.ID]; dup {
		err := schedError("Submit", ReasonDuplicateJob, "job %s already submitted", spec.ID)
		s.logLocked(Decision{At: s.now, Kind: DecisionReject, JobID: spec.ID,
			Reason: string(ReasonDuplicateJob), Detail: err.Error()})
		return err
	}
	if reason, detail := s.checkSubmitLimits(spec); reason != ReasonOK {
		err := schedError("Submit", reason, "%s", detail)
		s.logLocked(Decision{At: s.now, Kind: DecisionReject, JobID: spec.ID,
			Reason: string(reason), Detail: detail})
		return err
	}
	j := &job{spec: cloneJobSpec(spec), state: JobPending, submitSeq: s.submitSeq}
	s.submitSeq++
	for _, ts := range j.spec.Tasks {
		j.tasks = append(j.tasks, &task{spec: ts, state: TaskPending, numaID: ts.NUMAID})
	}
	s.jobs[j.spec.ID] = j
	s.order = append(s.order, j.spec.ID)
	s.logLocked(Decision{At: s.now, Kind: DecisionSubmit, JobID: j.spec.ID,
		Detail: summarizeJob(j)})
	return nil
}

// CancelJob 取消作业：运行中任务立即结算并回补份额，排队/被抢占任务终止并释放预留。
func (s *Scheduler) CancelJob(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return schedError("CancelJob", ReasonJobNotFound, "job %s not found", id)
	}
	if j.state == JobFinished || j.state == JobCanceled {
		return schedError("CancelJob", ReasonInvalidJob, "job %s already terminal: %s", id, j.state)
	}
	for _, t := range j.tasks {
		key := s.taskKey(id, t.spec.Name)
		switch t.state {
		case TaskRunning:
			s.settleTask(j, t, s.now, TermCanceled)
			t.state = TaskFailed
			key2 := s.taskKey(id, t.spec.Name)
			if nd := s.nodes[t.node]; nd != nil {
				undoNodeCommit(nd, t.spec, placeOn{numaID: t.numaID, gpu: t.gpu})
				delete(nd.running, key2)
			}
			t.node, t.gpu = "", ""
			t.numaID = t.spec.NUMAID
			s.release(key2)
		case TaskPending, TaskPreempted:
			t.state = TaskFailed
			t.term = TermCanceled
			s.releaseHold(key)
		}
	}
	j.state = JobCanceled
	s.logLocked(Decision{At: s.now, Kind: DecisionCancel, JobID: id,
		Detail: "job canceled; all tasks terminated and shares returned"})
	return nil
}

// Schedule 执行一个调度周期：按优先级+公平共享排序待调度作业，
// 对其全部待启动任务做整体放置；资源不足时尝试整体抢占严格更低优先级作业。
// 任何作业要么整体启动，要么不启动并给出可区分原因。
func (s *Scheduler) Schedule() (*ScheduleReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	report := &ScheduleReport{At: s.now, StartedTasks: map[string][]string{}}
	s.logLocked(Decision{At: s.now, Kind: DecisionSchedule, Detail: "schedule cycle begin"})
	for _, j := range s.pendingOrder() {
		// 全局运行任务数上限：本次需要全部待启动任务一起进入。
		if s.cfg.MaxRunningTasks > 0 && s.runningTaskCount()+int64(len(pendingTasks(j))) > int64(s.cfg.MaxRunningTasks) {
			s.appendUnschedulable(report, j, ReasonRunningTaskLimit, nil,
				"global running task cap %d reached", s.cfg.MaxRunningTasks)
			continue
		}
		// 配额预检：抢占不改变任何用户的 allocated 总量，故配额冲突直接拒绝。
		if reason, detail := s.checkStartQuota(j); reason != ReasonOK {
			s.appendUnschedulable(report, j, reason, nil, "%s", detail)
			continue
		}
		plan, reasons, ok := s.placeJob(j, nil)
		var victims map[*job]bool
		if !ok {
			var pr RejectReason
			var detail string
			plan, victims, pr, detail, reasons = s.preemptVictims(j)
			if plan == nil {
				s.appendUnschedulable(report, j, pr, reasons, "%s", detail)
				continue
			}
		}
		// 放置已在“预演”中成立：先结算抢占（回补份额），再整体启动。
		if len(victims) > 0 {
			s.applyPreemptions(victims, j, report)
		}
		names, err := s.startTasks(j, plan, s.now)
		if err != nil {
			// 抢占后仍超配额（不应发生，因为预演已校验）：整体不启动。
			s.appendUnschedulable(report, j, ReasonQuotaExceeded, reasons, "%s", err.Error())
			continue
		}
		report.StartedJobs = append(report.StartedJobs, j.spec.ID)
		report.StartedTasks[j.spec.ID] = names
	}
	s.logLocked(Decision{At: s.now, Kind: DecisionSchedule,
		Detail: summarizeReport(report)})
	return report, nil
}

// SetClock 设置模拟时钟（用于初始化时间原点；不得回拨到当前时间之前）。
func (s *Scheduler) SetClock(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t.After(s.now) || s.now.IsZero() {
		s.now = t
	}
}

// Tick 推进模拟时钟：自然完成给定任务（结算运行时并回收份额），
// 并刷新相关作业的聚合状态。节点失联故障在 SetNodeState 中即时处理。
func (s *Scheduler) Tick(now time.Time, completed []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.After(s.now) {
		s.now = now
	} else if !now.IsZero() {
		s.now = now
	}
	touched := map[*job]bool{}
	for _, key := range completed {
		j, t := s.findTaskLocked(key)
		if j == nil || t == nil || t.state != TaskRunning {
			continue // 未知/重复/非运行中键：忽略，绝不产生重复记账
		}
		s.settleTask(j, t, s.now, TermFinished)
		t.state = TaskFinished
		// 节点账本回补：与启动时的 commitNode 严格对称。
		if nd := s.nodes[t.node]; nd != nil {
			undoNodeCommit(nd, t.spec, placeOn{numaID: t.numaID, gpu: t.gpu})
			delete(nd.running, key)
		}
		t.node, t.gpu = "", ""
		t.numaID = t.spec.NUMAID
		s.release(key)
		s.logLocked(Decision{At: s.now, Kind: DecisionComplete, JobID: j.spec.ID,
			TaskID: t.spec.Name, Node: t.node,
			Detail: sprintf("finished; runtime=%s settled, share returned", t.runtime)})
		touched[j] = true
	}
	for j := range touched {
		s.recomputeJobState(j)
	}
}

// GetJob 返回作业只读视图（不存在返回 nil）。
func (s *Scheduler) GetJob(id string) *JobInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil
	}
	info := &JobInfo{
		ID: j.spec.ID, User: j.spec.User, Queue: j.spec.Queue,
		Priority: j.spec.Priority, State: j.state,
	}
	for _, t := range j.tasks {
		info.Tasks = append(info.Tasks, TaskInfo{
			Name: t.spec.Name, State: t.state, Node: t.node, NUMAID: t.numaID,
			GPU: t.gpu, Retries: t.retries, TermReason: t.term, Runtime: t.runtime,
		})
		switch t.state {
		case TaskPending:
			info.NumPending++
		case TaskPreempted:
			info.NumPreempted++
		case TaskRunning:
			info.NumRunning++
		case TaskFinished:
			info.NumFinished++
		case TaskFailed:
			info.NumFailed++
		}
	}
	return info
}

// Snapshot 返回当前资源核算快照（运行份额 + 预留份额分账列出）。
func (s *Scheduler) Snapshot() *UsageSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked(s.now)
}

// Decisions 返回有界决策日志的副本（按时间顺序）。
func (s *Scheduler) Decisions() []Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Decision, len(s.decisions))
	copy(out, s.decisions)
	return out
}

// ---- 内部辅助 ----

func (s *Scheduler) findTaskLocked(key string) (*job, *task) {
	id, name, ok := strings.Cut(key, "/")
	if !ok {
		return nil, nil
	}
	j, ok := s.jobs[id]
	if !ok {
		return nil, nil
	}
	for _, t := range j.tasks {
		if t.spec.Name == name {
			return j, t
		}
	}
	return j, nil
}

func (s *Scheduler) runningTaskCount() int64 {
	var n int64
	for _, nd := range s.nodes {
		n += int64(len(nd.running))
	}
	return n
}

func (s *Scheduler) appendUnschedulable(rpt *ScheduleReport, j *job, reason RejectReason,
	nodeReasons map[string][]FilterReason, format string, args ...any) {
	detail := sprintf(format, args...)
	rpt.Unschedulable = append(rpt.Unschedulable, UnschedulableJob{
		JobID: j.spec.ID, Reason: reason, NodeReasons: cloneReasons(nodeReasons), Detail: detail,
	})
	rs := summarizeReasons(nodeReasons)
	if rs == "no node-level reasons" {
		rs = ""
	} else {
		rs = rs + "; "
	}
	s.logLocked(Decision{At: s.now, Kind: DecisionUnschedulable, JobID: j.spec.ID,
		Reason: string(reason), Detail: rs + detail})
}

// applyPreemptions 结算被抢占作业：整作业中仍在运行的任务全部停止，
// 运行份额回补、等量预留份额登记，已耗运行时结算到任务与抢占记录。
func (s *Scheduler) applyPreemptions(victims map[*job]bool, winner *job, rpt *ScheduleReport) {
	vlist := make([]*job, 0, len(victims))
	for v := range victims {
		vlist = append(vlist, v)
	}
	sort.Slice(vlist, func(a, b int) bool { return vlist[a].spec.ID < vlist[b].spec.ID })
	for _, v := range vlist {
		rec := PreemptionRecord{PreemptedJobID: v.spec.ID, PreemptingJobID: winner.spec.ID}
		for _, t := range v.tasks {
			if t.state != TaskRunning {
				continue
			}
			key := s.taskKey(v.spec.ID, t.spec.Name)
			seg := s.settleTask(v, t, s.now, TermPreempted)
			t.state = TaskPreempted
			rec.PreemptedTasks = append(rec.PreemptedTasks, t.spec.Name)
			rec.Node = t.node
			rec.SettledRuntime += seg
			rec.ReturnedShare = rec.ReturnedShare.add(t.spec.Resources)
			nodeName := t.node
			// 节点账本立即回补（与完成对称），保证随后胜出者在干净账本上提交。
			if nd := s.nodes[t.node]; nd != nil {
				undoNodeCommit(nd, t.spec, placeOn{numaID: t.numaID, gpu: t.gpu})
				delete(nd.running, key)
			}
			t.node, t.gpu = "", ""
			t.numaID = t.spec.NUMAID
			s.release(key)                       // 用户/队列运行份额回补
			s.holdPreempt(key, t.spec.Resources) // 等量预留，防止超额扩容
			s.logLocked(Decision{At: s.now, Kind: DecisionPreempt, JobID: v.spec.ID,
				TaskID: t.spec.Name, Node: nodeName,
				Reason: string(TermPreempted),
				Detail: sprintf("preempted by %s; settled runtime=%s, share returned and held for restart",
					winner.spec.ID, seg),
			})
		}
		s.recomputeJobState(v)
		rpt.PreemptedJobs = append(rpt.PreemptedJobs, rec)
	}
}

func cloneNodeSpec(in NodeSpec) NodeSpec {
	out := in
	if in.Labels != nil {
		out.Labels = map[string]string{}
		for k, v := range in.Labels {
			out.Labels[k] = v
		}
	}
	if in.GPUs != nil {
		out.GPUs = append([]GPUDevice(nil), in.GPUs...)
	}
	if in.NUMA != nil {
		out.NUMA = append([]NUMAZone(nil), in.NUMA...)
	}
	return out
}

func cloneJobSpec(in JobSpec) JobSpec {
	out := in
	out.Tasks = append([]TaskSpec(nil), in.Tasks...)
	if in.Affinity.Required != nil {
		r := *in.Affinity.Required
		out.Affinity.Required = &r
	}
	if in.Affinity.Preferred != nil {
		p := *in.Affinity.Preferred
		out.Affinity.Preferred = &p
	}
	out.Affinity.AntiAffinityWith = append([]string(nil), in.Affinity.AntiAffinityWith...)
	return out
}

func cloneReasons(in map[string][]FilterReason) map[string][]FilterReason {
	if in == nil {
		return nil
	}
	out := make(map[string][]FilterReason, len(in))
	for k, v := range in {
		out[k] = append([]FilterReason(nil), v...)
	}
	return out
}
