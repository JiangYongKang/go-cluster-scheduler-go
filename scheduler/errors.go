package scheduler

import "fmt"

// 拒绝/失败原因常量，调用方可据 Reason 做可区分的错误归类。
const (
	ReasonQuotaExceeded       RejectReason = "QUOTA_EXCEEDED"
	ReasonQueueFull           RejectReason = "QUEUE_FULL"
	ReasonPendingLimit        RejectReason = "PENDING_LIMIT"
	ReasonInvalidJob          RejectReason = "INVALID_JOB"
	ReasonDuplicateJob        RejectReason = "DUPLICATE_JOB"
	ReasonJobNotFound         RejectReason = "JOB_NOT_FOUND"
	ReasonTaskNotFound        RejectReason = "TASK_NOT_FOUND"
	ReasonInsufficientRes     RejectReason = "INSUFFICIENT_RESOURCES"
	ReasonConstraintConflict  RejectReason = "CONSTRAINT_CONFLICT"
	ReasonPreemptionNotEnough RejectReason = "PREEMPTION_NOT_ENOUGH"
	ReasonNoPreemptableVictim RejectReason = "NO_PREEMPTABLE_VICTIM"
	ReasonNodeNotFound        RejectReason = "NODE_NOT_FOUND"
	ReasonNoCandidates        RejectReason = "NO_CANDIDATE_NODES"
	ReasonRunningTaskLimit    RejectReason = "RUNNING_TASK_LIMIT"
	ReasonNothingScheduled    RejectReason = "NOTHING_SCHEDULED"
)

// 节点过滤细分原因（用于可解释日志）。
type FilterReason string

const (
	FilterNodeNotReady    FilterReason = "NODE_NOT_READY"
	FilterLabelMismatch   FilterReason = "LABEL_MISMATCH"
	FilterAntiAffinity    FilterReason = "ANTI_AFFINITY"
	FilterCPU             FilterReason = "CPU_INSUFFICIENT"
	FilterMemory          FilterReason = "MEMORY_INSUFFICIENT"
	FilterGPUAbsent       FilterReason = "GPU_ABSENT"
	FilterGPUBusy         FilterReason = "GPU_BUSY"
	FilterGPUZoneMismatch FilterReason = "GPU_ZONE_MISMATCH"
	FilterNUMAAbsent      FilterReason = "NUMA_ZONE_ABSENT"
	FilterNUMAExhaust     FilterReason = "NUMA_ZONE_EXHAUSTED"
	FilterTaskCap         FilterReason = "NODE_TASK_CAP"
)

// 任务终态原因。
type TerminalReason string

const (
	TermFinished       TerminalReason = "FINISHED"
	TermCanceled       TerminalReason = "CANCELED"
	TermNodeLost       TerminalReason = "NODE_LOST"
	TermRetriesExhaust TerminalReason = "RETRIES_EXHAUSTED"
	TermPreempted      TerminalReason = "PREEMPTED"
)

// SchedError 携带可机器判别的原因码与可读详情。
type SchedError struct {
	Reason RejectReason
	Op     string
	Detail string
}

func (e *SchedError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s: %s", e.Op, e.Reason, e.Detail)
}

// IsReason 判断错误是否属于给定原因码。
func IsReason(err error, r RejectReason) bool {
	if err == nil {
		return false
	}
	if se, ok := err.(*SchedError); ok {
		return se.Reason == r
	}
	return false
}

func schedError(op string, r RejectReason, format string, args ...any) error {
	return &SchedError{Reason: r, Op: op, Detail: fmt.Sprintf(format, args...)}
}
