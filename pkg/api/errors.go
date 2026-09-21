package api

import "fmt"

// ErrorKind categorizes scheduler failures so callers can distinguish causes.
type ErrorKind int

const (
	ErrInvalid ErrorKind = iota
	ErrQuotaExceeded
	ErrQueueQuotaExceeded
	ErrDecisionLimit
	ErrNotFound
	ErrTerminal
	ErrConflict
)

func (k ErrorKind) String() string {
	switch k {
	case ErrInvalid:
		return "Invalid"
	case ErrQuotaExceeded:
		return "UserQuotaExceeded"
	case ErrQueueQuotaExceeded:
		return "QueueQuotaExceeded"
	case ErrDecisionLimit:
		return "DecisionLimitExceeded"
	case ErrNotFound:
		return "NotFound"
	case ErrTerminal:
		return "Terminal"
	case ErrConflict:
		return "Conflict"
	default:
		return "Unknown"
	}
}

// SchedulerError attaches a stable machine-readable kind to a failure.
type SchedulerError struct {
	Kind   ErrorKind
	Detail string
}

func (e *SchedulerError) Error() string {
	return fmt.Sprintf("scheduler error (%s): %s", e.Kind, e.Detail)
}

// NewError builds a *SchedulerError.
func NewError(kind ErrorKind, format string, args ...any) error {
	return &SchedulerError{Kind: kind, Detail: fmt.Sprintf(format, args...)}
}

// AsError extracts a *SchedulerError from err.
func AsError(err error) (*SchedulerError, bool) {
	if err == nil {
		return nil, false
	}
	if e, ok := err.(*SchedulerError); ok {
		return e, true
	}
	return nil, false
}

// RejectReason explains why one candidate node was excluded for a task.
type RejectReason string

const (
	RejectNodeNotReady RejectReason = "NodeNotReady"
	RejectInsufficient RejectReason = "InsufficientResources"
	RejectGPUModel     RejectReason = "GPUModelMismatch"
	RejectGPUExhausted RejectReason = "GPUsExhausted"
	RejectNumaLocal    RejectReason = "NUMALocalityUnsatisfiable"
	RejectNodeAffinity RejectReason = "NodeAffinityMismatch"
	RejectAntiAffinity RejectReason = "AntiAffinityConflict"
)
