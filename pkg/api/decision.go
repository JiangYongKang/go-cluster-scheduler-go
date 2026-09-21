package api

import "fmt"

// DecisionAction is the kind of action recorded for one task in a tick.
type DecisionAction string

const (
	DecisionPlaced    DecisionAction = "Placed"
	DecisionFinished  DecisionAction = "Finished"
	DecisionPreempted DecisionAction = "Preempted"
	DecisionRequeued  DecisionAction = "Requeued"
	DecisionFailed    DecisionAction = "Failed"
	DecisionLost      DecisionAction = "Lost"
	DecisionRejected  DecisionAction = "Rejected"
	DecisionDeferred  DecisionAction = "Deferred"
	DecisionCanceled  DecisionAction = "Canceled"
)

// TaskDecision is one explainable scheduling outcome.
type TaskDecision struct {
	Tick     int64                   `json:"tick"`
	JobID    string                  `json:"job_id"`
	TaskID   string                  `json:"task_id"`
	Action   DecisionAction          `json:"action"`
	NodeName string                  `json:"node_name,omitempty"`
	Reason   string                  `json:"reason,omitempty"`
	Rejects  map[string]RejectReason `json:"node_rejects,omitempty"`
}

func (d TaskDecision) String() string {
	if d.NodeName != "" {
		return fmt.Sprintf("tick=%d %s/%s %s node=%s reason=%s",
			d.Tick, d.JobID, d.TaskID, d.Action, d.NodeName, d.Reason)
	}
	return fmt.Sprintf("tick=%d %s/%s %s reason=%s",
		d.Tick, d.JobID, d.TaskID, d.Action, d.Reason)
}

// TickResult summarizes one scheduler cycle.
type TickResult struct {
	Tick      int64          `json:"tick"`
	Placed    []TaskDecision `json:"placed,omitempty"`
	Finished  []TaskDecision `json:"finished,omitempty"`
	Preempted []TaskDecision `json:"preempted,omitempty"`
	Requeued  []TaskDecision `json:"requeued,omitempty"`
	Failed    []TaskDecision `json:"failed,omitempty"`
	Lost      []TaskDecision `json:"lost,omitempty"`
	Rejected  []TaskDecision `json:"rejected,omitempty"`
	Deferred  []TaskDecision `json:"deferred,omitempty"`
	Canceled  []TaskDecision `json:"canceled,omitempty"`
}
