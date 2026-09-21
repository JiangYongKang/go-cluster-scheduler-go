package preemption

import (
	"testing"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
)

func TestSelectVictimsOrdering(t *testing.T) {
	cands := []TaskInfo{
		{TaskID: "p1", JobID: "j1", User: "u1", NodeName: "n", Res: api.Resources{MilliCPU: 1000}, Priority: 2, StartTick: 1, FairnessKey: 1},
		{TaskID: "p0", JobID: "j0", User: "u0", NodeName: "n", Res: api.Resources{MilliCPU: 1000}, Priority: 1, StartTick: 5, FairnessKey: 1},
		{TaskID: "p0b", JobID: "j0", User: "u0b", NodeName: "n", Res: api.Resources{MilliCPU: 1000}, Priority: 1, StartTick: 2, FairnessKey: 9},
		{TaskID: "same", JobID: "js", User: "me", NodeName: "n", Res: api.Resources{MilliCPU: 1000}, Priority: 1},
	}
	// Preemptor priority 5, user "me": lowest priority first; within prio 1
	// higher FairnessKey (p0b) goes first; own user excluded.
	vs := SelectVictims(cands, api.Resources{MilliCPU: 1500}, 5, "me", 0, "")
	if len(vs) != 2 || vs[0].TaskID != "p0b" || vs[1].TaskID != "p0" {
		t.Fatalf("order=%+v", vs)
	}
	// Cannot cover the demand -> nil.
	if vs := SelectVictims(cands, api.Resources{MilliCPU: 99999}, 5, "me", 0, ""); vs != nil {
		t.Fatalf("expected nil when insufficient, got %+v", vs)
	}
	// Equal priority is never preempted.
	if vs := SelectVictims(cands, api.Resources{MilliCPU: 1}, 1, "other", 0, ""); vs != nil {
		t.Fatalf("equal priority must not be eligible, got %+v", vs)
	}
	// Node filter restricts candidates.
	vs = SelectVictims(cands, api.Resources{MilliCPU: 1000}, 5, "me", 0, "other-node")
	if len(vs) != 0 {
		t.Fatalf("node filter failed: %+v", vs)
	}
}
