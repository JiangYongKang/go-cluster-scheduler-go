package accounting

import (
	"testing"

	"github.com/highcumontoa/go-cluster-scheduler-go/pkg/api"
)

func TestLedgerConservationAndQuota(t *testing.T) {
	l := NewLedger()
	l.SetUserQuota("u", Quota{MilliCPU: 1000, MemoryMB: 1 << 20, GPUCount: 2})
	l.SetQueueQuota("q", Quota{MilliCPU: 2000, MemoryMB: 1 << 20})
	l.SetWeight("heavy", 3)

	r := api.Resources{MilliCPU: 400, MemoryMB: 128}
	if err := l.CanAllocate("u", "q", r); err != nil {
		t.Fatal(err)
	}
	l.Allocate("u", "q", r)
	// Over the user cap must be refused; allocation state unchanged.
	if err := l.CanAllocate("u", "q", api.Resources{MilliCPU: 700}); err == nil {
		t.Fatal("expected user quota error")
	}
	if l.AllocatedUser("u").MilliCPU != 400 {
		t.Fatalf("allocation mutated by failed check: %d", l.AllocatedUser("u").MilliCPU)
	}

	// Running accrual then a settle moves usage without changing total.
	l.Accrue("u", "q", api.Scale(r, 5))
	before := l.Snapshot()["u"].Usage.Total()
	l.Settle("u", "q", api.Scale(r, 5), "preempted")
	after := l.Snapshot()["u"].Usage
	if after.Total().MilliCPU != before.MilliCPU {
		t.Fatalf("settle changed total: %d -> %d", before.MilliCPU, after.Total().MilliCPU)
	}
	if after.Running.MilliCPU != 0 || after.Preempted.MilliCPU != 400*5 {
		t.Fatalf("settle split wrong: running=%d preempted=%d", after.Running.MilliCPU, after.Preempted.MilliCPU)
	}

	// Release reverses instantaneous allocation exactly.
	l.Release("u", "q", r)
	if !l.AllocatedUser("u").IsZero() {
		t.Fatalf("allocation not released: %+v", l.AllocatedUser("u"))
	}

	// Weighted fair share over accounts u(1), heavy(3), a(1): total weight 5.
	l.SetWeight("a", 1)
	fs := l.FairShare(api.Resources{}, nil)
	if fs["heavy"] != 0.6 {
		t.Fatalf("heavy share=%f want 0.6 (%+v)", fs["heavy"], fs)
	}
	if fs["u"] != 0.2 || fs["a"] != 0.2 {
		t.Fatalf("shares wrong: %+v", fs)
	}
}
