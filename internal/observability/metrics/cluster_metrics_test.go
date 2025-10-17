package metrics

import (
	"testing"
	"time"

	"nyxdb/internal/cluster"

	"github.com/prometheus/client_golang/prometheus"
)

func TestClusterCollectorObserve(t *testing.T) {
	reg := prometheus.NewRegistry()
	collector := NewClusterCollector(reg, "nyxdb_test")

	now := time.Now().Add(-10 * time.Second)
	diag := cluster.Diagnostics{
		Term:                 3,
		CommittedIndex:       42,
		AppliedIndex:         40,
		LastRaftIndex:        50,
		EntriesSinceSnapshot: 8,
		MemberCount:          3,
		ReadTxnCount:         1,
		SnapshotInProgress:   true,
		LastSnapshotTime:     now,
	}
	collector.Observe(diag)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	if len(mfs) == 0 {
		t.Fatalf("expected metrics to be registered")
	}
}
