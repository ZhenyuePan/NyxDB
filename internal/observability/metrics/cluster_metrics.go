package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"nyxdb/internal/cluster"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ClusterCollector exposes cluster diagnostics as Prometheus metrics.
type ClusterCollector struct {
	term                prometheus.Gauge
	commitIndex         prometheus.Gauge
	appliedIndex        prometheus.Gauge
	lastRaftIndex       prometheus.Gauge
	entriesSinceSnap    prometheus.Gauge
	memberCount         prometheus.Gauge
	readTxnCount        prometheus.Gauge
	snapshotInProgress  prometheus.Gauge
	lastSnapshotUnix    prometheus.Gauge
	lastSnapshotAgeSecs prometheus.Gauge
}

// NewClusterCollector creates a collector registered on the provided registry (default if nil).
func NewClusterCollector(reg prometheus.Registerer, namespace string) *ClusterCollector {
	if namespace == "" {
		namespace = "nyxdb"
	}
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	builder := promauto.With(reg)
	return &ClusterCollector{
		term: builder.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cluster_term",
			Help:      "Current raft term observed by this node.",
		}),
		commitIndex: builder.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cluster_commit_index",
			Help:      "Latest committed raft index.",
		}),
		appliedIndex: builder.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cluster_applied_index",
			Help:      "Latest applied raft index.",
		}),
		lastRaftIndex: builder.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cluster_last_raft_index",
			Help:      "Latest raft index persisted in storage.",
		}),
		entriesSinceSnap: builder.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cluster_entries_since_snapshot",
			Help:      "Number of raft log entries generated since the last snapshot.",
		}),
		memberCount: builder.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cluster_member_count",
			Help:      "Number of known cluster members.",
		}),
		readTxnCount: builder.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cluster_read_txn_count",
			Help:      "Active read transactions tracked by the leader.",
		}),
		snapshotInProgress: builder.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cluster_snapshot_in_progress",
			Help:      "Whether a snapshot is currently running (1=yes, 0=no).",
		}),
		lastSnapshotUnix: builder.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cluster_last_snapshot_unix",
			Help:      "Unix timestamp of the last completed snapshot.",
		}),
		lastSnapshotAgeSecs: builder.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cluster_last_snapshot_age_seconds",
			Help:      "Age of the last snapshot in seconds.",
		}),
	}
}

// Observe updates metrics from the supplied diagnostics sample.
func (c *ClusterCollector) Observe(diag cluster.Diagnostics) {
	c.term.Set(float64(diag.Term))
	c.commitIndex.Set(float64(diag.CommittedIndex))
	c.appliedIndex.Set(float64(diag.AppliedIndex))
	c.lastRaftIndex.Set(float64(diag.LastRaftIndex))
	c.entriesSinceSnap.Set(float64(diag.EntriesSinceSnapshot))
	c.memberCount.Set(float64(diag.MemberCount))
	c.readTxnCount.Set(float64(diag.ReadTxnCount))
	if diag.SnapshotInProgress {
		c.snapshotInProgress.Set(1)
	} else {
		c.snapshotInProgress.Set(0)
	}
	if diag.LastSnapshotTime.IsZero() {
		c.lastSnapshotUnix.Set(0)
		c.lastSnapshotAgeSecs.Set(0)
	} else {
		c.lastSnapshotUnix.Set(float64(diag.LastSnapshotTime.Unix()))
		age := time.Since(diag.LastSnapshotTime).Seconds()
		if age < 0 {
			age = 0
		}
		c.lastSnapshotAgeSecs.Set(age)
	}
}

// StartServer serves Prometheus metrics on the provided address until the context is canceled.
func StartServer(ctx context.Context, addr string) error {
	if addr == "" {
		return fmt.Errorf("metrics address is empty")
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("metrics server error: %v\n", err)
		}
	}()

	return nil
}
