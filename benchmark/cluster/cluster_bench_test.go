package clusterbench

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"nyxdb/internal/cluster"
	db "nyxdb/internal/engine"
	rafttransport "nyxdb/internal/raft"
)

var (
	benchCluster *cluster.Cluster
	benchDir     string
	setupOnce    sync.Once
	prefillOnce  sync.Once
	keyCounter   uint64
	prefillKeys  = 1000
)

func TestMain(m *testing.M) {
	setupOnce.Do(func() {
		benchDir, _ = os.MkdirTemp("", "nyxdb-cluster-bench")
		opts := db.DefaultOptions
		opts.DirPath = benchDir
		opts.EnableDiagnostics = false
		opts.ClusterConfig = &db.ClusterOptions{
			ClusterMode:      true,
			NodeAddress:      "127.0.0.1:19001",
			ClusterAddresses: []string{"1@127.0.0.1:19001"},
		}

		engine, err := db.Open(opts)
		if err != nil {
			panic(fmt.Sprintf("open engine: %v", err))
		}
		cl, err := cluster.NewClusterWithTransport(1, opts, engine, rafttransport.NewNoopTransport())
		if err != nil {
			panic(fmt.Sprintf("new cluster: %v", err))
		}
		if err := cl.Start(); err != nil {
			panic(fmt.Sprintf("start cluster: %v", err))
		}
		benchCluster = cl
	})

	code := m.Run()

	if benchCluster != nil {
		_ = benchCluster.Stop()
	}
	if benchDir != "" {
		_ = os.RemoveAll(benchDir)
	}

	os.Exit(code)
}

func clusterInstance(b *testing.B) *cluster.Cluster {
	if benchCluster == nil {
		b.Fatal("cluster not initialized")
	}
	return benchCluster
}

func ensurePrefill(b *testing.B, cl *cluster.Cluster) {
	prefillOnce.Do(func() {
		rand.Seed(time.Now().UnixNano())
		for i := 0; i < prefillKeys; i++ {
			key := []byte(fmt.Sprintf("prefill-key-%d", i))
			val := []byte("value")
			if err := cl.Put(key, val); err != nil {
				b.Fatalf("prefill put: %v", err)
			}
		}
		time.Sleep(200 * time.Millisecond)
	})
}

func BenchmarkClusterPut(b *testing.B) {
	cl := clusterInstance(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := atomic.AddUint64(&keyCounter, 1)
		key := []byte(fmt.Sprintf("bench-key-%d", id))
		val := []byte("value")
		if err := cl.Put(key, val); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func BenchmarkClusterGet(b *testing.B) {
	cl := clusterInstance(b)
	ensurePrefill(b, cl)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("prefill-key-%d", i%prefillKeys))
		if _, err := cl.Get(key); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}
