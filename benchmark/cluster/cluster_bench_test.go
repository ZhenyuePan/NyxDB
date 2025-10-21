package clusterbench

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.uber.org/zap"
	"nyxdb/internal/cluster"
	db "nyxdb/internal/layers/engine"
	rafttransport "nyxdb/internal/layers/raft/transport"
	regionpkg "nyxdb/internal/region"
)

const prefillKeyCount = 2000

var (
	singleCtx benchContext
	multiCtx  benchContext
)

type benchContext struct {
	name        string
	cluster     *cluster.Cluster
	dir         string
	multiRegion bool

	keyCounter  uint64
	prefillOnce sync.Once
}

func (ctx *benchContext) nextKey(id uint64) []byte {
	if !ctx.multiRegion {
		return []byte(fmt.Sprintf("bench-key-%d", id))
	}
	if id%2 == 0 {
		return []byte(fmt.Sprintf("a-key-%d", id))
	}
	return []byte(fmt.Sprintf("z-key-%d", id))
}

func (ctx *benchContext) prefill(b *testing.B) {
	ctx.prefillOnce.Do(func() {
		for i := 0; i < prefillKeyCount; i++ {
			key := ctx.nextKey(uint64(i))
			if err := ctx.cluster.Put(key, []byte("value")); err != nil {
				b.Fatalf("prefill put: %v", err)
			}
		}
		time.Sleep(200 * time.Millisecond)
	})
}

func (ctx *benchContext) stop() {
	if ctx.cluster != nil {
		_ = ctx.cluster.Stop()
	}
	if ctx.dir != "" {
		_ = os.RemoveAll(ctx.dir)
	}
}

func TestMain(m *testing.M) {
	log.SetOutput(io.Discard)
	zap.ReplaceGlobals(zap.NewNop())
	raft.SetLogger(&raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)})

	singleCtx = mustCreateContext("single", false, nil)
	multiCtx = mustCreateContext("multi", true, []regionpkg.KeyRange{
		{Start: []byte("a"), End: []byte("m")},
		{Start: []byte("m"), End: nil},
	})

	code := m.Run()

	singleCtx.stop()
	multiCtx.stop()

	os.Exit(code)
}

func mustCreateContext(name string, multi bool, ranges []regionpkg.KeyRange) benchContext {
	dir, err := os.MkdirTemp("", fmt.Sprintf("nyxdb-bench-%s", name))
	if err != nil {
		panic(err)
	}
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = false
	opts.GroupCommit.MaxWait = 0
	opts.ClusterConfig = &db.ClusterOptions{
		ClusterMode:      true,
		NodeAddress:      fmt.Sprintf("127.0.0.1:%d", 19000+len(name)),
		ClusterAddresses: []string{fmt.Sprintf("1@127.0.0.1:%d", 19000+len(name))},
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
	waitForLeader(cl)
	for _, rg := range ranges {
		if _, err := cl.CreateStaticRegion(rg); err != nil {
			panic(fmt.Sprintf("create region %+v: %v", rg, err))
		}
		waitForLeader(cl)
	}

	return benchContext{
		name:        name,
		cluster:     cl,
		dir:         dir,
		multiRegion: multi,
	}
}

func waitForLeader(cl *cluster.Cluster) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		if cl.IsLeader() {
			return
		}
		if time.Now().After(deadline) {
			panic("cluster leader election timeout")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func benchmarkClusterPut(b *testing.B, ctx *benchContext) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := atomic.AddUint64(&ctx.keyCounter, 1)
		key := ctx.nextKey(id)
		if err := ctx.cluster.Put(key, []byte("value")); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

func benchmarkClusterGet(b *testing.B, ctx *benchContext) {
	ctx.prefill(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := ctx.nextKey(uint64(i % prefillKeyCount))
		if _, err := ctx.cluster.Get(key); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkClusterPutSingleRegion(b *testing.B) {
	benchmarkClusterPut(b, &singleCtx)
}

func BenchmarkClusterPutMultiRegion(b *testing.B) {
	benchmarkClusterPut(b, &multiCtx)
}

func BenchmarkClusterGetSingleRegion(b *testing.B) {
	benchmarkClusterGet(b, &singleCtx)
}

func BenchmarkClusterGetMultiRegion(b *testing.B) {
	benchmarkClusterGet(b, &multiCtx)
}
