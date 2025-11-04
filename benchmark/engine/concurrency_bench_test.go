package benchmark

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	db "nyxdb/internal/layers/engine"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	benchValueSizes   = []int{16, 128, 1024, 4096}
	benchReadPercents = []int{0, 70, 100}
	benchThreads      = []int{1, 2, 4, 8, 16, 32}
	benchKeySpace     = 1 << 15 // 32k logical keys
)

// Benchmark_EngineConcurrent exercises the storage engine under varying
// concurrency levels, read/write ratios, and value sizes. Environment
// variables can be used to tweak engine options:
//
//	NYXDB_BENCH_SYNC_WRITES      (bool, default: options.SyncWrites)
//	NYXDB_BENCH_GROUP_COMMIT     (bool, default: options.GroupCommit.Enabled)
//	NYXDB_BENCH_BYTES_PER_SYNC   (int, default: options.BytesPerSync)
//	NYXDB_BENCH_INDEX            (btree|skiplist|sharded|art, default: DefaultOptions.IndexType)
//	NYXDB_BENCH_KEY_DIST         (uniform|zipf, default: uniform)
//
// Example:
//
//	NYXDB_BENCH_SYNC_WRITES=false NYXDB_BENCH_KEY_DIST=zipf \
//	  go test ./benchmark -run ^$ -bench Benchmark_EngineConcurrent -benchmem
func Benchmark_EngineConcurrent(b *testing.B) {
	keyDist := benchKeyDistribution()
	prevProcs := runtime.GOMAXPROCS(runtime.NumCPU())
	defer runtime.GOMAXPROCS(prevProcs)

	for _, valueSize := range benchValueSizes {
		valueSize := valueSize
		b.Run(fmt.Sprintf("value=%dB", valueSize), func(b *testing.B) {
			opts := loadBenchOptions(b.TempDir())
			database, err := db.Open(opts)
			if err != nil {
				b.Fatalf("open db: %v", err)
			}
			b.Cleanup(func() {
				if err := database.Close(); err != nil {
					b.Fatalf("close db: %v", err)
				}
			})

			seedDatabase(b, database, valueSize)

			for _, readPct := range benchReadPercents {
				readPct := readPct
				b.Run(fmt.Sprintf("read=%d%%", readPct), func(b *testing.B) {
					for _, threads := range benchThreads {
						threads := threads
						b.Run(fmt.Sprintf("threads=%d", threads), func(b *testing.B) {
							runConcurrentBenchmark(b, database, benchKeySpace, valueSize, readPct, threads, keyDist)
						})
					}
				})
			}
		})
	}
}

func loadBenchOptions(dir string) db.Options {
	opts := db.DefaultOptions
	opts.DirPath = dir

	if v, ok := lookupEnvBool("NYXDB_BENCH_SYNC_WRITES"); ok {
		opts.SyncWrites = v
	}
	if v, ok := lookupEnvBool("NYXDB_BENCH_GROUP_COMMIT"); ok {
		opts.GroupCommit.Enabled = v
	}
	if v, ok := lookupEnvUint("NYXDB_BENCH_BYTES_PER_SYNC"); ok {
		opts.BytesPerSync = uint(v)
	}
	if idx, ok := os.LookupEnv("NYXDB_BENCH_INDEX"); ok {
		switch strings.ToLower(idx) {
		case "btree":
			opts.IndexType = db.Btree
		case "skiplist":
			opts.IndexType = db.SkipList
		case "sharded":
			opts.IndexType = db.ShardedBtree
		case "art":
			opts.IndexType = db.AdaptiveRadix
		default:
			bad := fmt.Sprintf("unknown NYXDB_BENCH_INDEX=%s", idx)
			panic(bad)
		}
	}
	return opts
}

func benchKeyDistribution() string {
	if v := os.Getenv("NYXDB_BENCH_KEY_DIST"); v != "" {
		switch strings.ToLower(v) {
		case "uniform", "zipf":
			return strings.ToLower(v)
		default:
			panic(fmt.Sprintf("unknown NYXDB_BENCH_KEY_DIST=%s", v))
		}
	}
	return "uniform"
}

func seedDatabase(b *testing.B, database *db.DB, valueSize int) {
	b.Helper()
	value := make([]byte, valueSize)
	for i := 0; i < benchKeySpace; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		if err := database.Put(key, value); err != nil {
			b.Fatalf("seed put failed: %v", err)
		}
	}
}

func runConcurrentBenchmark(b *testing.B, database *db.DB, keyspace, valueSize, readPercent, threads int, keyDist string) {
	b.Helper()
	var operations uint64
	var wg sync.WaitGroup
	wg.Add(threads)

	b.ReportAllocs()
	b.SetBytes(int64(valueSize))
	b.ResetTimer()
	start := time.Now()

	for worker := 0; worker < threads; worker++ {
		worker := worker
		go func() {
			defer wg.Done()

			seed := time.Now().UnixNano() + int64(worker)<<32
			rng := rand.New(rand.NewSource(seed))
			var zipf *rand.Zipf
			if keyDist == "zipf" && keyspace > 1 {
				zipf = rand.NewZipf(rng, 1.1, 1, uint64(keyspace-1))
			}

			var keyBuf [8]byte
			value := make([]byte, valueSize)

			for {
				op := atomic.AddUint64(&operations, 1) - 1
				if op >= uint64(b.N) {
					return
				}

				var keyIdx uint64
				if zipf != nil {
					keyIdx = zipf.Uint64()
				} else {
					keyIdx = uint64(rng.Intn(keyspace))
				}
				binary.BigEndian.PutUint64(keyBuf[:], keyIdx)
				key := keyBuf[:]

				if readPercent > 0 && rng.Intn(100) < readPercent {
					if _, err := database.Get(key); err != nil && err != db.ErrKeyNotFound {
						panic(fmt.Sprintf("get failed: %v", err))
					}
				} else {
					if err := database.Put(key, value); err != nil {
						panic(fmt.Sprintf("put failed: %v", err))
					}
				}
			}
		}()
	}

	wg.Wait()
	b.StopTimer()

	elapsed := time.Since(start)
	if elapsed > 0 {
		ops := float64(b.N) / elapsed.Seconds()
		b.ReportMetric(ops, "ops/s")
	}
}

func lookupEnvBool(key string) (bool, bool) {
	val, ok := os.LookupEnv(key)
	if !ok {
		return false, false
	}
	v, err := strconv.ParseBool(val)
	if err != nil {
		panic(fmt.Sprintf("invalid bool for %s: %v", key, err))
	}
	return v, true
}

func lookupEnvUint(key string) (uint64, bool) {
	val, ok := os.LookupEnv(key)
	if !ok {
		return 0, false
	}
	v, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("invalid integer for %s: %v", key, err))
	}
	return v, true
}
