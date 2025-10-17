package benchmark

import (
	"bytes"
	"math/rand"
	db "nyxdb/internal/engine"
	"nyxdb/internal/utils"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var dbInstance *db.DB

func init() {
	// 初始化用于基准测试的存储引擎
	options := db.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-bench")
	options.DirPath = dir

	var err error
	dbInstance, err = db.Open(options)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := dbInstance.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := dbInstance.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := dbInstance.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != db.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		err := dbInstance.Delete(utils.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}
}

func Benchmark_ReadTxnSnapshot(b *testing.B) {
    key := []byte("benchmark-readtxn")
	currentVal := []byte("init-value")
	assert.Nil(b, dbInstance.Put(key, currentVal))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rt := dbInstance.BeginReadTxn()
		oldVal := append([]byte(nil), currentVal...)

		newVal := utils.RandomValue(64)
		err := dbInstance.Put(key, newVal)
		assert.Nil(b, err)

		val, err := rt.Get(key)
		if err != nil {
			b.Fatal(err)
		}
		if !bytes.Equal(val, oldVal) {
			b.Fatalf("snapshot read mismatch, expected %q got %q", oldVal, val)
		}
		rt.Close()
		currentVal = newVal
	}
}
