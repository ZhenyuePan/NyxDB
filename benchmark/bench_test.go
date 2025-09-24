package benchmark

import (
	"math/rand"
	"nyxdb/internel/db"
	"nyxdb/internel/utils"
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
