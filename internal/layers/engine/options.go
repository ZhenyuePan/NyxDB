package db

import (
	"os"
	"time"
)

type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件的大小
	DataFileSize int64

	// 每次写数据是否持久化
	SyncWrites bool

	// 累计写到多少字节后进行持久化
	BytesPerSync uint

	// 索引类型
	IndexType IndexerType

	// 启动时是否使用 MMap 加载数据
	MMapAtStartup bool

	//	数据文件合并的阈值
	DataFileMergeRatio float32

	// 集群配置
	ClusterConfig *ClusterOptions

	// 是否启用诊断日志
	EnableDiagnostics bool

	// GroupCommit 控制 fsync 组提交行为。
	GroupCommit GroupCommitOptions
}

// ClusterOptions 集群配置选项
type ClusterOptions struct {
	// 是否启用集群模式
	ClusterMode bool

	// 集群节点地址
	ClusterAddresses []string

	// 当前节点地址
	NodeAddress string

	// 自动快照相关配置
	AutoSnapshot           bool
	SnapshotInterval       time.Duration
	SnapshotThreshold      uint64
	SnapshotCatchUpEntries uint64

	// SnapshotDirSizeThreshold is an optional threshold (in bytes) for
	// triggering snapshots based on on-disk size of the data directory.
	// If zero, this dimension is ignored.
	SnapshotDirSizeThreshold uint64

	// SnapshotMaxDuration bounds a single snapshot operation duration for
	// guard/diagnostics purposes. If zero, a default guard may be used.
	SnapshotMaxDuration time.Duration

	// SnapshotMinInterval enforces a minimum elapsed time between two
	// generated snapshots. If zero, defaults to half of SnapshotInterval.
	SnapshotMinInterval time.Duration

	// SnapshotMaxAppliedLag limits the difference between the latest raft
	// index and the applied index before allowing a snapshot. If zero, we
	// fall back to 2x catch-up entries.
	SnapshotMaxAppliedLag uint64
}

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	// 遍历前缀为指定值的 Key，默认为空
	Prefix []byte
	// 是否反向遍历，默认 false 是正向
	Reverse bool
}

// WriteBatchOptions 批量写配置项
type WriteBatchOptions struct {
	// 一个批次当中最大的数据量
	MaxBatchNum uint

	// 提交时是否 sync 持久化
	SyncWrites bool
}

// GroupCommitOptions 配置 fsync 组提交。
type GroupCommitOptions struct {
	// 是否启用组提交
	Enabled bool
	// 每次组提交聚合的最大提交数
	MaxBatchEntries int
	// 每次组提交聚合的最大字节数
	MaxBatchBytes int64
	// 组提交的最大等待时间
	MaxWait time.Duration
}

type IndexerType = int8

const (
	Btree IndexerType = iota
	SkipList
	AdaptiveRadix
	ShardedBtree
)

var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024, // 256MB
	SyncWrites:         false,
	BytesPerSync:       0,
	IndexType:          ShardedBtree,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
	ClusterConfig:      nil, // 默认不启用集群
	GroupCommit: GroupCommitOptions{
		Enabled:         true,
		MaxBatchEntries: 16,
		MaxBatchBytes:   1 << 20, // 1MB
		MaxWait:         2 * time.Millisecond,
	},
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
