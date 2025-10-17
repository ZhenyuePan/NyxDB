package cluster

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	db "nyxdb/internal/engine"
	raftnode "nyxdb/internal/node"
	rafttransport "nyxdb/internal/raft"
	regionpkg "nyxdb/internal/region"
	replication "nyxdb/internal/replication"
	utils "nyxdb/internal/utils"
	api "nyxdb/pkg/api"

	etcdraft "go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	protobuf "google.golang.org/protobuf/proto"
)

const (
	defaultSnapshotInterval       = 5 * time.Minute
	defaultSnapshotThreshold      = 1024
	defaultSnapshotCatchUpEntries = 64
	defaultDiagnosticsInterval    = 30 * time.Second
)

// Cluster 代表一个分布式存储集群
type Cluster struct {
	nodeID    uint64
	options   db.Options
	db        *db.DB
	raftNode  *raftnode.Node
	transport rafttransport.Transport
	storage   *RaftStorage

	// 集群成员管理
	members     map[uint64]string // nodeID -> address
	membersMu   sync.RWMutex
	memberStore *memberStore

	commitMu     sync.Mutex
	nextCommitTs uint64
	applier      *replication.Applier

	// 数据提交通道
	commitC chan *raftnode.Commit
	errorC  chan error

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 读事务
	readTxnMu  sync.RWMutex
	readTxns   map[string]*readTxnEntry
	readTxnTTL time.Duration

	// 快照
	snapshotEnabled        bool
	snapshotInterval       time.Duration
	snapshotMinEntries     uint64
	snapshotCatchUpEntries uint64
	snapshotMinInterval    time.Duration
	snapshotMaxAppliedLag  uint64
	snapshotMu             sync.Mutex
	snapshotInProgress     bool
	snapshotStartAt        time.Time
	snapshotMaxDuration    time.Duration
	lastSnapshotIndex      uint64
	lastSnapshotDuration   time.Duration
	lastSnapshotSizeBytes  uint64

	// Leader change observation to avoid snapshot jitter
	lastLeader         uint64
	lastLeaderChangeAt time.Time

	// Last snapshot completion time for frequency guard
	lastSnapshotTime time.Time

	diagnosticsEnabled   bool
	diagnosticsInterval  time.Duration
	diagnosticsMu        sync.RWMutex
	diagnosticsObservers []func(Diagnostics)

	regionMu       sync.RWMutex
	regions        map[regionpkg.ID]*regionpkg.Region
	regionReplicas map[regionpkg.ID]*RegionReplica
}

type peerAddress struct {
	id   uint64
	addr string
}

func parsePeerAddresses(entries []string) []peerAddress {
	peers := make([]peerAddress, 0, len(entries))
	for idx, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		var (
			id   uint64
			addr string
			err  error
		)
		if strings.Contains(entry, "@") {
			parts := strings.SplitN(entry, "@", 2)
			id, err = strconv.ParseUint(parts[0], 10, 64)
			if err == nil {
				addr = parts[1]
			}
		} else if strings.Contains(entry, "=") {
			parts := strings.SplitN(entry, "=", 2)
			id, err = strconv.ParseUint(parts[0], 10, 64)
			if err == nil {
				addr = parts[1]
			}
		} else {
			id = uint64(idx + 1)
			addr = entry
		}
		if err != nil || addr == "" {
			continue
		}
		peers = append(peers, peerAddress{id: id, addr: addr})
	}
	return peers
}

// NewCluster 创建一个新的集群实例
func NewCluster(nodeID uint64, options db.Options, database *db.DB) (*Cluster, error) {
	return NewClusterWithTransport(nodeID, options, database, nil)
}

func NewClusterWithTransport(nodeID uint64, options db.Options, database *db.DB, transport rafttransport.Transport) (*Cluster, error) {
	ctx, cancel := context.WithCancel(context.Background())

	if transport == nil {
		if options.ClusterConfig != nil && options.ClusterConfig.ClusterMode {
			transport = rafttransport.NewGRPCTransport(nodeID, nil)
		} else {
			transport = rafttransport.NewDefaultTransport()
		}
	}

	cluster := &Cluster{
		nodeID:              nodeID,
		options:             options,
		db:                  database,
		members:             make(map[uint64]string),
		commitC:             make(chan *raftnode.Commit, 100),
		errorC:              make(chan error, 100),
		ctx:                 ctx,
		cancel:              cancel,
		nextCommitTs:        database.MaxCommittedTs(),
		transport:           transport,
		readTxns:            make(map[string]*readTxnEntry),
		readTxnTTL:          time.Minute,
		diagnosticsEnabled:  options.EnableDiagnostics,
		diagnosticsInterval: defaultDiagnosticsInterval,
		regions:             make(map[regionpkg.ID]*regionpkg.Region),
		regionReplicas:      make(map[regionpkg.ID]*RegionReplica),
	}

	if options.ClusterConfig != nil {
		cc := options.ClusterConfig
		cluster.snapshotEnabled = cc.AutoSnapshot
		if cc.SnapshotInterval > 0 {
			cluster.snapshotInterval = cc.SnapshotInterval
		} else {
			cluster.snapshotInterval = defaultSnapshotInterval
		}
		if cc.SnapshotThreshold > 0 {
			cluster.snapshotMinEntries = cc.SnapshotThreshold
		} else {
			cluster.snapshotMinEntries = defaultSnapshotThreshold
		}
		if cc.SnapshotCatchUpEntries > 0 {
			cluster.snapshotCatchUpEntries = cc.SnapshotCatchUpEntries
		} else {
			cluster.snapshotCatchUpEntries = defaultSnapshotCatchUpEntries
		}
		if cc.SnapshotMaxDuration > 0 {
			cluster.snapshotMaxDuration = cc.SnapshotMaxDuration
		} else {
			cluster.snapshotMaxDuration = 2 * time.Minute
		}
		if cc.SnapshotMinInterval > 0 {
			cluster.snapshotMinInterval = cc.SnapshotMinInterval
		} else {
			cluster.snapshotMinInterval = cluster.snapshotInterval / 2
			if cluster.snapshotMinInterval <= 0 {
				cluster.snapshotMinInterval = time.Minute
			}
		}
		if cc.SnapshotMaxAppliedLag > 0 {
			cluster.snapshotMaxAppliedLag = cc.SnapshotMaxAppliedLag
		} else if cluster.snapshotCatchUpEntries > 0 {
			cluster.snapshotMaxAppliedLag = 2 * cluster.snapshotCatchUpEntries
		}
	}
	if cluster.snapshotInterval <= 0 {
		cluster.snapshotInterval = defaultSnapshotInterval
	}
	if cluster.snapshotMinInterval <= 0 {
		cluster.snapshotMinInterval = cluster.snapshotInterval / 2
		if cluster.snapshotMinInterval <= 0 {
			cluster.snapshotMinInterval = time.Minute
		}
	}
	if cluster.snapshotCatchUpEntries == 0 {
		cluster.snapshotCatchUpEntries = defaultSnapshotCatchUpEntries
	}
	if cluster.snapshotMaxAppliedLag == 0 {
		cluster.snapshotMaxAppliedLag = 2 * cluster.snapshotCatchUpEntries
	}
	if cluster.snapshotMaxDuration <= 0 {
		cluster.snapshotMaxDuration = 2 * time.Minute
	}

	cluster.applier = replication.NewApplier(database)
	cluster.initDefaultRegions()

	memberDir := filepath.Join(options.DirPath, "cluster")
	store, err := newMemberStore(memberDir)
	if err != nil {
		cancel()
		return nil, err
	}
	cluster.memberStore = store

	if err := cluster.restoreMembers(); err != nil {
		cancel()
		return nil, err
	}

	if options.ClusterConfig != nil && options.ClusterConfig.NodeAddress != "" {
		cluster.membersMu.Lock()
		if cluster.members[nodeID] == "" {
			cluster.members[nodeID] = options.ClusterConfig.NodeAddress
		}
		cluster.membersMu.Unlock()
	}

	storage, err := NewRaftStorage(regionRaftDir(options.DirPath, defaultRegionID))
	if err != nil {
		cancel()
		return nil, err
	}
	cluster.storage = storage
	if snap, err := cluster.storage.Snapshot(); err == nil {
		cluster.lastSnapshotIndex = snap.Metadata.Index
	}

	// 初始化RAFT节点
	raftConfig := &raftnode.NodeConfig{
		ID:            nodeID,
		Cluster:       cluster.buildRaftPeers(),
		Storage:       storage,
		Transport:     cluster.transport,
		ElectionTick:  10,
		HeartbeatTick: 1,
	}

	if err := cluster.persistMembers(); err != nil {
		cancel()
		return nil, err
	}

	cluster.raftNode = raftnode.NewNode(raftConfig)
	cluster.registerReplica(&RegionReplica{
		Region:  cluster.regions[defaultRegionID],
		Node:    cluster.raftNode,
		Storage: storage,
	})

	return cluster, nil
}

// Start 启动集群
func (c *Cluster) Start() error {
	// 启动RAFT节点
	c.raftNode.Start(c.commitC, c.errorC)

	// 启动后台处理协程
	c.wg.Add(2)
	go c.handleCommits()
	go c.handleErrors()
	c.wg.Add(1)
	go c.runReadTxnCleaner()
	if c.snapshotEnabled && c.snapshotInterval > 0 {
		c.wg.Add(1)
		go c.runAutoSnapshot()
	}
	if c.diagnosticsEnabled || c.hasDiagnosticsObserver() {
		if c.diagnosticsInterval <= 0 {
			c.diagnosticsInterval = defaultDiagnosticsInterval
		}
		c.wg.Add(1)
		go c.runDiagnostics()
	}

	return nil
}

// Stop 停止集群
func (c *Cluster) Stop() error {
	c.cancel()
	c.wg.Wait()

	if c.raftNode != nil {
		c.raftNode.Stop()
	}

	return c.db.Close()
}

// LeaderAddress returns the best-known leader address, or empty string if unknown.
func (c *Cluster) LeaderAddress() string {
	if c.raftNode == nil {
		return ""
	}
	status := c.raftNode.Status()
	leaderID := status.Lead
	if leaderID == 0 {
		return ""
	}
	c.membersMu.RLock()
	addr := c.members[leaderID]
	c.membersMu.RUnlock()
	return addr
}

// Put 通过RAFT集群存储数据
func (c *Cluster) Put(key, value []byte) error {
	cmd := &replication.Command{
		CommitTs: c.allocateCommitTs(),
		Operations: []replication.Operation{
			{Key: append([]byte(nil), key...), Value: append([]byte(nil), value...), Type: replication.OpPut},
		},
	}

	data, err := cmd.Marshal()
	if err != nil {
		return err
	}

	// 通过RAFT提交命令
	return c.raftNode.Propose(data)
}

// Delete 通过RAFT集群删除数据
func (c *Cluster) Delete(key []byte) error {
	cmd := &replication.Command{
		CommitTs: c.allocateCommitTs(),
		Operations: []replication.Operation{
			{Key: append([]byte(nil), key...), Type: replication.OpDelete},
		},
	}

	data, err := cmd.Marshal()
	if err != nil {
		return err
	}

	// 通过RAFT提交命令
	return c.raftNode.Propose(data)
}

func (c *Cluster) RaftNode() *raftnode.Node {
	return c.raftNode
}

// Get 从本地数据库获取数据
func (c *Cluster) Get(key []byte) ([]byte, error) {
	// 读操作不需要通过RAFT，直接从本地数据库获取
	return c.db.Get(key)
}

// GetLinearizable 执行线性一致读
func (c *Cluster) GetLinearizable(ctx context.Context, key []byte) ([]byte, error) {
	if !c.IsLeader() {
		leaderAddr := c.LeaderAddress()
		if leaderAddr != "" {
			return nil, fmt.Errorf("%w: leader=%s", ErrNotLeader, leaderAddr)
		}
		return nil, ErrNotLeader
	}
	index, err := c.raftNode.ReadIndex(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.raftNode.WaitApplied(ctx, index); err != nil {
		return nil, err
	}
	return c.db.Get(key)
}

// BeginReadTxn 开启快照读事务并返回句柄与快照时间戳
func (c *Cluster) BeginReadTxn() ([]byte, uint64, error) {
	txn := c.db.BeginReadTxn()

	handle, err := c.registerReadTxn(txn)
	if err != nil {
		txn.Close()
		return nil, 0, err
	}
	return handle, txn.ReadTs(), nil
}

// ReadTxnGet 在指定读事务下读取数据
func (c *Cluster) ReadTxnGet(handle []byte, key []byte) ([]byte, bool, error) {
	txn, release, ok := c.borrowReadTxn(handle)
	if !ok {
		return nil, false, ErrReadTxnNotFound
	}
	defer release()

	val, err := txn.Get(key)
	if err != nil {
		if errors.Is(err, db.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return val, true, nil
}

// EndReadTxn 结束读事务
func (c *Cluster) EndReadTxn(handle []byte) error {
	txn := c.deregisterReadTxn(handle)
	if txn == nil {
		return ErrReadTxnNotFound
	}
	txn.Close()
	return nil
}

// IsLeader 检查当前节点是否为Leader
func (c *Cluster) IsLeader() bool {
	return c.raftNode.IsLeader()
}

// AddMember 添加新成员到集群
func (c *Cluster) AddMember(nodeID uint64, address string) error {
	// 构造配置变更命令
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  nodeID,
		Context: []byte(address),
	}

	// 通过RAFT提交配置变更
	return c.raftNode.ProposeConfChange(cc)
}

// RemoveMember 从集群中移除成员
func (c *Cluster) RemoveMember(nodeID uint64) error {
	// 构造配置变更命令
	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: nodeID,
	}

	// 通过RAFT提交配置变更
	return c.raftNode.ProposeConfChange(cc)
}

// Members 获取集群成员列表
func (c *Cluster) Members() map[uint64]string {
	c.membersMu.RLock()
	defer c.membersMu.RUnlock()

	members := make(map[uint64]string)
	for id, addr := range c.members {
		members[id] = addr
	}

	return members
}

// handleCommits 处理RAFT提交的数据
func (c *Cluster) handleCommits() {
	defer c.wg.Done()

	for {
		select {
		case commit := <-c.commitC:
			if commit == nil {
				continue
			}

			if commit.Snapshot != nil {
				if err := c.applySnapshot(commit.Snapshot); err != nil {
					fmt.Printf("failed to apply snapshot: %v\n", err)
				}
				continue
			}

			if commit.ConfChange != nil {
				c.applyConfChange(commit.ConfChange, commit.ConfState)
				continue
			}

			if len(commit.Data) == 0 {
				continue
			}

			// 解析命令
			ts, err := c.applier.Apply(commit.Data)
			if err != nil {
				fmt.Printf("failed to apply commit: %v\n", err)
				continue
			}
			if ts > 0 {
				c.observeCommitTs(ts)
			}

		case <-c.ctx.Done():
			return
		}
	}
}

// handleErrors 处理RAFT错误
func (c *Cluster) handleErrors() {
	defer c.wg.Done()

	for {
		select {
		case err := <-c.errorC:
			if err != nil {
				fmt.Printf("RAFT error: %v\n", err)
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// buildRaftPeers 根据配置构建RAFT peers
func (c *Cluster) buildRaftPeers() []etcdraft.Peer {
	c.membersMu.Lock()
	defer c.membersMu.Unlock()

	if cfg := c.options.ClusterConfig; cfg != nil && cfg.ClusterMode {
		for _, p := range parsePeerAddresses(cfg.ClusterAddresses) {
			if _, exists := c.members[p.id]; !exists || c.members[p.id] == "" {
				c.members[p.id] = p.addr
			}
		}
	}

	peers := make([]etcdraft.Peer, 0, len(c.members))
	for id, addr := range c.members {
		peers = append(peers, etcdraft.Peer{ID: id})
		_ = c.transport.AddMember(id, []string{addr})
	}
	return peers
}

func (c *Cluster) allocateCommitTs() uint64 {
	c.commitMu.Lock()
	defer c.commitMu.Unlock()
	c.nextCommitTs++
	return c.nextCommitTs
}

func (c *Cluster) observeCommitTs(ts uint64) {
	c.commitMu.Lock()
	if ts > c.nextCommitTs {
		c.nextCommitTs = ts
	}
	c.commitMu.Unlock()
}

// TriggerMerge 启动合并过程
func (c *Cluster) TriggerMerge(force bool) error {
	return c.db.MergeWithOptions(db.MergeOptions{
		Force:              force,
		DiagnosticsContext: "admin-trigger",
	})
}

func (c *Cluster) TriggerSnapshot(force bool) error {
	if c.storage == nil {
		return fmt.Errorf("raft storage not initialized")
	}
	if !force && !c.IsLeader() {
		leader := c.LeaderAddress()
		if leader != "" {
			return fmt.Errorf("%w: leader=%s", ErrNotLeader, leader)
		}
		return ErrNotLeader
	}
	now := time.Now()
	c.snapshotMu.Lock()
	if c.snapshotInProgress {
		// timeout guard
		if !c.snapshotStartAt.IsZero() && c.snapshotMaxDuration > 0 && now.Sub(c.snapshotStartAt) > c.snapshotMaxDuration {
			fmt.Printf("snapshot timeout exceeded (>%s), clearing in-progress flag\n", c.snapshotMaxDuration)
			c.snapshotInProgress = false
		} else {
			c.snapshotMu.Unlock()
			return ErrSnapshotInProgress
		}
	}
	c.snapshotInProgress = true
	c.snapshotStartAt = now
	c.snapshotMu.Unlock()
	defer func() {
		c.snapshotMu.Lock()
		c.snapshotInProgress = false
		c.snapshotStartAt = time.Time{}
		c.snapshotMu.Unlock()
	}()

	lastIndex, err := c.storage.LastIndex()
	if err != nil {
		return fmt.Errorf("snapshot last index: %w", err)
	}
	entriesSince := lastIndex - c.lastSnapshotIndex
	if !force {
		if c.snapshotMinEntries > 0 && entriesSince < c.snapshotMinEntries {
			return ErrSnapshotNotNeeded
		}
		if lastIndex <= c.lastSnapshotIndex {
			return ErrSnapshotNotNeeded
		}
	}
	// Guard: avoid too-frequent snapshots
	if !force && !c.lastSnapshotTime.IsZero() && c.snapshotMinInterval > 0 {
		if time.Since(c.lastSnapshotTime) < c.snapshotMinInterval {
			return ErrSnapshotNotNeeded
		}
	}
	// Guard: avoid jitter shortly after leader changes
	if !force && !c.lastLeaderChangeAt.IsZero() && time.Since(c.lastLeaderChangeAt) < 10*time.Second {
		return ErrSnapshotNotNeeded
	}

	// Guard: size-based triggering (skip if under threshold)
	if !force && c.options.ClusterConfig != nil && c.options.ClusterConfig.SnapshotDirSizeThreshold > 0 {
		if dirSize, err := utils.DirSize(c.options.DirPath); err == nil {
			if uint64(dirSize) < c.options.ClusterConfig.SnapshotDirSizeThreshold {
				return ErrSnapshotNotNeeded
			}
		} else {
			fmt.Printf("warn: failed to stat dir size: %v\n", err)
		}
	}
	index := c.raftNode.AppliedIndex()
	if index == 0 {
		return ErrSnapshotNotNeeded
	}
	if !force && index <= c.lastSnapshotIndex {
		return ErrSnapshotNotNeeded
	}
	// Guard: if applied is far behind last, wait a bit
	if !force && lastIndex > index {
		lag := lastIndex - index
		maxLag := c.snapshotMaxAppliedLag
		if maxLag == 0 {
			maxLag = 2 * c.snapshotCatchUpEntries
			if maxLag == 0 {
				maxLag = 2 * defaultSnapshotCatchUpEntries
			}
		}
		if lag > maxLag {
			return ErrSnapshotNotNeeded
		}
	}

	st := c.raftNode.Status()
	fmt.Printf("snapshot start: entriesSince=%d applied=%d lastIndex=%d term=%d lead=%d\n", entriesSince, index, lastIndex, st.Term, st.Lead)
	backup, err := c.db.CreateSnapshot()
	if err != nil {
		return fmt.Errorf("create db snapshot: %w", err)
	}
	payload := &api.SnapshotPayload{
		Backup:       backup,
		NextCommitTs: c.nextCommitTs,
	}
	data, err := protobuf.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal snapshot payload: %w", err)
	}
	conf := c.storage.ConfState()
	newSnap, err := c.storage.CreateSnapshot(index, data, &conf)
	if err != nil {
		if errors.Is(err, etcdraft.ErrSnapOutOfDate) {
			return ErrSnapshotNotNeeded
		}
		return fmt.Errorf("persist snapshot: %w", err)
	}
	compactIndex := index
	if c.snapshotCatchUpEntries > 0 && compactIndex > c.snapshotCatchUpEntries {
		compactIndex = index - c.snapshotCatchUpEntries
	}
	if err := c.storage.Compact(compactIndex); err != nil && !errors.Is(err, etcdraft.ErrCompacted) {
		return fmt.Errorf("compact log: %w", err)
	}
	finish := time.Now()
	elapsed := finish.Sub(now)
	payloadSize := len(payload.Backup)
	c.lastSnapshotIndex = newSnap.Metadata.Index
	c.lastSnapshotTime = finish
	c.lastSnapshotDuration = elapsed
	c.lastSnapshotSizeBytes = uint64(payloadSize)
	fmt.Printf("snapshot complete: index=%d payloadSize=%d compactTo=%d elapsed=%s\n", newSnap.Metadata.Index, payloadSize, compactIndex, elapsed)
	return nil
}

func (c *Cluster) applyConfChange(cc *raftpb.ConfChange, cs *raftpb.ConfState) {
	switch cc.Type {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		addr := string(cc.Context)
		c.membersMu.Lock()
		c.members[cc.NodeID] = addr
		c.membersMu.Unlock()
		_ = c.transport.AddMember(cc.NodeID, []string{addr})
	case raftpb.ConfChangeRemoveNode:
		c.membersMu.Lock()
		delete(c.members, cc.NodeID)
		c.membersMu.Unlock()
		_ = c.transport.RemoveMember(cc.NodeID)
	case raftpb.ConfChangeUpdateNode:
		addr := string(cc.Context)
		c.membersMu.Lock()
		c.members[cc.NodeID] = addr
		c.membersMu.Unlock()
		_ = c.transport.AddMember(cc.NodeID, []string{addr})
	}
	if err := c.persistMembers(); err != nil {
		fmt.Printf("failed to persist members: %v\n", err)
	}
	if cs != nil && c.storage != nil {
		if err := c.storage.SetConfState(cs); err != nil {
			fmt.Printf("failed to update raft conf state: %v\n", err)
		}
	}
}

func (c *Cluster) applySnapshot(snapshot *raftpb.Snapshot) error {
	if snapshot == nil || len(snapshot.Data) == 0 {
		return nil
	}
	payload := &api.SnapshotPayload{}
	if err := protobuf.Unmarshal(snapshot.Data, payload); err != nil {
		return fmt.Errorf("decode snapshot payload: %w", err)
	}
	if err := c.restoreDatabaseFromSnapshot(payload.GetBackup()); err != nil {
		return err
	}
	if payload.GetNextCommitTs() > 0 {
		c.observeCommitTs(payload.GetNextCommitTs())
	}
	c.lastSnapshotIndex = snapshot.Metadata.Index
	return nil
}

func (c *Cluster) restoreDatabaseFromSnapshot(backup []byte) error {
	if len(backup) == 0 {
		return fmt.Errorf("snapshot payload is empty")
	}
	if err := c.db.Close(); err != nil {
		return fmt.Errorf("close db before restore: %w", err)
	}
	preserve := map[string]struct{}{
		"raft":    {},
		"regions": {},
		"cluster": {},
		"flock":   {},
	}
	if err := utils.ClearDirExcept(c.options.DirPath, preserve); err != nil {
		return fmt.Errorf("clear data dir: %w", err)
	}
	if err := utils.UntarGz(backup, c.options.DirPath); err != nil {
		return fmt.Errorf("restore snapshot files: %w", err)
	}
	newDB, err := db.Open(c.options)
	if err != nil {
		return fmt.Errorf("reopen db: %w", err)
	}
	c.db = newDB
	c.applier = replication.NewApplier(newDB)
	return nil
}

func (c *Cluster) restoreMembers() error {
	if c.memberStore == nil {
		return nil
	}
	stored, err := c.memberStore.Load()
	if err != nil {
		return err
	}
	c.membersMu.Lock()
	for id, addr := range stored {
		c.members[id] = addr
	}
	c.membersMu.Unlock()
	return nil
}

func (c *Cluster) persistMembers() error {
	if c.memberStore == nil {
		return nil
	}
	c.membersMu.RLock()
	snapshot := make(map[uint64]string, len(c.members))
	for id, addr := range c.members {
		snapshot[id] = addr
	}
	c.membersMu.RUnlock()
	return c.memberStore.Save(snapshot)
}

var (
	ErrReadTxnNotFound    = errors.New("cluster: read transaction not found")
	ErrNotLeader          = errors.New("cluster: not leader")
	ErrSnapshotNotNeeded  = errors.New("cluster: snapshot not needed")
	ErrSnapshotInProgress = errors.New("cluster: snapshot in progress")
)

// Diagnostics captures a point-in-time view of cluster metrics for observability.
type Diagnostics struct {
	LeaderID              uint64
	Term                  uint64
	AppliedIndex          uint64
	CommittedIndex        uint64
	LastRaftIndex         uint64
	LastSnapshotIndex     uint64
	EntriesSinceSnapshot  uint64
	SnapshotInProgress    bool
	SnapshotStartTime     time.Time
	LastSnapshotTime      time.Time
	ReadTxnCount          int
	MemberCount           int
	LastSnapshotDuration  time.Duration
	LastSnapshotSizeBytes uint64
}

// SnapshotStatus describes current snapshotting state and recent metrics.
type SnapshotStatus struct {
	InProgress            bool
	LastSnapshotIndex     uint64
	EntriesSince          uint64
	LastSnapshotTime      time.Time
	InProgressSince       time.Time
	AppliedIndex          uint64
	LastRaftIndex         uint64
	Leader                string
	LastSnapshotDuration  time.Duration
	LastSnapshotSizeBytes uint64
}

// SnapshotStatus returns the current snapshotting status/metrics.
func (c *Cluster) SnapshotStatus() SnapshotStatus {
	var status SnapshotStatus
	status.AppliedIndex = c.raftNode.AppliedIndex()
	if last, err := c.storage.LastIndex(); err == nil {
		status.LastRaftIndex = last
		status.EntriesSince = last - c.lastSnapshotIndex
	}
	status.LastSnapshotIndex = c.lastSnapshotIndex
	status.LastSnapshotTime = c.lastSnapshotTime
	status.LastSnapshotDuration = c.lastSnapshotDuration
	status.LastSnapshotSizeBytes = c.lastSnapshotSizeBytes
	status.Leader = c.LeaderAddress()
	c.snapshotMu.Lock()
	status.InProgress = c.snapshotInProgress
	status.InProgressSince = c.snapshotStartAt
	c.snapshotMu.Unlock()
	return status
}

// Diagnostics returns the latest cluster metrics useful for observability.
func (c *Cluster) Diagnostics() Diagnostics {
	diag := Diagnostics{
		LastSnapshotIndex:     c.lastSnapshotIndex,
		LastSnapshotTime:      c.lastSnapshotTime,
		LastSnapshotDuration:  c.lastSnapshotDuration,
		LastSnapshotSizeBytes: c.lastSnapshotSizeBytes,
	}
	if c.raftNode != nil {
		st := c.raftNode.Status()
		diag.LeaderID = st.Lead
		diag.Term = st.Term
		diag.AppliedIndex = st.Applied
		diag.CommittedIndex = st.Commit
	}
	if c.storage != nil {
		if last, err := c.storage.LastIndex(); err == nil {
			diag.LastRaftIndex = last
			if last >= diag.LastSnapshotIndex {
				diag.EntriesSinceSnapshot = last - diag.LastSnapshotIndex
			}
		}
	}
	c.snapshotMu.Lock()
	diag.SnapshotInProgress = c.snapshotInProgress
	diag.SnapshotStartTime = c.snapshotStartAt
	c.snapshotMu.Unlock()
	diag.ReadTxnCount = c.readTxnCount()
	c.membersMu.RLock()
	diag.MemberCount = len(c.members)
	c.membersMu.RUnlock()
	return diag
}

// RegisterDiagnosticsObserver registers a callback invoked whenever diagnostics are sampled.
func (c *Cluster) RegisterDiagnosticsObserver(fn func(Diagnostics)) {
	if fn == nil {
		return
	}
	c.diagnosticsMu.Lock()
	c.diagnosticsObservers = append(c.diagnosticsObservers, fn)
	c.diagnosticsMu.Unlock()
}

func (c *Cluster) notifyDiagnostics(diag Diagnostics) {
	c.diagnosticsMu.RLock()
	observers := append([]func(Diagnostics){}, c.diagnosticsObservers...)
	c.diagnosticsMu.RUnlock()
	for _, obs := range observers {
		obs(diag)
	}
}

func (c *Cluster) hasDiagnosticsObserver() bool {
	c.diagnosticsMu.RLock()
	defer c.diagnosticsMu.RUnlock()
	return len(c.diagnosticsObservers) > 0
}

type readTxnEntry struct {
	txn       *db.ReadTxn
	createdAt time.Time
}

func (c *Cluster) registerReadTxn(txn *db.ReadTxn) ([]byte, error) {
	const handleSize = 16
	handle := make([]byte, handleSize)

	c.readTxnMu.Lock()
	defer c.readTxnMu.Unlock()

	for {
		if _, err := rand.Read(handle); err != nil {
			return nil, err
		}
		key := string(handle)
		if _, exists := c.readTxns[key]; exists {
			continue
		}
		c.readTxns[key] = &readTxnEntry{
			txn:       txn,
			createdAt: time.Now(),
		}
		return append([]byte(nil), handle...), nil
	}
}

func (c *Cluster) borrowReadTxn(handle []byte) (*db.ReadTxn, func(), bool) {
	c.readTxnMu.RLock()
	entry, ok := c.readTxns[string(handle)]
	if !ok {
		c.readTxnMu.RUnlock()
		return nil, nil, false
	}
	return entry.txn, func() { c.readTxnMu.RUnlock() }, true
}

func (c *Cluster) deregisterReadTxn(handle []byte) *db.ReadTxn {
	c.readTxnMu.Lock()
	defer c.readTxnMu.Unlock()

	key := string(handle)
	entry, ok := c.readTxns[key]
	if ok {
		delete(c.readTxns, key)
	}
	if entry != nil {
		return entry.txn
	}
	return nil
}

func (c *Cluster) runReadTxnCleaner() {
	defer c.wg.Done()
	interval := c.readTxnTTL / 2
	if interval <= 0 {
		interval = time.Second
	}
	if interval > 5*time.Second {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanupExpiredReadTxns()
		case <-c.ctx.Done():
			c.cleanupExpiredReadTxns()
			return
		}
	}
}

func (c *Cluster) cleanupExpiredReadTxns() {
	ttl := c.readTxnTTL
	if ttl <= 0 {
		ttl = time.Minute
	}
	now := time.Now()
	c.readTxnMu.Lock()
	for handle, entry := range c.readTxns {
		if now.Sub(entry.createdAt) > ttl {
			entry.txn.Close()
			delete(c.readTxns, handle)
		}
	}
	c.readTxnMu.Unlock()
}

func (c *Cluster) readTxnCount() int {
	c.readTxnMu.RLock()
	defer c.readTxnMu.RUnlock()
	return len(c.readTxns)
}

func (c *Cluster) runAutoSnapshot() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.snapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if !c.snapshotEnabled {
				continue
			}
			// Observe leader changes to avoid jitter
			if c.raftNode != nil {
				st := c.raftNode.Status()
				if st.Lead != c.lastLeader {
					c.lastLeader = st.Lead
					c.lastLeaderChangeAt = time.Now()
				}
			}
			// Skip on followers.
			if !c.IsLeader() {
				continue
			}
			if err := c.TriggerSnapshot(false); err != nil {
				if errors.Is(err, ErrSnapshotNotNeeded) || errors.Is(err, ErrSnapshotInProgress) || errors.Is(err, ErrNotLeader) {
					continue
				}
				fmt.Printf("auto snapshot error: %v\n", err)
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Cluster) runDiagnostics() {
	defer c.wg.Done()
	interval := c.diagnosticsInterval
	if interval <= 0 {
		interval = defaultDiagnosticsInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			diag := c.Diagnostics()
			if c.diagnosticsEnabled {
				fmt.Printf("diagnostics: term=%d leader=%d members=%d commit=%d applied=%d lastIndex=%d lastSnapshot=%d entriesSince=%d readTxns=%d snapshotInProgress=%v snapshotDuration=%s snapshotBytes=%d\n",
					diag.Term, diag.LeaderID, diag.MemberCount, diag.CommittedIndex, diag.AppliedIndex, diag.LastRaftIndex,
					diag.LastSnapshotIndex, diag.EntriesSinceSnapshot, diag.ReadTxnCount, diag.SnapshotInProgress,
					diag.LastSnapshotDuration, diag.LastSnapshotSizeBytes)
			}
			c.notifyDiagnostics(diag)
		case <-c.ctx.Done():
			return
		}
	}
}
