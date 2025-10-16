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

	db "nyxdb/internal/engine"
	raftnode "nyxdb/internal/node"
	rafttransport "nyxdb/internal/raft"
	replication "nyxdb/internal/replication"

	etcdraft "go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Cluster 代表一个分布式存储集群
type Cluster struct {
	nodeID    uint64
	options   db.Options
	db        *db.DB
	raftNode  *raftnode.Node
	transport rafttransport.Transport

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
	readTxnMu sync.RWMutex
	readTxns  map[string]*db.ReadTxn
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
		nodeID:       nodeID,
		options:      options,
		db:           database,
		members:      make(map[uint64]string),
		commitC:      make(chan *raftnode.Commit, 100),
		errorC:       make(chan error, 100),
		ctx:          ctx,
		cancel:       cancel,
		nextCommitTs: database.MaxCommittedTs(),
		transport:    transport,
		readTxns:     make(map[string]*db.ReadTxn),
	}

	cluster.applier = replication.NewApplier(database)

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

	storage, err := NewRaftStorage(filepath.Join(options.DirPath, "raft"))
	if err != nil {
		cancel()
		return nil, err
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

			if commit.ConfChange != nil {
				c.applyConfChange(commit.ConfChange)
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

	if len(c.members) == 0 {
		cfg := c.options.ClusterConfig
		if cfg != nil && cfg.ClusterMode {
			for _, p := range parsePeerAddresses(cfg.ClusterAddresses) {
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

func (c *Cluster) applyConfChange(cc *raftpb.ConfChange) {
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
	ErrReadTxnNotFound = errors.New("cluster: read transaction not found")
	ErrNotLeader       = errors.New("cluster: not leader")
)

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
		c.readTxns[key] = txn
		return append([]byte(nil), handle...), nil
	}
}

func (c *Cluster) borrowReadTxn(handle []byte) (*db.ReadTxn, func(), bool) {
	c.readTxnMu.RLock()
	txn, ok := c.readTxns[string(handle)]
	if !ok {
		c.readTxnMu.RUnlock()
		return nil, nil, false
	}
	return txn, c.readTxnMu.RUnlock, true
}

func (c *Cluster) deregisterReadTxn(handle []byte) *db.ReadTxn {
	c.readTxnMu.Lock()
	defer c.readTxnMu.Unlock()

	key := string(handle)
	txn, ok := c.readTxns[key]
	if ok {
		delete(c.readTxns, key)
	}
	return txn
}
