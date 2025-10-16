package cluster

import (
	"context"
	"fmt"
	db "nyxdb/internal/engine"
	raftnode "nyxdb/internal/node"
	rafttransport "nyxdb/internal/raft"
	proxy "nyxdb/internal/server/proxy"
	"sync"

	etcdraft "go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Cluster 代表一个分布式存储集群
type Cluster struct {
	nodeID    uint64
	options   db.Options
	db        *db.DB
	raftNode  *raftnode.Node
	proxy     *proxy.Proxy
	transport rafttransport.Transport

	// 集群成员管理
	members   map[uint64]string // nodeID -> address
	membersMu sync.RWMutex

	commitMu     sync.Mutex
	nextCommitTs uint64

	// 数据提交通道
	commitC chan *raftnode.Commit
	errorC  chan error

	// 控制
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewCluster 创建一个新的集群实例
func NewCluster(nodeID uint64, options db.Options, database *db.DB) (*Cluster, error) {
	ctx, cancel := context.WithCancel(context.Background())

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
	}

	// 初始化传输层
	cluster.transport = rafttransport.NewDefaultTransport()

	// 初始化代理
	cluster.proxy = proxy.NewProxy(options, database)

	// 初始化RAFT节点
	raftConfig := &raftnode.NodeConfig{
		ID:            nodeID,
		Cluster:       cluster.buildRaftPeers(),
		Storage:       NewRaftStorage(), // 需要实现一个持久化存储
		Transport:     cluster.transport,
		ElectionTick:  10,
		HeartbeatTick: 1,
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
	cmd := &Command{
		CommitTs: c.allocateCommitTs(),
		Operations: []Operation{
			{Key: append([]byte(nil), key...), Value: append([]byte(nil), value...), Type: OpPut},
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
	cmd := &Command{
		CommitTs: c.allocateCommitTs(),
		Operations: []Operation{
			{Key: append([]byte(nil), key...), Type: OpDelete},
		},
	}

	data, err := cmd.Marshal()
	if err != nil {
		return err
	}

	// 通过RAFT提交命令
	return c.raftNode.Propose(data)
}

// Get 从本地数据库获取数据
func (c *Cluster) Get(key []byte) ([]byte, error) {
	// 读操作不需要通过RAFT，直接从本地数据库获取
	return c.db.Get(key)
}

// IsLeader 检查当前节点是否为Leader
func (c *Cluster) IsLeader() bool {
	return c.raftNode.IsLeader()
}

// AddMember 添加新成员到集群
func (c *Cluster) AddMember(nodeID uint64, address string) error {
	// 更新本地成员列表
	c.membersMu.Lock()
	c.members[nodeID] = address
	c.membersMu.Unlock()

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
	// 从本地成员列表中移除
	c.membersMu.Lock()
	delete(c.members, nodeID)
	c.membersMu.Unlock()

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

			// 解析命令
			cmd, err := UnmarshalCommand(commit.Data)
			if err != nil {
				fmt.Printf("Failed to unmarshal command: %v\n", err)
				continue
			}

			replOps := make([]db.ReplicatedOp, 0, len(cmd.Operations))
			for _, op := range cmd.Operations {
				repl := db.ReplicatedOp{Key: append([]byte(nil), op.Key...)}
				if op.Type == OpDelete {
					repl.Delete = true
				} else {
					repl.Value = append([]byte(nil), op.Value...)
				}
				replOps = append(replOps, repl)
			}
			if err := c.db.ApplyReplicated(cmd.CommitTs, replOps); err != nil {
				fmt.Printf("failed to apply replicated command: %v\n", err)
			}
			c.observeCommitTs(cmd.CommitTs)

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
	var peers []etcdraft.Peer

	if c.options.ClusterConfig != nil {
		// 根据集群配置构建peers
		// 这里需要根据实际配置实现
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

// RaftStorage RAFT存储实现
type RaftStorage struct {
	// 实现etcd/raft/v3/raftpb.Storage接口
	// 这里需要根据实际存储需求实现
}

// NewRaftStorage 创建RAFT存储实例
func NewRaftStorage() *RaftStorage {
	return &RaftStorage{}
}

// InitialState 实现Storage接口
func (s *RaftStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	// 返回初始状态
	return raftpb.HardState{}, raftpb.ConfState{}, nil
}

// Entries 实现Storage接口
func (s *RaftStorage) Entries(lo, hi uint64, maxSize uint64) ([]raftpb.Entry, error) {
	// 返回指定范围的条目
	return []raftpb.Entry{}, nil
}

// Term 实现Storage接口
func (s *RaftStorage) Term(i uint64) (uint64, error) {
	// 返回指定索引的任期
	return 0, nil
}

// LastIndex 实现Storage接口
func (s *RaftStorage) LastIndex() (uint64, error) {
	// 返回最后一条日志的索引
	return 0, nil
}

// FirstIndex 实现Storage接口
func (s *RaftStorage) FirstIndex() (uint64, error) {
	// 返回第一条日志的索引
	return 0, nil
}

// Snapshot 实现Storage接口
func (s *RaftStorage) Snapshot() (raftpb.Snapshot, error) {
	// 返回当前快照
	return raftpb.Snapshot{}, nil
}

// ApplySnapshot 实现Storage接口
func (s *RaftStorage) ApplySnapshot(snap raftpb.Snapshot) error {
	// 应用快照
	return nil
}

// SetHardState 实现Storage接口
func (s *RaftStorage) SetHardState(st raftpb.HardState) error {
	// 设置硬状态
	return nil
}

// Append 实现Storage接口
func (s *RaftStorage) Append(entries []raftpb.Entry) error {
	// 添加条目
	return nil
}
