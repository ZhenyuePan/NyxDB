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
		nodeID:  nodeID,
		options: options,
		db:      database,
		members: make(map[uint64]string),
		commitC: make(chan *raftnode.Commit, 100),
		errorC:  make(chan error, 100),
		ctx:     ctx,
		cancel:  cancel,
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
	// 构造操作命令
	cmd := &Command{
		Type:  CmdPut,
		Key:   key,
		Value: value,
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
	// 构造操作命令
	cmd := &Command{
		Type: CmdDelete,
		Key:  key,
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
			cmd := &Command{}
			if err := cmd.Unmarshal(commit.Data); err != nil {
				fmt.Printf("Failed to unmarshal command: %v\n", err)
				continue
			}

			// 执行命令
			switch cmd.Type {
			case CmdPut:
				_ = c.db.Put(cmd.Key, cmd.Value)
			case CmdDelete:
				_ = c.db.Delete(cmd.Key)
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
	var peers []etcdraft.Peer

	if c.options.ClusterConfig != nil {
		// 根据集群配置构建peers
		// 这里需要根据实际配置实现
	}

	return peers
}

// Command 定义在RAFT中传播的命令
type Command struct {
	Type  CommandType
	Key   []byte
	Value []byte
}

// CommandType 命令类型
type CommandType byte

const (
	CmdPut CommandType = iota
	CmdDelete
)

// Marshal 序列化命令
func (c *Command) Marshal() ([]byte, error) {
	// 简单的序列化实现
	data := make([]byte, 1+len(c.Key)+len(c.Value)+8)
	data[0] = byte(c.Type)

	// 写入key长度和key
	keyLen := len(c.Key)
	copy(data[1:], encodeUint64(uint64(keyLen)))
	copy(data[9:], c.Key)

	// 写入value长度和value
	valueLen := len(c.Value)
	copy(data[9+keyLen:], encodeUint64(uint64(valueLen)))
	copy(data[17+keyLen:], c.Value)

	return data, nil
}

// Unmarshal 反序列化命令
func (c *Command) Unmarshal(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("invalid command data")
	}

	c.Type = CommandType(data[0])

	if len(data) < 9 {
		return fmt.Errorf("invalid command data")
	}

	// 读取key
	keyLen := int(decodeUint64(data[1:]))
	if len(data) < 9+keyLen {
		return fmt.Errorf("invalid command data")
	}
	c.Key = make([]byte, keyLen)
	copy(c.Key, data[9:9+keyLen])

	// 读取value
	if len(data) < 17+keyLen {
		return fmt.Errorf("invalid command data")
	}
	valueLen := int(decodeUint64(data[9+keyLen:]))
	if len(data) < 17+keyLen+valueLen {
		return fmt.Errorf("invalid command data")
	}
	c.Value = make([]byte, valueLen)
	copy(c.Value, data[17+keyLen:17+keyLen+valueLen])

	return nil
}

// encodeUint64 编码uint64为字节
func encodeUint64(v uint64) []byte {
	data := make([]byte, 8)
	data[0] = byte(v >> 56)
	data[1] = byte(v >> 48)
	data[2] = byte(v >> 40)
	data[3] = byte(v >> 32)
	data[4] = byte(v >> 24)
	data[5] = byte(v >> 16)
	data[6] = byte(v >> 8)
	data[7] = byte(v)
	return data
}

// decodeUint64 从字节解码uint64
func decodeUint64(data []byte) uint64 {
	return uint64(data[0])<<56 | uint64(data[1])<<48 | uint64(data[2])<<40 |
		uint64(data[3])<<32 | uint64(data[4])<<24 | uint64(data[5])<<16 |
		uint64(data[6])<<8 | uint64(data[7])
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
