package raft

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	rafttransport "nyxdb/internal/raft"
)

// Node RAFT节点结构
type Node struct {
	id        uint64
	raftNode  raft.Node
	config    *raft.Config
	transport rafttransport.Transport
	storage   raft.Storage

	// 状态相关
	mu      sync.RWMutex
	applied uint64

	// 通道
	proposeC    chan []byte
	confChangeC chan raftpb.ConfChange
	commitC     chan<- *Commit
	errorC      chan<- error

	// 控制
	ctx    context.Context
	cancel context.CancelFunc

	readReqMu  sync.Mutex
	readReqs   map[string]chan uint64
	readReqSeq uint64
}

// Commit 提交的数据
type Commit struct {
	Data       []byte
	Index      uint64
	Term       uint64
	ConfChange *raftpb.ConfChange
}

// NodeConfig RAFT节点配置
type NodeConfig struct {
	ID            uint64
	Cluster       []raft.Peer
	Storage       raft.Storage
	Transport     rafttransport.Transport
	TickMs        uint64
	ElectionTick  int
	HeartbeatTick int
}

// NewNode 创建新的RAFT节点
func NewNode(config *NodeConfig) *Node {
	// 设置RAFT配置
	raftConfig := &raft.Config{
		ID:              config.ID,
		ElectionTick:    config.ElectionTick,
		HeartbeatTick:   config.HeartbeatTick,
		Storage:         config.Storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
		CheckQuorum:     true,
		PreVote:         true,
	}

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())

	// 创建传输层
	nodeTransport := config.Transport
	if nodeTransport == nil {
		nodeTransport = rafttransport.NewDefaultTransport()
	}

	node := &Node{
		id:          config.ID,
		config:      raftConfig,
		transport:   nodeTransport,
		storage:     config.Storage,
		proposeC:    make(chan []byte, 100),
		confChangeC: make(chan raftpb.ConfChange),
		ctx:         ctx,
		cancel:      cancel,
		readReqs:    make(map[string]chan uint64),
	}

	// 创建RAFT节点
	if len(config.Cluster) > 0 {
		node.raftNode = raft.StartNode(raftConfig, config.Cluster)
	} else {
		node.raftNode = raft.RestartNode(raftConfig)
	}

	return node
}

// Start 启动RAFT节点
func (n *Node) Start(commitC chan<- *Commit, errorC chan<- error) {
	n.commitC = commitC
	n.errorC = errorC

	// 启动后台协程处理RAFT逻辑
	go n.run()
}

// Stop 停止RAFT节点
func (n *Node) Stop() {
	n.cancel()
	n.raftNode.Stop()
}

// Propose 提议一个新的数据
func (n *Node) Propose(data []byte) error {
	ctx, cancel := context.WithTimeout(n.ctx, time.Second*5)
	defer cancel()

	return n.raftNode.Propose(ctx, data)
}

// ProposeConfChange 提议配置变更
func (n *Node) ProposeConfChange(cc raftpb.ConfChange) error {
	ctx, cancel := context.WithTimeout(n.ctx, time.Second*5)
	defer cancel()

	return n.raftNode.ProposeConfChange(ctx, cc)
}

// Step processes an incoming raft message.
func (n *Node) Step(ctx context.Context, msg raftpb.Message) error {
	return n.raftNode.Step(ctx, msg)
}

// run 运行RAFT节点主循环
func (n *Node) run() {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.raftNode.Tick()

		case rd := <-n.raftNode.Ready():
			// 处理状态变更
			if !raft.IsEmptyHardState(rd.HardState) {
				// 保存硬状态
				// 修改: 使用类型断言来调用SetHardState方法
				if storage, ok := n.storage.(interface{ SetHardState(raftpb.HardState) error }); ok {
					if err := storage.SetHardState(rd.HardState); err != nil {
						n.sendError(err)
					}
				}
			}

			// 保存条目
			if len(rd.Entries) > 0 {
				// 修改: 使用类型断言来调用Append方法
				if storage, ok := n.storage.(interface{ Append([]raftpb.Entry) error }); ok {
					if err := storage.Append(rd.Entries); err != nil {
						n.sendError(err)
					}
				}
			}

			// 发送消息
			n.sendMessages(rd.Messages)

			// 应用快照
			if !raft.IsEmptySnap(rd.Snapshot) {
				// 修改: 使用类型断言来调用ApplySnapshot方法
				if storage, ok := n.storage.(interface{ ApplySnapshot(raftpb.Snapshot) error }); ok {
					if err := storage.ApplySnapshot(rd.Snapshot); err != nil {
						n.sendError(err)
					}
				}
				n.applySnapshot(rd.Snapshot)
			}

			// 提交数据
			n.applyCommits(rd.CommittedEntries)

			if len(rd.ReadStates) > 0 {
				n.handleReadStates(rd.ReadStates)
			}

			// 处理就绪状态
			n.raftNode.Advance()

		case <-n.ctx.Done():
			return
		}
	}
}

// sendMessages 发送消息
func (n *Node) sendMessages(messages []raftpb.Message) {
	for _, msg := range messages {
		if msg.To == 0 {
			continue
		}

		// 发送到目标节点
		err := n.transport.Send(msg.To, []raftpb.Message{msg})
		if err != nil {
			// 发送失败处理
			n.sendError(err)
		}
	}
}

// applyCommits 应用提交的条目
func (n *Node) applyCommits(committedEntries []raftpb.Entry) {
	for _, entry := range committedEntries {
		switch entry.Type {
		case raftpb.EntryNormal:
			if len(entry.Data) > 0 {
				commit := &Commit{
					Data:  entry.Data,
					Index: entry.Index,
					Term:  entry.Term,
				}
				select {
				case n.commitC <- commit:
				case <-n.ctx.Done():
					return
				}
			}
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				n.sendError(err)
				continue
			}
			n.raftNode.ApplyConfChange(cc)
			ccCopy := cc
			commit := &Commit{
				Index:      entry.Index,
				Term:       entry.Term,
				ConfChange: &ccCopy,
			}
			select {
			case n.commitC <- commit:
			case <-n.ctx.Done():
				return
			}
		}

		n.mu.Lock()
		if entry.Index > n.applied {
			n.applied = entry.Index
		}
		n.mu.Unlock()
	}
}

// applySnapshot 应用快照
func (n *Node) applySnapshot(snapshot raftpb.Snapshot) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.applied = snapshot.Metadata.Index
}

// IsLeader 判断当前节点是否为领导者
func (n *Node) IsLeader() bool {
	return n.raftNode.Status().Lead == n.id
}

// Status 获取节点状态
func (n *Node) Status() raft.Status {
	return n.raftNode.Status()
}

// sendError 发送错误信息
func (n *Node) sendError(err error) {
	if n.errorC != nil {
		select {
		case n.errorC <- err:
		default:
		}
	}
}

func (n *Node) handleReadStates(states []raft.ReadState) {
	for _, rs := range states {
		if ch := n.takeReadRequest(rs.RequestCtx); ch != nil {
			ch <- rs.Index
			close(ch)
		}
	}
}

func (n *Node) addReadRequest(reqCtx []byte, ch chan uint64) {
	n.readReqMu.Lock()
	n.readReqs[string(reqCtx)] = ch
	n.readReqMu.Unlock()
}

func (n *Node) takeReadRequest(reqCtx []byte) chan uint64 {
	n.readReqMu.Lock()
	defer n.readReqMu.Unlock()
	key := string(reqCtx)
	ch := n.readReqs[key]
	if ch != nil {
		delete(n.readReqs, key)
	}
	return ch
}

// ReadIndex issues a linearizable read and returns the log index that satisfies the read.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	seq := atomic.AddUint64(&n.readReqSeq, 1)
	reqCtx := make([]byte, 8)
	binary.BigEndian.PutUint64(reqCtx, seq)
	ch := make(chan uint64, 1)
	n.addReadRequest(reqCtx, ch)

	if err := n.raftNode.ReadIndex(ctx, reqCtx); err != nil {
		if pending := n.takeReadRequest(reqCtx); pending != nil {
			close(pending)
		}
		return 0, err
	}

	select {
	case idx, ok := <-ch:
		if !ok {
			return 0, context.Canceled
		}
		return idx, nil
	case <-ctx.Done():
		if pending := n.takeReadRequest(reqCtx); pending != nil {
			close(pending)
		}
		return 0, ctx.Err()
	}
}

// AppliedIndex returns the index of the latest applied entry.
func (n *Node) AppliedIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.applied
}

// WaitApplied blocks until the applied index reaches at least the target index.
func (n *Node) WaitApplied(ctx context.Context, index uint64) error {
	if index == 0 {
		return nil
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if n.AppliedIndex() >= index {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
