package raft

import "go.etcd.io/etcd/raft/v3/raftpb"

// Transport RAFT传输层接口
type Transport interface {
	// Send 发送消息到指定节点
	Send(to uint64, messages []raftpb.Message) error

	// SendSnapshot 发送快照到指定节点
	SendSnapshot(to uint64, snapshot raftpb.Snapshot) error

	// AddMember 添加新成员
	AddMember(id uint64, peerURLs []string) error

	// RemoveMember 移除成员
	RemoveMember(id uint64) error
}

// NoopTransport is a placeholder transport used by single-node/tests where
// messages never leave the process.
type NoopTransport struct{}

// Send 发送消息到指定节点
func (t *NoopTransport) Send(to uint64, messages []raftpb.Message) error {
	// TODO: 实现实际的消息发送逻辑
	// 这里应该通过网络将消息发送到目标节点
	// 为满足当前需求，添加基础实现
	for _, msg := range messages {
		// 在实际实现中，这里应该通过网络发送消息到目标节点
		// 暂时留空，需要根据实际网络库实现
		_ = msg
	}
	return nil
}

// SendSnapshot 发送快照到指定节点
func (t *NoopTransport) SendSnapshot(to uint64, snapshot raftpb.Snapshot) error {
	// TODO: 实现快照发送逻辑
	// 在实际实现中，这里应该通过网络发送快照数据到目标节点
	_ = snapshot
	return nil
}

// AddMember 添加新成员
func (t *NoopTransport) AddMember(id uint64, peerURLs []string) error {
	// TODO: 实现添加成员逻辑
	// 在实际实现中，这里应该通知集群添加新成员
	_ = id
	_ = peerURLs
	return nil
}

// RemoveMember 移除成员
func (t *NoopTransport) RemoveMember(id uint64) error {
	// TODO: 实现移除成员逻辑
	// 在实际实现中，这里应该通知集群移除成员
	_ = id
	return nil
}

// NewNoopTransport 创建默认传输实例
func NewNoopTransport() Transport {
	return &NoopTransport{}
}

// Deprecated: use NewNoopTransport for in-process tests or NewGRPCTransport for production.
func NewDefaultTransport() Transport {
	return NewNoopTransport()
}
