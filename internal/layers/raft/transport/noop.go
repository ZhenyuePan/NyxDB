package raft

import "go.etcd.io/etcd/raft/v3/raftpb"

// NewNoopTransport creates a transport that drops all messages; useful for single-node tests.
var NewNoopTransport = func() Transport {
	return noopTransport{}
}

type noopTransport struct{}

func (noopTransport) Send(uint64, []raftpb.Message) error        { return nil }
func (noopTransport) SendSnapshot(uint64, raftpb.Snapshot) error { return nil }
func (noopTransport) AddMember(uint64, []string) error           { return nil }
func (noopTransport) RemoveMember(uint64) error                  { return nil }
