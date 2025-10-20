package raft

import "go.etcd.io/etcd/raft/v3/raftpb"

// Transport defines the contract for sending raft messages between stores.
type Transport interface {
    Send(toPeer uint64, messages []raftpb.Message) error
    SendSnapshot(toPeer uint64, snapshot raftpb.Snapshot) error
    AddMember(peerID uint64, peerURLs []string) error
    RemoveMember(peerID uint64) error
}
