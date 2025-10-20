package cluster

import (
	"context"
	"fmt"
	"sync"

	regionmgr "nyxdb/internal/cluster/regions"
	regionpkg "nyxdb/internal/region"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// raftRouter routes raft messages to region-specific peers.
type raftRouter struct {
	mu    sync.RWMutex
	peers map[uint64]*regionmgr.Replica
}

func newRaftRouter() *raftRouter {
	return &raftRouter{
		peers: make(map[uint64]*regionmgr.Replica),
	}
}

// Register associates a replica with its peer id.
func (r *raftRouter) Register(peerID uint64, replica *regionmgr.Replica) {
	if replica == nil {
		return
	}
	r.mu.Lock()
	r.peers[peerID] = replica
	r.mu.Unlock()
}

// Unregister removes a replica mapping.
func (r *raftRouter) Unregister(peerID uint64) {
	r.mu.Lock()
	delete(r.peers, peerID)
	r.mu.Unlock()
}

// Reset clears all routing entries.
func (r *raftRouter) Reset() {
	r.mu.Lock()
	r.peers = make(map[uint64]*regionmgr.Replica)
	r.mu.Unlock()
}

// Step routes the raft message to the appropriate replica.
func (r *raftRouter) Step(ctx context.Context, msg raftpb.Message) error {
	r.mu.RLock()
	replica := r.peers[msg.To]
	r.mu.RUnlock()
	if replica == nil || replica.Node == nil {
		return fmt.Errorf("raft router: no replica for peer %d", msg.To)
	}
	return replica.Node.Step(ctx, msg)
}

// RegionByPeer returns the region id owning the peer.
func (r *raftRouter) RegionByPeer(peerID uint64) regionpkg.ID {
	r.mu.RLock()
	replica := r.peers[peerID]
	r.mu.RUnlock()
	if replica != nil && replica.Region != nil {
		return replica.Region.ID
	}
	return 0
}
