package cluster

import (
	"fmt"

	regionmgr "nyxdb/internal/cluster/regions"
	raftnode "nyxdb/internal/layers/raft/node"
	raftstorage "nyxdb/internal/layers/raft/storage"
	regionpkg "nyxdb/internal/region"
)

// replica returns the replica for a region ID.
func (c *Cluster) replica(regionID regionpkg.ID) *regionmgr.Replica {
	return c.regionMgr.Replica(regionID)
}

func (c *Cluster) regionReplicaList() []*regionmgr.Replica {
	return c.regionMgr.Replicas()
}

// createRegionReplica builds storage and a raft node for the provided region metadata.
func (c *Cluster) createRegionReplica(id regionpkg.ID, region *regionpkg.Region) (*regionmgr.Replica, error) {
	if region == nil {
		return nil, fmt.Errorf("region metadata missing for id %d", id)
	}
	storage, err := raftstorage.New(regionRaftDir(c.options.DirPath, id))
	if err != nil {
		return nil, err
	}

	raftConfig := &raftnode.NodeConfig{
		ID:            c.nodeID,
		Cluster:       c.buildRaftPeers(),
		Storage:       storage,
		Transport:     c.transport,
		ElectionTick:  10,
		HeartbeatTick: 1,
	}

	if err := c.persistMembers(); err != nil {
		return nil, err
	}

	node := raftnode.NewNode(raftConfig)
	replica := &regionmgr.Replica{
		Region:  region,
		Node:    node,
		Storage: storage,
	}
	c.regionMgr.RegisterReplica(replica)
	if c.isStarted() {
		replica.Node.Start(c.commitC, c.errorC)
	}
	return replica, nil
}
