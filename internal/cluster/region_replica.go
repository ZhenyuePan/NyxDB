package cluster

import (
	"fmt"

	raftnode "nyxdb/internal/node"
	regionpkg "nyxdb/internal/region"
)

// RegionReplica ties a Region metadata entry to its Raft node and storage.
type RegionReplica struct {
	Region  *regionpkg.Region
	Node    *raftnode.Node
	Storage *RaftStorage
}

func (c *Cluster) registerReplica(rep *RegionReplica) {
	if rep == nil || rep.Region == nil {
		return
	}
	c.regionMu.Lock()
	c.regionReplicas[rep.Region.ID] = rep
	c.regionMu.Unlock()
}

func (c *Cluster) replica(regionID regionpkg.ID) *RegionReplica {
	c.regionMu.RLock()
	defer c.regionMu.RUnlock()
	return c.regionReplicas[regionID]
}

// createRegionReplica builds storage and a raft node for the provided region metadata.
func (c *Cluster) createRegionReplica(id regionpkg.ID, region *regionpkg.Region) (*RegionReplica, error) {
	if region == nil {
		return nil, fmt.Errorf("region metadata missing for id %d", id)
	}
	storage, err := NewRaftStorage(regionRaftDir(c.options.DirPath, id))
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
	replica := &RegionReplica{
		Region:  region,
		Node:    node,
		Storage: storage,
	}
	c.registerReplica(replica)
	if c.isStarted() {
		replica.Node.Start(c.commitC, c.errorC)
	}
	return replica, nil
}

func (c *Cluster) regionReplicaList() []*RegionReplica {
	c.regionMu.RLock()
	defer c.regionMu.RUnlock()
	list := make([]*RegionReplica, 0, len(c.regionReplicas))
	for _, rep := range c.regionReplicas {
		list = append(list, rep)
	}
	return list
}
