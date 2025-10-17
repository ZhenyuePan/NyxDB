package cluster

import (
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
