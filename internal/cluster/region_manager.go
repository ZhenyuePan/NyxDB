package cluster

import (
	"fmt"

	regionmgr "nyxdb/internal/cluster/regions"
	regionpkg "nyxdb/internal/region"
	utils "nyxdb/internal/utils"
)

// CreateStaticRegion registers a new region with the provided key range and
// spins up its Raft replica. This is a scaffolding helper before PD/Region
// scheduling is introduced. The method must be called before conflicting
// regions exist; no overlap detection is currently performed.
func (c *Cluster) CreateStaticRegion(keyRange regionpkg.KeyRange) (*regionpkg.Region, error) {
	region := c.regionMgr.CreateRegion(keyRange)
	if region == nil {
		return nil, fmt.Errorf("failed to allocate region")
	}

	replica, err := c.createRegionReplica(region.ID, region)
	if err != nil {
		c.regionMgr.RemoveRegion(region.ID)
		return nil, err
	}
	if err := c.persistRegions(); err != nil {
		if replica.Node != nil {
			replica.Node.Stop()
		}
		if replica.Storage != nil {
			_ = replica.Storage.Close()
		}
		c.router.Unregister(replica.PeerID)
		c.regionMgr.RemoveRegion(region.ID)
		return nil, err
	}
	c.sendPDHeartbeat()

	clone := replica.Region.Clone()
	return &clone, nil
}

func (c *Cluster) isStarted() bool {
	c.lifecycleMu.RLock()
	defer c.lifecycleMu.RUnlock()
	return c.started
}

func (c *Cluster) setStarted(v bool) {
	c.lifecycleMu.Lock()
	c.started = v
	c.lifecycleMu.Unlock()
}

// RemoveRegion shuts down the replica for a region and deletes its metadata.
func (c *Cluster) RemoveRegion(id regionpkg.ID) error {
	if id == regionmgr.DefaultRegionID {
		return fmt.Errorf("cannot remove default region")
	}
	rep := c.regionMgr.RemoveRegion(id)
	if rep == nil {
		return nil
	}
	if rep.Node != nil {
		rep.Node.Stop()
	}
	if err := rep.Storage.Close(); err != nil {
		return err
	}
	c.router.Unregister(rep.PeerID)
	if err := c.persistRegions(); err != nil {
		return err
	}
	c.sendPDHeartbeat()
	return utils.RemoveDir(regionBaseDir(c.options.DirPath, id))
}
