package cluster

import (
	"fmt"

	regionpkg "nyxdb/internal/region"
)

// CreateStaticRegion registers a new region with the provided key range and
// spins up its Raft replica. This is a scaffolding helper before PD/Region
// scheduling is introduced. The method must be called before conflicting
// regions exist; no overlap detection is currently performed.
func (c *Cluster) CreateStaticRegion(keyRange regionpkg.KeyRange) (*regionpkg.Region, error) {
	c.regionMu.Lock()
	id := c.nextRegionID
	c.nextRegionID++
	if _, exists := c.regions[id]; exists {
		c.regionMu.Unlock()
		return nil, fmt.Errorf("region id %d already exists", id)
	}
	region := &regionpkg.Region{
		ID:    id,
		Range: keyRange,
		Epoch: regionpkg.Epoch{Version: 1, ConfVersion: 1},
		State: regionpkg.StateActive,
	}
	c.regions[id] = region
	c.regionMu.Unlock()

	replica, err := c.createRegionReplica(id, region)
	if err != nil {
		c.regionMu.Lock()
		delete(c.regions, id)
		c.regionMu.Unlock()
		return nil, err
	}

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
