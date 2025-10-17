package cluster

import (
	"bytes"
	"sort"

	regionpkg "nyxdb/internal/region"
)

const defaultRegionID regionpkg.ID = 1

func (c *Cluster) initDefaultRegions() {
	c.regionMu.Lock()
	defer c.regionMu.Unlock()
	if len(c.regions) == 0 {
		c.regions[defaultRegionID] = &regionpkg.Region{
			ID:    defaultRegionID,
			Range: regionpkg.KeyRange{},
			Epoch: regionpkg.Epoch{Version: 1, ConfVersion: 1},
			State: regionpkg.StateActive,
		}
	}
	if c.nextRegionID <= defaultRegionID {
		c.nextRegionID = defaultRegionID + 1
	}
}

// Regions returns a snapshot of registered regions ordered by start key.
func (c *Cluster) Regions() []regionpkg.Region {
	c.regionMu.RLock()
	defer c.regionMu.RUnlock()
	out := make([]regionpkg.Region, 0, len(c.regions))
	for _, r := range c.regions {
		if r == nil {
			continue
		}
		out = append(out, r.Clone())
	}
	sort.Slice(out, func(i, j int) bool {
		return string(out[i].Range.Start) < string(out[j].Range.Start)
	})
	return out
}

// RegionForKey finds the region containing the provided key.
func (c *Cluster) RegionForKey(key []byte) *regionpkg.Region {
	regions := c.Regions()
	var best *regionpkg.Region
	for i := range regions {
		r := &regions[i]
		if !r.ContainsKey(key) {
			continue
		}
		if best == nil {
			clone := r.Clone()
			best = &clone
			continue
		}
		// Prefer regions with the greatest start key (most specific range).
		if bytes.Compare(r.Range.Start, best.Range.Start) > 0 {
			clone := r.Clone()
			best = &clone
		}
	}
	return best
}
