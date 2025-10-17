package cluster

import (
	"time"

	pd "nyxdb/internal/pd"
	regionpkg "nyxdb/internal/region"
)

// AttachPD configures the cluster to report heartbeats to a PD service. If
// called after Start, the heartbeat loop is launched immediately.
func (c *Cluster) AttachPD(service pd.Heartbeater, interval time.Duration) {
	c.lifecycleMu.Lock()
	c.pdClient = service
	if interval > 0 {
		c.pdHeartbeatInterval = interval
	} else if c.pdHeartbeatInterval <= 0 {
		c.pdHeartbeatInterval = 2 * time.Second
	}
	startLoop := c.started && service != nil && !c.pdHeartbeatStarted
	if startLoop {
		c.pdHeartbeatStarted = true
	}
	c.lifecycleMu.Unlock()

	if startLoop {
		c.wg.Add(1)
		go c.runPDHeartbeats()
	}
}

func (c *Cluster) runPDHeartbeats() {
	defer c.wg.Done()
	interval := c.pdHeartbeatInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	c.sendPDHeartbeat()
	for {
		select {
		case <-ticker.C:
			c.sendPDHeartbeat()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Cluster) sendPDHeartbeat() {
	c.lifecycleMu.RLock()
	client := c.pdClient
	c.lifecycleMu.RUnlock()
	if client == nil {
		return
	}

	hb := pd.StoreHeartbeat{
		StoreID:   c.nodeID,
		Address:   c.storeAddress,
		Timestamp: time.Now(),
	}
	regions := c.Regions()
	hb.Regions = make([]pd.RegionHeartbeat, 0, len(regions))
	for _, r := range regions {
		applied := uint64(0)
		if rep := c.replica(r.ID); rep != nil && rep.Node != nil {
			applied = rep.Node.AppliedIndex()
		}
		hb.Regions = append(hb.Regions, pd.RegionHeartbeat{
			Region:       r,
			StoreID:      c.nodeID,
			Role:         regionpkg.Voter,
			AppliedIndex: applied,
		})
	}
	client.HandleHeartbeat(hb)
}
