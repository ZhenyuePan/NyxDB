package cluster

import (
	"context"
	"fmt"
	"time"

	pd "nyxdb/internal/layers/pd"
	pdgrpc "nyxdb/internal/layers/pd/grpc"
	regionpkg "nyxdb/internal/region"
)

// AttachPD configures the cluster to report heartbeats to a PD service. If
// called after Start, the heartbeat loop is launched immediately.
func (c *Cluster) AttachPD(service pd.MetadataClient, interval time.Duration) {
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

	if service != nil {
		c.syncRegionsWithPD(service)
	}

	if startLoop {
		c.wg.Add(1)
		go c.runPDHeartbeats()
	}
}

func (c *Cluster) AttachPDClient(ctx context.Context, target string, interval time.Duration) error {
	client, err := pdgrpc.NewClient(ctx, target)
	if err != nil {
		return err
	}
	c.lifecycleMu.Lock()
	if c.pdCloser != nil {
		_ = c.pdCloser()
	}
	c.pdCloser = client.Close
	c.lifecycleMu.Unlock()
	c.AttachPD(client, interval)
	return nil
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
	client := c.metadataClient()
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
			PeerID:       peerIDFor(r.ID, c.nodeID),
			Role:         regionpkg.Voter,
			AppliedIndex: applied,
		})
	}
	client.HandleHeartbeat(hb)
}

func (c *Cluster) metadataClient() pd.MetadataClient {
	c.lifecycleMu.RLock()
	client := c.pdClient
	c.lifecycleMu.RUnlock()
	return client
}

func (c *Cluster) syncRegionsWithPD(client pd.MetadataClient) {
	regions := c.regionMgr.Regions()
	for _, region := range regions {
		c.registerRegionWithPD(client, region)
	}
}

func (c *Cluster) registerRegionWithPD(client pd.MetadataClient, region regionpkg.Region) {
	if client == nil || region.ID == 0 {
		return
	}
	if _, err := client.RegisterRegion(region); err != nil {
		if pd.IsRegionExistsError(err) {
			if _, updateErr := client.UpdateRegion(region); updateErr != nil && !pd.IsRegionNotFoundError(updateErr) {
				fmt.Printf("pd: update region %d failed: %v\n", region.ID, updateErr)
			}
			return
		}
		fmt.Printf("pd: register region %d failed: %v\n", region.ID, err)
	}
}

func (c *Cluster) tombstoneRegionWithPD(client pd.MetadataClient, region regionpkg.Region) {
	if client == nil || region.ID == 0 {
		return
	}
	region.State = regionpkg.StateTombstone
	if _, err := client.UpdateRegion(region); err != nil && !pd.IsRegionNotFoundError(err) {
		fmt.Printf("pd: tombstone region %d failed: %v\n", region.ID, err)
	}
}
