package cluster

import (
	"bytes"
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
	snapshots := client.RegionsByStore(c.nodeID)
	if snapshots == nil {
		fmt.Printf("pd sync: fetch regions for store %d failed\n", c.nodeID)
		return
	}

	seen := make(map[regionpkg.ID]struct{}, len(snapshots))
	dirty := false

	for _, snapshot := range snapshots {
		region := snapshot.Region.Clone()
		if region.ID == 0 {
			continue
		}
		seen[region.ID] = struct{}{}

		local := c.regionMgr.Region(region.ID)
		update := false
		if local == nil {
			fmt.Printf("pd sync: adopting region %d metadata from PD\n", region.ID)
			update = true
		} else {
			if !bytes.Equal(local.Range.Start, region.Range.Start) || !bytes.Equal(local.Range.End, region.Range.End) {
				fmt.Printf("pd sync: region %d range mismatch (local [%x,%x) vs PD [%x,%x)); adopting PD metadata\n",
					region.ID, local.Range.Start, local.Range.End, region.Range.Start, region.Range.End)
				update = true
			}
			if local.Epoch.Version != region.Epoch.Version || local.Epoch.ConfVersion != region.Epoch.ConfVersion {
				fmt.Printf("pd sync: region %d epoch mismatch (local v=%d,c=%d PD v=%d,c=%d); adopting PD metadata\n",
					region.ID, local.Epoch.Version, local.Epoch.ConfVersion, region.Epoch.Version, region.Epoch.ConfVersion)
				update = true
			}
		}
		if update {
			c.regionMgr.UpsertRegion(region)
			dirty = true
			if c.replica(region.ID) == nil {
				fmt.Printf("pd sync: region %d metadata updated but no local replica registered; region remains inactive until replica is created\n", region.ID)
			}
		}
	}

	locals := c.regionMgr.Regions()
	for _, local := range locals {
		if _, ok := seen[local.ID]; ok {
			continue
		}
		fmt.Printf("pd sync: region %d missing on PD; registering local metadata\n", local.ID)
		c.registerRegionWithPD(client, local)
	}

	if dirty {
		if err := c.persistRegions(); err != nil {
			fmt.Printf("pd sync: persist regions failed: %v\n", err)
		}
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
