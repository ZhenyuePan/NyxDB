package cluster

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	regionmgr "nyxdb/internal/cluster/regions"
	pd "nyxdb/internal/layers/pd"
	regionpkg "nyxdb/internal/region"
	utils "nyxdb/internal/utils"
	api "nyxdb/pkg/api"
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
	if client := c.metadataClient(); client != nil {
		c.registerRegionWithPD(client, clone)
	}
	// Broadcast to other members to create their local replicas (best-effort).
	go c.broadcastCreateRegionReplica(clone)
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
	var regionSnapshot *regionpkg.Region
	if meta := c.regionMgr.Region(id); meta != nil {
		clone := meta.Clone()
		regionSnapshot = &clone
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
	if regionSnapshot != nil {
		client := c.metadataClient()
		if client != nil {
			c.tombstoneRegionWithPD(client, *regionSnapshot)
		}
	}
	return utils.RemoveDir(regionBaseDir(c.options.DirPath, id))
}

// EnsureRegionReplica ensures the local store has a replica for the region, creating one if needed.
func (c *Cluster) EnsureRegionReplica(region regionpkg.Region) error {
	// Upsert region metadata
	c.regionMgr.UpsertRegion(region)
	if rep := c.replica(region.ID); rep != nil {
		// Peer already exists; best-effort PD update to reflect peers.
		if client := c.metadataClient(); client != nil {
			c.registerRegionWithPD(client, region)
		}
		return nil
	}
	// Create local replica
	_, err := c.createRegionReplica(region.ID, &region)
	if err != nil {
		return err
	}
	if err := c.persistRegions(); err != nil {
		return err
	}
	// Best-effort PD update to include this store's peer.
	if client := c.metadataClient(); client != nil {
		c.registerRegionWithPD(client, region)
	}
	return nil
}

func (c *Cluster) broadcastCreateRegionReplica(region regionpkg.Region) {
	c.membersMu.RLock()
	peers := make(map[uint64]string, len(c.members))
	for id, addr := range c.members {
		peers[id] = addr
	}
	c.membersMu.RUnlock()
	// Skip self
	delete(peers, c.nodeID)
	if len(peers) == 0 {
		return
	}
	// Prepare payload
	desc := pd.RegionToProto(region)
	req := &api.CreateRegionReplicaRequest{Region: desc}
	for _, addr := range peers {
		go func(target string) {
			// Best-effort dial; cluster nodeAddress may or may not match gRPC address in MVP.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			conn, err := grpc.DialContext(ctx, target, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				fmt.Printf("broadcast create replica to %s failed: %v\n", target, err)
				return
			}
			defer conn.Close()
			client := api.NewAdminClient(conn)
			if _, err := client.CreateRegionReplica(ctx, req); err != nil {
				fmt.Printf("admin CreateRegionReplica to %s failed: %v\n", target, err)
			}
		}(addr)
	}
}
