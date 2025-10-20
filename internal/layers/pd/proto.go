package pd

import (
	"fmt"
	"time"

	regionpkg "nyxdb/internal/region"
	api "nyxdb/pkg/api"
)

func ProtoToStoreHeartbeat(p *api.StoreHeartbeatProto) (StoreHeartbeat, error) {
	if p == nil {
		return StoreHeartbeat{}, fmt.Errorf("heartbeat proto is nil")
	}
	hb := StoreHeartbeat{
		StoreID:   p.GetStoreId(),
		Address:   p.GetAddress(),
		Timestamp: time.UnixMilli(p.GetTimestampMs()),
	}
	for _, r := range p.GetRegions() {
		regionMeta := ProtoToRegion(r.GetRegion())
		if regionMeta.ID == 0 {
			regionMeta.ID = regionpkg.ID(r.GetRegionId())
		}
		hb.Regions = append(hb.Regions, RegionHeartbeat{
			Region:       regionMeta,
			StoreID:      r.GetStoreId(),
			PeerID:       r.GetPeerId(),
			Role:         protoRoleToPeerRole(r.GetRole()),
			AppliedIndex: r.GetAppliedIndex(),
		})
	}
	return hb, nil
}

func StoreHeartbeatToProto(hb StoreHeartbeat) *api.StoreHeartbeatProto {
	regions := make([]*api.RegionReplicaDescriptor, 0, len(hb.Regions))
	for _, r := range hb.Regions {
		regions = append(regions, &api.RegionReplicaDescriptor{
			RegionId:     uint64(r.Region.ID),
			StoreId:      r.StoreID,
			PeerId:       r.PeerID,
			Role:         PeerRoleToProto(r.Role),
			AppliedIndex: r.AppliedIndex,
			Region:       RegionToProto(r.Region),
		})
	}
	return &api.StoreHeartbeatProto{
		StoreId:     hb.StoreID,
		Address:     hb.Address,
		Regions:     regions,
		TimestampMs: hb.Timestamp.UnixMilli(),
	}
}

func protoRoleToPeerRole(role api.RegionRole) regionpkg.PeerRole {
	switch role {
	case api.RegionRole_REGION_ROLE_VOTER:
		return regionpkg.Voter
	case api.RegionRole_REGION_ROLE_LEARNER:
		return regionpkg.Learner
	default:
		return regionpkg.Voter
	}
}

func PeerRoleToProto(role regionpkg.PeerRole) api.RegionRole {
	switch role {
	case regionpkg.Voter:
		return api.RegionRole_REGION_ROLE_VOTER
	case regionpkg.Learner:
		return api.RegionRole_REGION_ROLE_LEARNER
	default:
		return api.RegionRole_REGION_ROLE_UNSPECIFIED
	}
}

// RegionToProto converts a Region metadata object into its protobuf counterpart.
func RegionToProto(region regionpkg.Region) *api.RegionDescriptor {
	desc := &api.RegionDescriptor{
		RegionId:     uint64(region.ID),
		StartKey:     append([]byte(nil), region.Range.Start...),
		EndKey:       append([]byte(nil), region.Range.End...),
		Version:      region.Epoch.Version,
		ConfVersion:  region.Epoch.ConfVersion,
		State:        regionStateToProto(region.State),
		LeaderPeerId: region.Leader,
	}
	return desc
}

// ProtoToRegion converts a protobuf RegionDescriptor into engine metadata.
func ProtoToRegion(desc *api.RegionDescriptor) regionpkg.Region {
	if desc == nil {
		return regionpkg.Region{}
	}
	return regionpkg.Region{
		ID: regionpkg.ID(desc.GetRegionId()),
		Range: regionpkg.KeyRange{
			Start: append([]byte(nil), desc.GetStartKey()...),
			End:   append([]byte(nil), desc.GetEndKey()...),
		},
		Epoch: regionpkg.Epoch{
			Version:     desc.GetVersion(),
			ConfVersion: desc.GetConfVersion(),
		},
		State:  protoStateToRegionState(desc.GetState()),
		Leader: desc.GetLeaderPeerId(),
	}
}

func regionStateToProto(state regionpkg.State) api.RegionState {
	switch state {
	case regionpkg.StateActive:
		return api.RegionState_REGION_STATE_ACTIVE
	case regionpkg.StateSplitting:
		return api.RegionState_REGION_STATE_SPLITTING
	case regionpkg.StateMerging:
		return api.RegionState_REGION_STATE_MERGING
	case regionpkg.StateTombstone:
		return api.RegionState_REGION_STATE_TOMBSTONE
	default:
		return api.RegionState_REGION_STATE_UNSPECIFIED
	}
}

func protoStateToRegionState(state api.RegionState) regionpkg.State {
	switch state {
	case api.RegionState_REGION_STATE_ACTIVE:
		return regionpkg.StateActive
	case api.RegionState_REGION_STATE_SPLITTING:
		return regionpkg.StateSplitting
	case api.RegionState_REGION_STATE_MERGING:
		return regionpkg.StateMerging
	case api.RegionState_REGION_STATE_TOMBSTONE:
		return regionpkg.StateTombstone
	default:
		return regionpkg.StateActive
	}
}

// RegionMetadataToProto converts persisted region metadata into API form.
func RegionMetadataToProto(region regionpkg.Region) *api.RegionMetadata {
	meta := &api.RegionMetadata{
		Region: RegionToProto(region),
		Peers:  make([]*api.RegionPeerMetadata, 0, len(region.Peers)),
	}
	for _, peer := range region.Peers {
		meta.Peers = append(meta.Peers, &api.RegionPeerMetadata{
			PeerId:  peer.ID,
			StoreId: peer.StoreID,
			Role:    PeerRoleToProto(peer.Role),
		})
	}
	return meta
}

// ProtoToRegionMetadata converts API metadata into the internal Region structure.
func ProtoToRegionMetadata(meta *api.RegionMetadata) (regionpkg.Region, error) {
	if meta == nil {
		return regionpkg.Region{}, fmt.Errorf("region metadata is nil")
	}
	region := ProtoToRegion(meta.GetRegion())
	if region.ID == 0 {
		return regionpkg.Region{}, fmt.Errorf("region metadata missing region id")
	}
	peers := meta.GetPeers()
	if len(peers) > 0 {
		region.Peers = make([]regionpkg.Peer, 0, len(peers))
		for _, peer := range peers {
			region.Peers = append(region.Peers, regionpkg.Peer{
				ID:      peer.GetPeerId(),
				StoreID: peer.GetStoreId(),
				Role:    protoRoleToPeerRole(peer.GetRole()),
			})
		}
	}
	return region, nil
}

// RegionSnapshotToProto converts a snapshot into protobuf form.
func RegionSnapshotToProto(snapshot RegionSnapshot) *api.RegionSnapshot {
	return &api.RegionSnapshot{
		Region:   RegionToProto(snapshot.Region),
		Replicas: RegionHeartbeatsToProto(snapshot.Peers),
	}
}

// RegionHeartbeatsToProto converts a slice of heartbeats into replica descriptors.
func RegionHeartbeatsToProto(peers []RegionHeartbeat) []*api.RegionReplicaDescriptor {
	replicas := make([]*api.RegionReplicaDescriptor, 0, len(peers))
	for _, peer := range peers {
		replicas = append(replicas, &api.RegionReplicaDescriptor{
			RegionId:     uint64(peer.Region.ID),
			StoreId:      peer.StoreID,
			PeerId:       peer.PeerID,
			Role:         PeerRoleToProto(peer.Role),
			AppliedIndex: peer.AppliedIndex,
			Region:       RegionToProto(peer.Region),
		})
	}
	return replicas
}

// ProtoToRegionSnapshot converts a protobuf snapshot into internal structures.
func ProtoToRegionSnapshot(snapshot *api.RegionSnapshot) (RegionSnapshot, error) {
	if snapshot == nil {
		return RegionSnapshot{}, fmt.Errorf("region snapshot is nil")
	}
	region := ProtoToRegion(snapshot.GetRegion())
	replicas := snapshot.GetReplicas()
	peers := make([]RegionHeartbeat, 0, len(replicas))
	for _, replica := range replicas {
		regionMeta := ProtoToRegion(replica.GetRegion())
		if regionMeta.ID == 0 {
			regionMeta.ID = regionpkg.ID(replica.GetRegionId())
		}
		peers = append(peers, RegionHeartbeat{
			Region:       regionMeta,
			StoreID:      replica.GetStoreId(),
			PeerID:       replica.GetPeerId(),
			Role:         protoRoleToPeerRole(replica.GetRole()),
			AppliedIndex: replica.GetAppliedIndex(),
		})
	}
	return RegionSnapshot{Region: region, Peers: peers}, nil
}
