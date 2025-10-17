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
		hb.Regions = append(hb.Regions, RegionHeartbeat{
			Region:       regionpkg.Region{ID: regionpkg.ID(r.GetRegionId())},
			StoreID:      r.GetStoreId(),
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
			Role:         peerRoleToProto(r.Role),
			AppliedIndex: r.AppliedIndex,
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

func peerRoleToProto(role regionpkg.PeerRole) api.RegionRole {
	switch role {
	case regionpkg.Voter:
		return api.RegionRole_REGION_ROLE_VOTER
	case regionpkg.Learner:
		return api.RegionRole_REGION_ROLE_LEARNER
	default:
		return api.RegionRole_REGION_ROLE_UNSPECIFIED
	}
}
