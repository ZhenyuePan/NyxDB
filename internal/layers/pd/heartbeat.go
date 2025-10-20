package pd

import (
	"time"

	regionpkg "nyxdb/internal/region"
)

// RegionHeartbeat carries metadata about a region replica hosted on a store.
type RegionHeartbeat struct {
	Region       regionpkg.Region
	StoreID      uint64
	PeerID       uint64
	Role         regionpkg.PeerRole
	AppliedIndex uint64
}

// StoreHeartbeat aggregates information reported by a store to PD.
type StoreHeartbeat struct {
	StoreID   uint64
	Address   string
	Regions   []RegionHeartbeat
	Timestamp time.Time
}

// StoreHeartbeatResponse conveys scheduling信息（MVP 暂不下发命令）。
type StoreHeartbeatResponse struct{}

// Heartbeater abstracts PD services that consume store heartbeats.
type Heartbeater interface {
	HandleHeartbeat(StoreHeartbeat) StoreHeartbeatResponse
}

// MetadataClient exposes region registration and query capabilities.
type MetadataClient interface {
	Heartbeater
	RegisterRegion(regionpkg.Region) (regionpkg.Region, error)
	UpdateRegion(regionpkg.Region) (regionpkg.Region, error)
	RegionsByStore(storeID uint64) []RegionSnapshot
	AllocateTimestamps(count uint32) (uint64, uint32, error)
}
