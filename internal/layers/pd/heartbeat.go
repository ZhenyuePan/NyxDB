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

// Command represents a PD scheduling command (placeholder).
type Command struct {
	Type string
}

// StoreHeartbeatResponse conveys scheduling decisions back to the store.
type StoreHeartbeatResponse struct {
	Commands []Command
}

// Heartbeater abstracts PD services that consume store heartbeats.
type Heartbeater interface {
	HandleHeartbeat(StoreHeartbeat) StoreHeartbeatResponse
}
