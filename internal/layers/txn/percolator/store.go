package percolator

import (
	db "nyxdb/internal/layers/engine"
	pd "nyxdb/internal/layers/pd"
)

// TSO provides monotonically increasing timestamps.
type TSO interface {
	Allocate(count uint32) (base uint64, allocated uint32, err error)
}

// Store applies committed mutations with a specific commit timestamp.
type Store interface {
	Apply(commitTs uint64, ops []db.ReplicatedOp) error
}

// EngineStore adapts the local engine DB to the transaction Store interface.
type EngineStore struct {
	db *db.DB
}

// NewEngineStore constructs a Store backed by an engine DB.
func NewEngineStore(database *db.DB) *EngineStore {
	return &EngineStore{db: database}
}

// Apply commits the replicated operations using the provided commit timestamp.
func (s *EngineStore) Apply(commitTs uint64, ops []db.ReplicatedOp) error {
	return s.db.ApplyReplicated(commitTs, ops)
}

// ServiceTSO adapts a PD service to the TSO interface.
type ServiceTSO struct {
	client pd.MetadataClient
}

// NewServiceTSO wraps a PD metadata client as a TSO provider.
func NewServiceTSO(client pd.MetadataClient) *ServiceTSO {
	return &ServiceTSO{client: client}
}

// Allocate obtains monotonically increasing timestamps from PD.
func (t *ServiceTSO) Allocate(count uint32) (uint64, uint32, error) {
	return t.client.AllocateTimestamps(count)
}
