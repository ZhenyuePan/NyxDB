package regions

import (
	"bytes"
	"sort"
	"sync"

	raftnode "nyxdb/internal/node"
	raftstorage "nyxdb/internal/raftstorage"
	regionpkg "nyxdb/internal/region"
)

const DefaultRegionID regionpkg.ID = 1

// Replica ties a region metadata entry to its Raft node and storage.
type Replica struct {
	Region  *regionpkg.Region
	Node    *raftnode.Node
	Storage *raftstorage.Storage
}

// Manager maintains region metadata and replicas for a store.
type Manager struct {
	mu       sync.RWMutex
	regions  map[regionpkg.ID]*regionpkg.Region
	replicas map[regionpkg.ID]*Replica
	nextID   regionpkg.ID
}

// NewManager constructs a manager with the default region registered.
func NewManager() *Manager {
	m := &Manager{
		regions:  make(map[regionpkg.ID]*regionpkg.Region),
		replicas: make(map[regionpkg.ID]*Replica),
		nextID:   DefaultRegionID + 1,
	}
	r := &regionpkg.Region{
		ID:    DefaultRegionID,
		Range: regionpkg.KeyRange{},
		Epoch: regionpkg.Epoch{Version: 1, ConfVersion: 1},
		State: regionpkg.StateActive,
	}
	m.regions[DefaultRegionID] = r
	return m
}

// Regions returns a snapshot of regions ordered by start key.
func (m *Manager) Regions() []regionpkg.Region {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]regionpkg.Region, 0, len(m.regions))
	for _, r := range m.regions {
		out = append(out, r.Clone())
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Range.Start, out[j].Range.Start) < 0
	})
	return out
}

// RegionForKey finds the region containing the provided key.
func (m *Manager) RegionForKey(key []byte) *regionpkg.Region {
	regions := m.Regions()
	var best *regionpkg.Region
	for i := range regions {
		r := &regions[i]
		if !r.ContainsKey(key) {
			continue
		}
		if best == nil || bytes.Compare(r.Range.Start, best.Range.Start) > 0 {
			clone := r.Clone()
			best = &clone
		}
	}
	return best
}

// Region returns the stored region metadata pointer (internal use).
func (m *Manager) Region(id regionpkg.ID) *regionpkg.Region {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.regions[id]
}

// CreateRegion allocates a new region with the provided key range.
func (m *Manager) CreateRegion(keyRange regionpkg.KeyRange) *regionpkg.Region {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.nextID
	m.nextID++
	r := &regionpkg.Region{
		ID:    id,
		Range: keyRange,
		Epoch: regionpkg.Epoch{Version: 1, ConfVersion: 1},
		State: regionpkg.StateActive,
	}
	m.regions[id] = r
	return r
}

// RegisterReplica stores metadata about a running replica.
func (m *Manager) RegisterReplica(rep *Replica) {
	if rep == nil || rep.Region == nil {
		return
	}
	m.mu.Lock()
	m.replicas[rep.Region.ID] = rep
	m.mu.Unlock()
}

// Replica returns the replica for region id.
func (m *Manager) Replica(id regionpkg.ID) *Replica {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.replicas[id]
}

// Replicas returns a snapshot of all replicas.
func (m *Manager) Replicas() []*Replica {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Replica, 0, len(m.replicas))
	for _, rep := range m.replicas {
		out = append(out, rep)
	}
	return out
}

// RemoveRegion deletes region metadata and replica, returning the removed replica (if any).
func (m *Manager) RemoveRegion(id regionpkg.ID) *Replica {
	m.mu.Lock()
	defer m.mu.Unlock()
	rep := m.replicas[id]
	delete(m.replicas, id)
	delete(m.regions, id)
	return rep
}

// ResetReplicas clears replica mappings (used during shutdown).
func (m *Manager) ResetReplicas() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replicas = make(map[regionpkg.ID]*Replica)
}
