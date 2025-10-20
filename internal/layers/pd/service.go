package pd

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"

	regionpkg "nyxdb/internal/region"
)

// Service stores PD metadata, optionally persisting to NyxDB.
type Service struct {
	mu            sync.RWMutex
	stores        map[uint64]StoreHeartbeat
	regions       map[uint64]*regionMeta
	regionEntries map[uint64]regionpkg.Region
	regionStore   regionMetadataStore
	tsoLast       uint64
}

var (
	// ErrRegionExists indicates the region is already present in PD metadata.
	ErrRegionExists = errors.New("pd: region already registered")
	// ErrRegionNotFound indicates the region metadata is unknown to PD.
	ErrRegionNotFound = errors.New("pd: region not registered")
)

// NewService creates a pure in-memory PD service.
func NewService() *Service {
	svc := &Service{
		stores:        make(map[uint64]StoreHeartbeat),
		regions:       make(map[uint64]*regionMeta),
		regionEntries: make(map[uint64]regionpkg.Region),
	}
	return svc
}

// NewPersistentService persists heartbeats under dir so PD metadata survives restarts.
func NewPersistentService(dir string) (*Service, error) {
	regionStore, err := newBoltRegionStore(dir)
	if err != nil {
		return nil, fmt.Errorf("open pd region storage: %w", err)
	}

	svc := &Service{
		stores:        make(map[uint64]StoreHeartbeat),
		regions:       make(map[uint64]*regionMeta),
		regionEntries: make(map[uint64]regionpkg.Region),
		regionStore:   regionStore,
	}
	if err := svc.loadFromStore(); err != nil {
		_ = regionStore.Close()
		return nil, err
	}
	svc.rebuildRegionsLocked()
	return svc, nil
}

// HandleHeartbeat stores the latest heartbeat from a store.
func (s *Service) HandleHeartbeat(hb StoreHeartbeat) StoreHeartbeatResponse {
	s.mu.Lock()
	s.stores[hb.StoreID] = hb
	s.rebuildRegionsLocked()
	s.mu.Unlock()
	return StoreHeartbeatResponse{}
}

// Store returns the last heartbeat for a given store.
func (s *Service) Store(id uint64) (StoreHeartbeat, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	hb, ok := s.stores[id]
	return hb, ok
}

// Stores returns a snapshot of all known store heartbeats.
func (s *Service) Stores() []StoreHeartbeat {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]StoreHeartbeat, 0, len(s.stores))
	for _, hb := range s.stores {
		out = append(out, hb)
	}
	return out
}

// Close releases persistent resources if present.
func (s *Service) Close() error {
	var err error
	if s.regionStore != nil {
		if e := s.regionStore.Close(); err == nil {
			err = e
		}
	}
	return err
}

func (s *Service) loadFromStore() error {
	if s.regionStore != nil {
		if err := s.regionStore.ForEach(func(region regionpkg.Region) error {
			s.regionEntries[uint64(region.ID)] = region.Clone()
			return nil
		}); err != nil {
			return err
		}
		if value, err := s.regionStore.LoadTSO(); err != nil {
			return err
		} else {
			s.tsoLast = value
		}
	}
	return nil
}

// RegisterRegion persists a new region definition into PD metadata.
func (s *Service) RegisterRegion(region regionpkg.Region) (regionpkg.Region, error) {
	if region.ID == 0 {
		return regionpkg.Region{}, fmt.Errorf("region id is zero")
	}
	clone := region.Clone()
	id := uint64(clone.ID)

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.regionEntries[id]; exists {
		return regionpkg.Region{}, fmt.Errorf("%w: %d", ErrRegionExists, id)
	}
	if err := s.upsertRegionLocked(clone); err != nil {
		return regionpkg.Region{}, err
	}
	s.rebuildRegionsLocked()
	return clone, nil
}

// UpdateRegion upserts metadata for an existing region.
func (s *Service) UpdateRegion(region regionpkg.Region) (regionpkg.Region, error) {
	if region.ID == 0 {
		return regionpkg.Region{}, fmt.Errorf("region id is zero")
	}
	clone := region.Clone()
	id := uint64(clone.ID)

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.regionEntries[id]; !exists {
		return regionpkg.Region{}, fmt.Errorf("%w: %d", ErrRegionNotFound, id)
	}
	if err := s.upsertRegionLocked(clone); err != nil {
		return regionpkg.Region{}, err
	}
	s.rebuildRegionsLocked()
	return clone, nil
}

func (s *Service) upsertRegionLocked(region regionpkg.Region) error {
	id := uint64(region.ID)
	prev, hadPrev := s.regionEntries[id]
	s.regionEntries[id] = region
	if s.regionStore != nil {
		if err := s.regionStore.Put(region); err != nil {
			if hadPrev {
				s.regionEntries[id] = prev
			} else {
				delete(s.regionEntries, id)
			}
			return fmt.Errorf("persist region %d: %w", region.ID, err)
		}
	}
	return nil
}

// AllocateTimestamps returns a monotonically increasing range of timestamps.
func (s *Service) AllocateTimestamps(count uint32) (uint64, uint32, error) {
	if count == 0 {
		count = 1
	}
	s.mu.Lock()
	base := s.tsoLast + 1
	s.tsoLast = base + uint64(count) - 1
	var err error
	if s.regionStore != nil {
		err = s.regionStore.SaveTSO(s.tsoLast)
	}
	s.mu.Unlock()
	return base, count, err
}

// RegionMetadata returns the persisted metadata for a region.
func (s *Service) RegionMetadata(id regionpkg.ID) (regionpkg.Region, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	region, ok := s.regionEntries[uint64(id)]
	if !ok {
		return regionpkg.Region{}, false
	}
	return region.Clone(), true
}

// RegionsByStore returns region snapshots containing replicas on the given store.
func (s *Service) RegionsByStore(storeID uint64) []RegionSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshots := make([]RegionSnapshot, 0)
	for _, meta := range s.regions {
		if storeID == 0 {
			snapshots = append(snapshots, meta.snapshot())
			continue
		}
		if _, ok := meta.peers[storeID]; ok {
			snapshots = append(snapshots, meta.snapshot())
		}
	}
	sortSnapshots(snapshots)
	return snapshots
}

// RegionSnapshot captures metadata about a region and its replicas.
type RegionSnapshot struct {
	Region regionpkg.Region
	Peers  []RegionHeartbeat
}

type regionMeta struct {
	region regionpkg.Region
	peers  map[uint64]RegionHeartbeat
}

func (m *regionMeta) snapshot() RegionSnapshot {
	regionClone := m.region.Clone()
	peers := make([]RegionHeartbeat, 0, len(m.peers))
	for _, hb := range m.peers {
		hbCopy := hb
		hbCopy.Region = regionClone
		peers = append(peers, hbCopy)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].StoreID < peers[j].StoreID })
	return RegionSnapshot{Region: regionClone, Peers: peers}
}

func (s *Service) rebuildRegionsLocked() {
	regions := make(map[uint64]*regionMeta, len(s.regionEntries))
	for id, region := range s.regionEntries {
		clone := region.Clone()
		meta := &regionMeta{
			region: clone,
			peers:  make(map[uint64]RegionHeartbeat),
		}
		for _, peer := range clone.Peers {
			meta.peers[peer.StoreID] = RegionHeartbeat{
				Region:  clone,
				StoreID: peer.StoreID,
				PeerID:  peer.ID,
				Role:    peer.Role,
			}
		}
		regions[id] = meta
	}
	for _, hb := range s.stores {
		for _, rh := range hb.Regions {
			regionID := uint64(rh.Region.ID)
			meta := regions[regionID]
			if meta == nil {
				continue
			}
			peer, ok := meta.peers[rh.StoreID]
			if !ok {
				continue
			}
			peer.AppliedIndex = rh.AppliedIndex
			meta.peers[rh.StoreID] = peer
		}
	}
	s.regions = regions
}

func (s *Service) RegionsSnapshot() []RegionSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshots := make([]RegionSnapshot, 0, len(s.regions))
	for _, meta := range s.regions {
		snapshots = append(snapshots, meta.snapshot())
	}
	sortSnapshots(snapshots)
	return snapshots
}

func (s *Service) RegionSnapshot(regionID uint64) (RegionSnapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	meta, ok := s.regions[regionID]
	if !ok {
		return RegionSnapshot{}, false
	}
	return meta.snapshot(), true
}

func (s *Service) RegionByKey(key []byte) (RegionSnapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var best *regionMeta
	for _, meta := range s.regions {
		if meta == nil || !meta.region.ContainsKey(key) {
			continue
		}
		if best == nil {
			best = meta
			continue
		}
		if bytes.Compare(meta.region.Range.Start, best.region.Range.Start) > 0 {
			best = meta
		}
	}
	if best == nil {
		return RegionSnapshot{}, false
	}
	return best.snapshot(), true
}

func sortSnapshots(snaps []RegionSnapshot) {
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].Region.ID < snaps[j].Region.ID })
}
