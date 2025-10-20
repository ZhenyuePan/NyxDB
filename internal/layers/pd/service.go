package pd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	bolt "go.etcd.io/bbolt"
	regionpkg "nyxdb/internal/region"
)

type heartbeatStore interface {
	Put(StoreHeartbeat) error
	ForEach(func(StoreHeartbeat) error) error
	Close() error
}

type boltHeartbeatStore struct {
	db *bolt.DB
}

const (
	boltFileName  = "pd.meta"
	boltBucketKey = "stores"
)

func newBoltHeartbeatStore(dir string) (*boltHeartbeatStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("pd directory is empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	filePath := filepath.Join(dir, boltFileName)
	db, err := bolt.Open(filePath, 0o600, &bolt.Options{Timeout: 0})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(boltBucketKey))
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &boltHeartbeatStore{db: db}, nil
}

func (b *boltHeartbeatStore) Put(hb StoreHeartbeat) error {
	data, err := json.Marshal(hb)
	if err != nil {
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", storeKeyPrefix, hb.StoreID))
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(boltBucketKey))
		if bucket == nil {
			return fmt.Errorf("bucket %s missing", boltBucketKey)
		}
		return bucket.Put(key, data)
	})
}

func (b *boltHeartbeatStore) ForEach(fn func(StoreHeartbeat) error) error {
	return b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(boltBucketKey))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(_, v []byte) error {
			var hb StoreHeartbeat
			if err := json.Unmarshal(v, &hb); err != nil {
				return err
			}
			return fn(hb)
		})
	})
}

func (b *boltHeartbeatStore) Close() error {
	return b.db.Close()
}

// Service stores PD metadata, optionally persisting to NyxDB.
type Service struct {
	mu            sync.RWMutex
	stores        map[uint64]StoreHeartbeat
	store         heartbeatStore
	regions       map[uint64]*regionMeta
	regionEntries map[uint64]regionpkg.Region
	regionStore   regionMetadataStore
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
	svc.rebuildRegionsLocked()
	return svc
}

// NewPersistentService persists heartbeats under dir so PD metadata survives restarts.
func NewPersistentService(dir string) (*Service, error) {
	store, err := newBoltHeartbeatStore(dir)
	if err != nil {
		return nil, fmt.Errorf("open pd storage: %w", err)
	}

	regionStore, err := newBoltRegionStore(dir)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("open pd region storage: %w", err)
	}

	svc := &Service{
		stores:        make(map[uint64]StoreHeartbeat),
		store:         store,
		regions:       make(map[uint64]*regionMeta),
		regionEntries: make(map[uint64]regionpkg.Region),
		regionStore:   regionStore,
	}
	if err := svc.loadFromStore(); err != nil {
		_ = store.Close()
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
	s.mu.Unlock()

	if s.store != nil {
		if err := s.store.Put(hb); err != nil {
			fmt.Printf("pd: persist heartbeat error: %v\n", err)
		}
	}
	s.mu.Lock()
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
	if s.store != nil {
		if e := s.store.Close(); err == nil {
			err = e
		}
	}
	if s.regionStore != nil {
		if e := s.regionStore.Close(); err == nil {
			err = e
		}
	}
	return err
}

const (
	storeKeyPrefix  = "store/"
	regionKeyPrefix = "region/"
)

func (s *Service) loadFromStore() error {
	if s.store != nil {
		if err := s.store.ForEach(func(hb StoreHeartbeat) error {
			s.stores[hb.StoreID] = hb
			return nil
		}); err != nil {
			return err
		}
	}
	if s.regionStore != nil {
		if err := s.regionStore.ForEach(func(region regionpkg.Region) error {
			s.regionEntries[uint64(region.ID)] = region.Clone()
			return nil
		}); err != nil {
			return err
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
func (s *Service) RegionsByStore(storeID uint64) ([]RegionSnapshot, error) {
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
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Region.ID < snapshots[j].Region.ID
	})
	return snapshots, nil
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
		copy := hb
		copy.Region = regionClone
		peers = append(peers, copy)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].StoreID < peers[j].StoreID })
	return RegionSnapshot{Region: regionClone, Peers: peers}
}

func (s *Service) rebuildRegionsLocked() {
	regions := make(map[uint64]*regionMeta)
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
				clone := rh.Region.Clone()
				meta = &regionMeta{
					region: clone,
					peers:  make(map[uint64]RegionHeartbeat),
				}
			}
			mergeRegionMeta(&meta.region, rh.Region)
			ensureRegionPeer(&meta.region, rh)
			meta.peers[rh.StoreID] = RegionHeartbeat{
				Region:       meta.region.Clone(),
				StoreID:      rh.StoreID,
				PeerID:       rh.PeerID,
				Role:         rh.Role,
				AppliedIndex: rh.AppliedIndex,
			}
			regions[regionID] = meta
		}
	}
	s.regions = regions
}

func mergeRegionMeta(dst *regionpkg.Region, incoming regionpkg.Region) {
	if dst == nil {
		return
	}
	if len(incoming.Range.Start) > 0 {
		dst.Range.Start = append([]byte(nil), incoming.Range.Start...)
	}
	if len(incoming.Range.End) > 0 {
		dst.Range.End = append([]byte(nil), incoming.Range.End...)
	}
	if incoming.Epoch.Version >= dst.Epoch.Version {
		dst.Epoch.Version = incoming.Epoch.Version
	}
	if incoming.Epoch.ConfVersion >= dst.Epoch.ConfVersion {
		dst.Epoch.ConfVersion = incoming.Epoch.ConfVersion
	}
	if incoming.State != 0 {
		dst.State = incoming.State
	}
	if incoming.Leader != 0 {
		dst.Leader = incoming.Leader
	}
}

func ensureRegionPeer(region *regionpkg.Region, hb RegionHeartbeat) {
	if region == nil {
		return
	}
	peerID := hb.PeerID
	if peerID == 0 {
		peerID = (uint64(region.ID) << 32) | hb.StoreID
	}
	for i := range region.Peers {
		if region.Peers[i].StoreID == hb.StoreID {
			region.Peers[i].ID = peerID
			region.Peers[i].Role = hb.Role
			return
		}
	}
	region.Peers = append(region.Peers, regionpkg.Peer{ID: peerID, StoreID: hb.StoreID, Role: hb.Role})
	if region.Leader == 0 && hb.Role == regionpkg.Voter {
		region.Leader = peerID
	}
}

func (s *Service) RegionsSnapshot() []RegionSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshots := make([]RegionSnapshot, 0, len(s.regions))
	for _, meta := range s.regions {
		snapshots = append(snapshots, meta.snapshot())
	}
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].Region.ID < snapshots[j].Region.ID })
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
