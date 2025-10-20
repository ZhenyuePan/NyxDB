package pd

import (
	"bytes"
	"encoding/json"
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
	mu      sync.RWMutex
	stores  map[uint64]StoreHeartbeat
	store   heartbeatStore
	regions map[uint64]*regionMeta
}

// NewService creates a pure in-memory PD service.
func NewService() *Service {
	svc := &Service{stores: make(map[uint64]StoreHeartbeat), regions: make(map[uint64]*regionMeta)}
	svc.rebuildRegionsLocked()
	return svc
}

// NewPersistentService persists heartbeats under dir so PD metadata survives restarts.
func NewPersistentService(dir string) (*Service, error) {
	store, err := newBoltHeartbeatStore(dir)
	if err != nil {
		return nil, fmt.Errorf("open pd storage: %w", err)
	}

	svc := &Service{stores: make(map[uint64]StoreHeartbeat), store: store, regions: make(map[uint64]*regionMeta)}
	if err := svc.loadFromStore(); err != nil {
		_ = store.Close()
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
	if s.store != nil {
		return s.store.Close()
	}
	return nil
}

const storeKeyPrefix = "store/"

func (s *Service) loadFromStore() error {
	if s.store == nil {
		return nil
	}
	return s.store.ForEach(func(hb StoreHeartbeat) error {
		s.stores[hb.StoreID] = hb
		return nil
	})
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
	peers := make([]RegionHeartbeat, 0, len(m.peers))
	for _, hb := range m.peers {
		peers = append(peers, hb)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].StoreID < peers[j].StoreID })
	return RegionSnapshot{Region: m.region.Clone(), Peers: peers}
}

func (s *Service) rebuildRegionsLocked() {
	if s.regions == nil {
		s.regions = make(map[uint64]*regionMeta)
	}
	s.regions = make(map[uint64]*regionMeta)
	for _, hb := range s.stores {
		for _, rh := range hb.Regions {
			regionID := uint64(rh.Region.ID)
			meta := s.regions[regionID]
			if meta == nil {
				meta = &regionMeta{
					region: rh.Region.Clone(),
					peers:  make(map[uint64]RegionHeartbeat),
				}
				s.regions[regionID] = meta
			} else {
				mergeRegionMeta(&meta.region, rh.Region)
			}
			ensureRegionPeer(&meta.region, rh)
			meta.peers[rh.StoreID] = rh
		}
	}
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
