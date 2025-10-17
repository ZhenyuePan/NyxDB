package pd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	bolt "go.etcd.io/bbolt"
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
	mu     sync.RWMutex
	stores map[uint64]StoreHeartbeat
	store  heartbeatStore
}

// NewService creates a pure in-memory PD service.
func NewService() *Service {
	return &Service{stores: make(map[uint64]StoreHeartbeat)}
}

// NewPersistentService persists heartbeats under dir so PD metadata survives restarts.
func NewPersistentService(dir string) (*Service, error) {
	store, err := newBoltHeartbeatStore(dir)
	if err != nil {
		return nil, fmt.Errorf("open pd storage: %w", err)
	}

	svc := &Service{stores: make(map[uint64]StoreHeartbeat), store: store}
	if err := svc.loadFromStore(); err != nil {
		_ = store.Close()
		return nil, err
	}
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
