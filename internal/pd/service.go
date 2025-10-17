package pd

import (
	"encoding/json"
	"fmt"
	"sync"

	db "nyxdb/internal/engine"
)

type heartbeatStore interface {
	Put(StoreHeartbeat) error
	ForEach(func(StoreHeartbeat) error) error
	Close() error
}

type bitcaskHeartbeatStore struct {
	db *db.DB
}

func newBitcaskHeartbeatStore(dir string) (*bitcaskHeartbeatStore, error) {
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = false
	opts.SyncWrites = true
	opts.ClusterConfig = nil

	database, err := db.Open(opts)
	if err != nil {
		return nil, err
	}
	return &bitcaskHeartbeatStore{db: database}, nil
}

func (b *bitcaskHeartbeatStore) Put(hb StoreHeartbeat) error {
	data, err := json.Marshal(hb)
	if err != nil {
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", storeKeyPrefix, hb.StoreID))
	return b.db.Put(key, data)
}

func (b *bitcaskHeartbeatStore) ForEach(fn func(StoreHeartbeat) error) error {
	iterOpts := db.DefaultIteratorOptions
	iterOpts.Prefix = []byte(storeKeyPrefix)
	iter := b.db.NewIterator(iterOpts)
	defer iter.Close()

	for iter.Rewind(); iter.Valid(); iter.Next() {
		value, err := iter.Value()
		if err != nil {
			return err
		}
		var hb StoreHeartbeat
		if err := json.Unmarshal(value, &hb); err != nil {
			return fmt.Errorf("decode heartbeat %s: %w", iter.Key(), err)
		}
		if err := fn(hb); err != nil {
			return err
		}
	}
	return nil
}

func (b *bitcaskHeartbeatStore) Close() error {
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
	store, err := newBitcaskHeartbeatStore(dir)
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
