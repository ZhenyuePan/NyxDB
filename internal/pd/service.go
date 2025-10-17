package pd

import (
	"encoding/json"
	"fmt"
	"sync"

	db "nyxdb/internal/engine"
)

// Service stores PD metadata, optionally persisting to NyxDB.
type Service struct {
	mu     sync.RWMutex
	stores map[uint64]StoreHeartbeat
	db     *db.DB
}

// NewService creates a pure in-memory PD service.
func NewService() *Service {
	return &Service{stores: make(map[uint64]StoreHeartbeat)}
}

// NewPersistentService persists heartbeats under dir so PD metadata survives restarts.
func NewPersistentService(dir string) (*Service, error) {
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = false
	opts.SyncWrites = true
	opts.ClusterConfig = nil

	database, err := db.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open pd storage: %w", err)
	}

	svc := &Service{stores: make(map[uint64]StoreHeartbeat), db: database}
	if err := svc.loadFromDB(); err != nil {
		_ = database.Close()
		return nil, err
	}
	return svc, nil
}

// HandleHeartbeat stores the latest heartbeat from a store.
func (s *Service) HandleHeartbeat(hb StoreHeartbeat) StoreHeartbeatResponse {
	s.mu.Lock()
	s.stores[hb.StoreID] = hb
	s.mu.Unlock()

	if s.db != nil {
		if err := s.persistStore(hb); err != nil {
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
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

const storeKeyPrefix = "store/"

func (s *Service) loadFromDB() error {
	if s.db == nil {
		return nil
	}
	iterOpts := db.DefaultIteratorOptions
	iterOpts.Prefix = []byte(storeKeyPrefix)
	iter := s.db.NewIterator(iterOpts)
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
		s.stores[hb.StoreID] = hb
	}
	return nil
}

func (s *Service) persistStore(hb StoreHeartbeat) error {
	if s.db == nil {
		return nil
	}
	data, err := json.Marshal(hb)
	if err != nil {
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", storeKeyPrefix, hb.StoreID))
	return s.db.Put(key, data)
}
