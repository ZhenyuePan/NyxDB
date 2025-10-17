package pd

import (
	"sync"
)

// Service is an in-memory PD prototype that records store heartbeats.
type Service struct {
	mu     sync.RWMutex
	stores map[uint64]StoreHeartbeat
}

// NewService creates a new in-memory PD service.
func NewService() *Service {
	return &Service{stores: make(map[uint64]StoreHeartbeat)}
}

// HandleHeartbeat stores the latest heartbeat from a store.
func (s *Service) HandleHeartbeat(hb StoreHeartbeat) StoreHeartbeatResponse {
	s.mu.Lock()
	s.stores[hb.StoreID] = hb
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
