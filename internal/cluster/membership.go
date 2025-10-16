package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const membersFileName = "members.json"

type memberStore struct {
	mu   sync.Mutex
	path string
}

func newMemberStore(dir string) (*memberStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("empty member store directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &memberStore{path: filepath.Join(dir, membersFileName)}, nil
}

func (s *memberStore) Load() (map[uint64]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[uint64]string{}, nil
		}
		return nil, err
	}
	var payload membershipPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	members := make(map[uint64]string, len(payload.Members))
	for _, m := range payload.Members {
		members[m.ID] = m.Address
	}
	return members, nil
}

func (s *memberStore) Save(members map[uint64]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	payload := membershipPayload{
		Members: make([]memberEntry, 0, len(members)),
	}
	for id, addr := range members {
		payload.Members = append(payload.Members, memberEntry{ID: id, Address: addr})
	}

	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

type membershipPayload struct {
	Members []memberEntry `json:"members"`
}

type memberEntry struct {
	ID      uint64 `json:"id"`
	Address string `json:"address"`
}
