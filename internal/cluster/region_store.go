package cluster

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	regionpkg "nyxdb/internal/region"
)

const regionsFileName = "regions.json"

type regionStore struct {
	mu   sync.Mutex
	path string
}

func newRegionStore(dir string) (*regionStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("empty region store directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &regionStore{path: filepath.Join(dir, regionsFileName)}, nil
}

func (s *regionStore) Load() ([]regionpkg.Region, regionpkg.ID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, regionpkg.ID(0), nil
		}
		return nil, 0, err
	}
	var payload regionPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, 0, err
	}
	regions := make([]regionpkg.Region, 0, len(payload.Regions))
	for _, entry := range payload.Regions {
		r, err := entry.toRegion()
		if err != nil {
			return nil, 0, err
		}
		regions = append(regions, r)
	}
	return regions, regionpkg.ID(payload.NextId), nil
}

func (s *regionStore) Save(regions []regionpkg.Region, nextID regionpkg.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	payload := regionPayload{NextId: uint64(nextID)}
	payload.Regions = make([]regionEntry, 0, len(regions))
	for _, r := range regions {
		payload.Regions = append(payload.Regions, makeRegionEntry(r))
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

type regionPayload struct {
	NextId  uint64        `json:"next_id"`
	Regions []regionEntry `json:"regions"`
}

type regionEntry struct {
	ID          uint64       `json:"id"`
	StartKey    string       `json:"start_key"`
	EndKey      string       `json:"end_key"`
	Version     uint64       `json:"version"`
	ConfVersion uint64       `json:"conf_version"`
	State       int          `json:"state"`
	Leader      uint64       `json:"leader"`
	Peers       []regionPeer `json:"peers"`
}

type regionPeer struct {
	ID      uint64 `json:"id"`
	StoreID uint64 `json:"store_id"`
	Role    int    `json:"role"`
}

func makeRegionEntry(region regionpkg.Region) regionEntry {
	entry := regionEntry{
		ID:          uint64(region.ID),
		StartKey:    base64.StdEncoding.EncodeToString(region.Range.Start),
		EndKey:      base64.StdEncoding.EncodeToString(region.Range.End),
		Version:     region.Epoch.Version,
		ConfVersion: region.Epoch.ConfVersion,
		State:       int(region.State),
		Leader:      region.Leader,
	}
	if len(region.Peers) > 0 {
		entry.Peers = make([]regionPeer, 0, len(region.Peers))
		for _, p := range region.Peers {
			entry.Peers = append(entry.Peers, regionPeer{ID: p.ID, StoreID: p.StoreID, Role: int(p.Role)})
		}
	}
	return entry
}

func (e regionEntry) toRegion() (regionpkg.Region, error) {
	start, err := base64.StdEncoding.DecodeString(e.StartKey)
	if err != nil {
		return regionpkg.Region{}, err
	}
	end, err := base64.StdEncoding.DecodeString(e.EndKey)
	if err != nil {
		return regionpkg.Region{}, err
	}
	region := regionpkg.Region{
		ID: regionpkg.ID(e.ID),
		Range: regionpkg.KeyRange{
			Start: start,
			End:   end,
		},
		Epoch: regionpkg.Epoch{
			Version:     e.Version,
			ConfVersion: e.ConfVersion,
		},
		State:  regionpkg.State(e.State),
		Leader: e.Leader,
	}
	if len(e.Peers) > 0 {
		region.Peers = make([]regionpkg.Peer, 0, len(e.Peers))
		for _, p := range e.Peers {
			region.Peers = append(region.Peers, regionpkg.Peer{ID: p.ID, StoreID: p.StoreID, Role: regionpkg.PeerRole(p.Role)})
		}
	}
	return region, nil
}
