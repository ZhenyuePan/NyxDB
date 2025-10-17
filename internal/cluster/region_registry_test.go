package cluster

import (
	"testing"

	regionpkg "nyxdb/internal/region"
)

func TestRegionForKey(t *testing.T) {
	c := &Cluster{
		regions: make(map[regionpkg.ID]*regionpkg.Region),
	}
	c.initDefaultRegions()

	// Register a second region for keys >= "m".
	c.regionMu.Lock()
	c.regions[regionpkg.ID(2)] = &regionpkg.Region{
		ID:    2,
		Range: regionpkg.KeyRange{Start: []byte("m"), End: nil},
		Epoch: regionpkg.Epoch{Version: 1, ConfVersion: 1},
		State: regionpkg.StateActive,
	}
	c.regionMu.Unlock()

	cases := []struct {
		key      string
		expectID regionpkg.ID
	}{
		{key: "alpha", expectID: 1},
		{key: "monkey", expectID: 2},
		{key: "zebra", expectID: 2},
	}

	for _, tc := range cases {
		r := c.RegionForKey([]byte(tc.key))
		if r == nil {
			t.Fatalf("expected region for key %q", tc.key)
		}
		if r.ID != tc.expectID {
			t.Fatalf("key %q mapped to region %d, want %d", tc.key, r.ID, tc.expectID)
		}
	}
}
