package pd_test

import (
	"testing"
	"time"

	"nyxdb/internal/pd"
	regionpkg "nyxdb/internal/region"
)

func TestServiceHandleHeartbeat(t *testing.T) {
	svc := pd.NewService()

	hb := pd.StoreHeartbeat{
		StoreID:   1,
		Address:   "127.0.0.1:19001",
		Timestamp: time.Now(),
		Regions: []pd.RegionHeartbeat{
			{
				Region:  regionpkg.Region{ID: 1},
				StoreID: 1,
				Role:    regionpkg.Voter,
			},
		},
	}

	resp := svc.HandleHeartbeat(hb)
	if len(resp.Commands) != 0 {
		t.Fatalf("expected no commands, got %+v", resp.Commands)
	}

	stored, ok := svc.Store(1)
	if !ok {
		t.Fatalf("store 1 not recorded")
	}
	if stored.Address != hb.Address {
		t.Fatalf("unexpected address %s", stored.Address)
	}

	all := svc.Stores()
	if len(all) != 1 {
		t.Fatalf("expected 1 store, got %d", len(all))
	}
}
