package pd_test

import (
	"testing"
	"time"

	"nyxdb/internal/layers/pd"
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
				PeerID:  0x0000000100000001,
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

	snapshot, ok := svc.RegionSnapshot(1)
	if !ok {
		t.Fatalf("region snapshot missing")
	}
	if snapshot.Region.ID != 1 || len(snapshot.Peers) != 1 || snapshot.Peers[0].PeerID == 0 {
		t.Fatalf("unexpected region snapshot: %+v", snapshot)
	}
}

func TestPersistentServiceBootstrap(t *testing.T) {
	dir := t.TempDir()
	svc, err := pd.NewPersistentService(dir)
	if err != nil {
		t.Fatalf("new persistent service: %v", err)
	}

	hb := pd.StoreHeartbeat{
		StoreID:   42,
		Address:   "10.0.0.1:10001",
		Timestamp: time.Now(),
		Regions: []pd.RegionHeartbeat{
			{Region: regionpkg.Region{ID: 5}, StoreID: 42, PeerID: 0x000000050000002a, Role: regionpkg.Voter},
		},
	}
	svc.HandleHeartbeat(hb)

	if err := svc.Close(); err != nil {
		t.Fatalf("close service: %v", err)
	}

	svc2, err := pd.NewPersistentService(dir)
	if err != nil {
		t.Fatalf("reopen persistent service: %v", err)
	}
	defer svc2.Close()

	stored, ok := svc2.Store(42)
	if !ok {
		t.Fatalf("expected store 42 after restart")
	}
	if stored.Address != hb.Address {
		t.Fatalf("unexpected addr %s", stored.Address)
	}
	if len(stored.Regions) != 1 || stored.Regions[0].Region.ID != 5 {
		t.Fatalf("unexpected regions: %+v", stored.Regions)
	}

	snapshot, ok := svc2.RegionSnapshot(5)
	if !ok {
		t.Fatalf("expected region snapshot after restart")
	}
	if len(snapshot.Peers) != 1 || snapshot.Peers[0].StoreID != 42 {
		t.Fatalf("unexpected region snapshot: %+v", snapshot)
	}
}

func TestServiceRegisterAndUpdateRegion(t *testing.T) {
	svc := pd.NewService()

	region := regionpkg.Region{
		ID: 7,
		Epoch: regionpkg.Epoch{
			Version:     1,
			ConfVersion: 1,
		},
		Peers: []regionpkg.Peer{
			{ID: 0x0000000700000001, StoreID: 1, Role: regionpkg.Voter},
			{ID: 0x0000000700000002, StoreID: 2, Role: regionpkg.Voter},
		},
		State:  regionpkg.StateActive,
		Leader: 0x0000000700000001,
	}

	if _, err := svc.RegisterRegion(region); err != nil {
		t.Fatalf("register region: %v", err)
	}
	if _, err := svc.RegisterRegion(region); !pd.IsRegionExistsError(err) {
		t.Fatalf("expected exists error, got %v", err)
	}

	region.Leader = 0x0000000700000002
	if _, err := svc.UpdateRegion(region); err != nil {
		t.Fatalf("update region: %v", err)
	}

	snapshot, ok := svc.RegionSnapshot(uint64(region.ID))
	if !ok {
		t.Fatalf("region snapshot missing")
	}
	if snapshot.Region.Leader != region.Leader {
		t.Fatalf("leader not updated: %+v", snapshot.Region)
	}

	snapshots, err := svc.RegionsByStore(2)
	if err != nil {
		t.Fatalf("regions by store: %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	}
}

func TestPersistentRegionMetadataRecovery(t *testing.T) {
	dir := t.TempDir()
	svc, err := pd.NewPersistentService(dir)
	if err != nil {
		t.Fatalf("new persistent service: %v", err)
	}

	region := regionpkg.Region{
		ID: 8,
		Epoch: regionpkg.Epoch{
			Version:     2,
			ConfVersion: 3,
		},
		Peers: []regionpkg.Peer{
			{ID: 0x0000000800000001, StoreID: 1, Role: regionpkg.Voter},
		},
		State:  regionpkg.StateActive,
		Leader: 0x0000000800000001,
	}

	if _, err := svc.RegisterRegion(region); err != nil {
		t.Fatalf("register region: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("close service: %v", err)
	}

	svc2, err := pd.NewPersistentService(dir)
	if err != nil {
		t.Fatalf("reopen persistent service: %v", err)
	}
	defer svc2.Close()

	snapshot, ok := svc2.RegionSnapshot(uint64(region.ID))
	if !ok {
		t.Fatalf("expected region snapshot")
	}
	if snapshot.Region.Epoch.Version != region.Epoch.Version {
		t.Fatalf("unexpected epoch: %+v", snapshot.Region.Epoch)
	}
	if len(snapshot.Region.Peers) != len(region.Peers) {
		t.Fatalf("unexpected peers: %+v", snapshot.Region.Peers)
	}
	snapshots, err := svc2.RegionsByStore(1)
	if err != nil {
		t.Fatalf("regions by store: %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("expected snapshot for store 1, got %d", len(snapshots))
	}
}
