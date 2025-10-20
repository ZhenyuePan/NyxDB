package cluster

import (
	"os"
	"path/filepath"
	"testing"

	db "nyxdb/internal/layers/engine"
	rafttransport "nyxdb/internal/layers/raft/transport"
	regionpkg "nyxdb/internal/region"

	"github.com/stretchr/testify/require"
)

func TestClusterCreateStaticRegion(t *testing.T) {
	dir := t.TempDir()
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = false
	opts.ClusterConfig = &db.ClusterOptions{
		ClusterMode:      true,
		NodeAddress:      "127.0.0.1:19001",
		ClusterAddresses: []string{"1@127.0.0.1:19001"},
	}

	engine, err := db.Open(opts)
	require.NoError(t, err)

	cl, err := NewClusterWithTransport(1, opts, engine, rafttransport.NewNoopTransport())
	require.NoError(t, err)
	defer func() { _ = cl.Stop() }()

	region, err := cl.CreateStaticRegion(regionpkg.KeyRange{Start: []byte("m"), End: []byte("z")})
	require.NoError(t, err)
	require.NotNil(t, region)
	require.Equal(t, regionpkg.ID(2), region.ID)

	// storage directory should be created for the new region
	regionDir := filepath.Join(opts.DirPath, "regions", "2", "raft")
	require.DirExists(t, regionDir)

	require.NoError(t, cl.Start())

	// region should be discoverable via routing helper
	routed := cl.RegionForKey([]byte("monkey"))
	require.NotNil(t, routed)
	require.Equal(t, regionpkg.ID(2), routed.ID)

	// internal replica should be registered
	rep := cl.replica(regionpkg.ID(2))
	require.NotNil(t, rep)
	require.NotNil(t, rep.Node)
}

func TestRemoveRegionCleansUp(t *testing.T) {
	dir := t.TempDir()
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = false
	opts.ClusterConfig = &db.ClusterOptions{
		ClusterMode:      true,
		NodeAddress:      "127.0.0.1:19001",
		ClusterAddresses: []string{"1@127.0.0.1:19001"},
	}

	engine, err := db.Open(opts)
	require.NoError(t, err)

	cl, err := NewClusterWithTransport(1, opts, engine, rafttransport.NewNoopTransport())
	require.NoError(t, err)

	region, err := cl.CreateStaticRegion(regionpkg.KeyRange{Start: []byte("x"), End: nil})
	require.NoError(t, err)
	require.Equal(t, regionpkg.ID(2), region.ID)

	rep := cl.replica(region.ID)
	require.NotNil(t, rep)

	require.NoError(t, cl.RemoveRegion(region.ID))

	require.Nil(t, cl.replica(region.ID))
	if _, err := os.Stat(filepath.Join(opts.DirPath, "regions", "2")); !os.IsNotExist(err) {
		t.Fatalf("expected region directory removed, err=%v", err)
	}

	_ = cl.Stop()
}
