package cluster

import (
	"testing"
	"time"

	db "nyxdb/internal/engine"
	pd "nyxdb/internal/pd"
	rafttransport "nyxdb/internal/raft"

	"github.com/stretchr/testify/require"
)

func TestClusterHeartbeatsToPD(t *testing.T) {
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

	cl, err := NewClusterWithTransport(1, opts, engine, rafttransport.NewDefaultTransport())
	require.NoError(t, err)
	defer func() { _ = cl.Stop() }()

	service := pd.NewService()
	cl.AttachPD(service, 20*time.Millisecond)

	require.NoError(t, cl.Start())

	require.Eventually(t, func() bool {
		hb, ok := service.Store(1)
		return ok && len(hb.Regions) > 0
	}, time.Second, 20*time.Millisecond)

	hb, ok := service.Store(1)
	require.True(t, ok)
	require.Equal(t, "127.0.0.1:19001", hb.Address)
	require.NotEmpty(t, hb.Regions)
	require.Equal(t, uint64(1), hb.Regions[0].StoreID)
}
