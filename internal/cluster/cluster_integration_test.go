package cluster

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	db "nyxdb/internal/engine"
	rafttransport "nyxdb/internal/raft"

	"github.com/stretchr/testify/require"
)

func TestClusterLinearizableRead(t *testing.T) {
	dir := t.TempDir()
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = false
	opts.ClusterConfig = &db.ClusterOptions{
		ClusterMode:      true,
		NodeAddress:      "127.0.0.1:9001",
		ClusterAddresses: []string{"1@127.0.0.1:9001"},
	}

	engine, err := db.Open(opts)
	require.NoError(t, err)

	cl, err := NewClusterWithTransport(1, opts, engine, rafttransport.NewDefaultTransport())
	require.NoError(t, err)
	require.NoError(t, cl.Start())
	defer func() { _ = cl.Stop() }()

	require.Eventually(t, func() bool { return cl.IsLeader() }, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, cl.Put([]byte("key"), []byte("value")))
	require.Eventually(t, func() bool {
		val, err := engine.Get([]byte("key"))
		return err == nil && bytes.Equal(val, []byte("value"))
	}, 5*time.Second, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	val, err := cl.GetLinearizable(ctx, []byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), val)
}

func TestClusterMembershipPersistence(t *testing.T) {
	dir := t.TempDir()
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = false
	opts.ClusterConfig = &db.ClusterOptions{
		ClusterMode:      true,
		NodeAddress:      "127.0.0.1:9001",
		ClusterAddresses: []string{"1@127.0.0.1:9001"},
	}

	engine, err := db.Open(opts)
	require.NoError(t, err)

	cl, err := NewClusterWithTransport(1, opts, engine, rafttransport.NewDefaultTransport())
	require.NoError(t, err)
	require.NoError(t, cl.Start())

	require.Eventually(t, func() bool { return cl.IsLeader() }, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, cl.AddMember(2, "127.0.0.1:9002"))
	require.Eventually(t, func() bool {
		return cl.Members()[2] == "127.0.0.1:9002"
	}, 5*time.Second, 50*time.Millisecond)

	membersFile := filepath.Join(opts.DirPath, "cluster", membersFileName)
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(membersFile)
		if err != nil {
			return false
		}
		return bytes.Contains(data, []byte("127.0.0.1:9002"))
	}, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, cl.Stop())

	engine2, err := db.Open(opts)
	require.NoError(t, err)
	defer engine2.Close()

	cl2, err := NewClusterWithTransport(1, opts, engine2, rafttransport.NewDefaultTransport())
	require.NoError(t, err)
	require.NoError(t, cl2.Start())
	defer func() { _ = cl2.Stop() }()

	require.Eventually(t, func() bool {
		return cl2.Members()[2] == "127.0.0.1:9002"
	}, 5*time.Second, 50*time.Millisecond)
}
