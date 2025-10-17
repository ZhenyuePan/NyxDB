package cluster

import (
	"testing"
	"time"

	db "nyxdb/internal/engine"
	rafttransport "nyxdb/internal/raft"

	"github.com/stretchr/testify/require"
)

func TestClusterReadTxnTTL(t *testing.T) {
	dir := t.TempDir()
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = false

	engine, err := db.Open(opts)
	require.NoError(t, err)

	cl, err := NewClusterWithTransport(1, opts, engine, rafttransport.NewNoopTransport())
	require.NoError(t, err)
	cl.readTxnTTL = 200 * time.Millisecond
	require.NoError(t, cl.Start())
	t.Cleanup(func() { _ = cl.Stop() })

	handle, _, err := cl.BeginReadTxn()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		cl.readTxnMu.RLock()
		_, ok := cl.readTxns[string(handle)]
		cl.readTxnMu.RUnlock()
		return ok
	}, time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		cl.readTxnMu.RLock()
		_, ok := cl.readTxns[string(handle)]
		cl.readTxnMu.RUnlock()
		return !ok
	}, time.Second, 20*time.Millisecond)

	err = cl.EndReadTxn(handle)
	require.ErrorIs(t, err, ErrReadTxnNotFound)

	_, _, err = cl.ReadTxnGet(handle, []byte("foo"))
	require.ErrorIs(t, err, ErrReadTxnNotFound)
}
