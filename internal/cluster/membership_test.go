package cluster

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemberStoreLoadSave(t *testing.T) {
	dir := t.TempDir()
	store, err := newMemberStore(dir)
	require.NoError(t, err)

	loaded, err := store.Load()
	require.NoError(t, err)
	require.Empty(t, loaded)

	members := map[uint64]string{
		1: "127.0.0.1:1001",
		2: "127.0.0.1:1002",
	}
	require.NoError(t, store.Save(members))

	data, err := os.ReadFile(filepath.Join(dir, membersFileName))
	require.NoError(t, err)
	require.NotEmpty(t, data)

	restored, err := store.Load()
	require.NoError(t, err)
	require.Equal(t, members, restored)
}
