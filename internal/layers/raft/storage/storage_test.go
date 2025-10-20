package raftstorage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	raftstorage "nyxdb/internal/layers/raft/storage"
)

func TestStorageAppendAndPersist(t *testing.T) {
	dir := t.TempDir()
	st, err := raftstorage.New(dir)
	require.NoError(t, err)

	first, err := st.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)

	last, err := st.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(0), last)

	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("e1")},
		{Index: 2, Term: 1, Data: []byte("e2")},
		{Index: 3, Term: 2, Data: []byte("e3")},
	}
	require.NoError(t, st.Append(entries))

	got, err := st.Entries(1, 4, 0)
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Equal(t, []byte("e1"), got[0].Data)

	term, err := st.Term(3)
	require.NoError(t, err)
	require.Equal(t, uint64(2), term)

	require.NoError(t, st.SetHardState(raftpb.HardState{Term: 2, Commit: 3}))

	st2, err := raftstorage.New(dir)
	require.NoError(t, err)

	hs, _, err := st2.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(2), hs.Term)
	require.Equal(t, uint64(3), hs.Commit)

	got2, err := st2.Entries(2, 4, 0)
	require.NoError(t, err)
	require.Len(t, got2, 2)
	require.Equal(t, []byte("e2"), got2[0].Data)
}

func TestStorageSnapshotAndCompaction(t *testing.T) {
	dir := t.TempDir()
	st, err := raftstorage.New(dir)
	require.NoError(t, err)

	require.NoError(t, st.Append([]raftpb.Entry{
		{Index: 5, Term: 3},
		{Index: 6, Term: 3},
		{Index: 7, Term: 4},
	}))

	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{Index: 6, Term: 3},
	}
	require.NoError(t, st.ApplySnapshot(snap))

	first, err := st.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(7), first)

	_, err = st.Term(5)
	require.ErrorIs(t, err, raft.ErrCompacted)

	require.NoError(t, st.Append([]raftpb.Entry{
		{Index: 7, Term: 4, Data: []byte("v7")},
		{Index: 8, Term: 4, Data: []byte("v8")},
	}))

	entries, err := st.Entries(7, 9, 0)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Equal(t, []byte("v7"), entries[0].Data)

	st2, err := raftstorage.New(dir)
	require.NoError(t, err)

	first2, err := st2.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(7), first2)
}

func TestStorageFilesCreated(t *testing.T) {
	dir := t.TempDir()
	st, err := raftstorage.New(dir)
	require.NoError(t, err)
	require.NoError(t, st.SetHardState(raftpb.HardState{Term: 1}))

	_, err = os.Stat(filepath.Join(dir, "raft_state.bin"))
	require.NoError(t, err)
}
