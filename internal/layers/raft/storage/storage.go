package raftstorage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

var (
	keyHardState = []byte("meta/hardstate")
	keyConfState = []byte("meta/confstate")
	keySnapshot  = []byte("meta/snapshot")
	keyFirst     = []byte("meta/first")
	keyLast      = []byte("meta/last")
	entryPrefix  = []byte("entry/")
	entryUpper   = append(append([]byte{}, entryPrefix...), 0xFF)
	syncWrite    = &pebble.WriteOptions{Sync: true}
)

// Storage implements raft.Storage backed by Pebble.
type Storage struct {
	mu        sync.RWMutex
	db        *pebble.DB
	firstIdx  uint64
	lastIdx   uint64
	hardState raftpb.HardState
	confState raftpb.ConfState
	snapshot  raftpb.Snapshot
}

func (s *Storage) currentDB() (*pebble.DB, error) {
	if s.db == nil {
		return nil, fmt.Errorf("raft storage closed")
	}
	return s.db, nil
}

// New opens / creates a Pebble-backed raft storage under dir.
func New(dir string) (*Storage, error) {
	if dir == "" {
		return nil, fmt.Errorf("raft storage dir is empty")
	}
	dbPath := filepath.Join(dir, "pebble")
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	st := &Storage{db: db}
	if err := st.load(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return st, nil
}

// Close releases the underlying resources.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func (s *Storage) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.loadProto(keyHardState, &s.hardState); err != nil {
		return err
	}
	if err := s.loadProto(keyConfState, &s.confState); err != nil {
		return err
	}
	if err := s.loadProto(keySnapshot, &s.snapshot); err != nil {
		return err
	}

	fi, err := s.loadUint64(keyFirst)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	li, err := s.loadUint64(keyLast)
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	if fi != 0 {
		s.firstIdx = fi
	}
	if li != 0 {
		s.lastIdx = li
	}

	if s.firstIdx == 0 {
		s.firstIdx = s.snapshot.Metadata.Index + 1
		if s.firstIdx == 0 {
			s.firstIdx = 1
		}
	}
	if s.lastIdx == 0 {
		s.lastIdx = s.snapshot.Metadata.Index
	}

	return s.refreshBoundsLocked()
}

func (s *Storage) loadProto(key []byte, msg proto.Message) error {
	if msg == nil {
		return nil
	}
	db, err := s.currentDB()
	if err != nil {
		return err
	}
	val, closer, err := db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	defer closer.Close()
	return proto.Unmarshal(val, msg)
}

func (s *Storage) loadUint64(key []byte) (uint64, error) {
	db, err := s.currentDB()
	if err != nil {
		return 0, err
	}
	val, closer, err := db.Get(key)
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(val) != 8 {
		return 0, fmt.Errorf("invalid uint64 length: %d", len(val))
	}
	return binary.BigEndian.Uint64(val), nil
}

func (s *Storage) persistUint64(key []byte, value uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	db, err := s.currentDB()
	if err != nil {
		return err
	}
	return db.Set(key, buf[:], syncWrite)
}

func (s *Storage) persistProto(key []byte, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	db, err := s.currentDB()
	if err != nil {
		return err
	}
	return db.Set(key, data, syncWrite)
}

func entryKey(index uint64) []byte {
	key := make([]byte, len(entryPrefix)+8)
	copy(key, entryPrefix)
	binary.BigEndian.PutUint64(key[len(entryPrefix):], index)
	return key
}

func entryIndex(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[len(entryPrefix):])
}

func (s *Storage) refreshBoundsLocked() error {
	db, err := s.currentDB()
	if err != nil {
		return err
	}
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: entryPrefix,
		UpperBound: entryUpper,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	first := s.snapshot.Metadata.Index + 1
	if first == 0 {
		first = 1
	}
	last := s.snapshot.Metadata.Index

	if iter.First() {
		first = entryIndex(iter.Key())
	}
	if iter.Last() {
		last = entryIndex(iter.Key())
	}

	s.firstIdx = first
	s.lastIdx = last

	if err := s.persistUint64(keyFirst, s.firstIdx); err != nil {
		return err
	}
	if err := s.persistUint64(keyLast, s.lastIdx); err != nil {
		return err
	}
	return nil
}

func (s *Storage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hardState, s.confState, nil
}

func (s *Storage) SetHardState(hs raftpb.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hardState = hs
	return s.persistProto(keyHardState, &hs)
}

func (s *Storage) SetConfState(cs *raftpb.ConfState) error {
	if cs == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cloned := proto.Clone(cs).(*raftpb.ConfState)
	s.confState = *cloned
	return s.persistProto(keyConfState, cloned)
}

func (s *Storage) ConfState() raftpb.ConfState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return *proto.Clone(&s.confState).(*raftpb.ConfState)
}

func (s *Storage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if lo < s.firstIdx {
		return nil, raft.ErrCompacted
	}
	if hi > s.lastIdx+1 {
		return nil, raft.ErrUnavailable
	}
	if lo == hi {
		return nil, nil
	}

	entries := make([]raftpb.Entry, 0, hi-lo)
	var size uint64
	for index := lo; index < hi; index++ {
		ent, err := s.getEntryLocked(index)
		if err != nil {
			return nil, err
		}
		entries = append(entries, *ent)
		if maxSize > 0 {
			size += uint64(ent.Size())
			if size > maxSize {
				entries = entries[:len(entries)-1]
				break
			}
		}
	}
	return entries, nil
}

func (s *Storage) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if snapIndex := s.snapshot.Metadata.Index; i == snapIndex {
		return s.snapshot.Metadata.Term, nil
	} else if i < snapIndex || i < s.firstIdx {
		return 0, raft.ErrCompacted
	}
	if i > s.lastIdx {
		return 0, raft.ErrUnavailable
	}
	ent, err := s.getEntryLocked(i)
	if err != nil {
		return 0, err
	}
	return ent.Term, nil
}

func (s *Storage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIdx, nil
}

func (s *Storage) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.firstIdx, nil
}

func (s *Storage) Snapshot() (raftpb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneSnapshot(s.snapshot), nil
}

func (s *Storage) ApplySnapshot(snap raftpb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if snap.Metadata.Index < s.snapshot.Metadata.Index {
		return raft.ErrSnapOutOfDate
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	if err := batch.DeleteRange(entryPrefix, entryUpper, nil); err != nil {
		return err
	}
	if err := batch.Commit(syncWrite); err != nil {
		return err
	}

	s.snapshot = cloneSnapshot(snap)
	s.confState = snap.Metadata.ConfState
	if err := s.persistProto(keySnapshot, &s.snapshot); err != nil {
		return err
	}
	if err := s.persistProto(keyConfState, &s.confState); err != nil {
		return err
	}

	s.firstIdx = s.snapshot.Metadata.Index + 1
	if s.firstIdx == 0 {
		s.firstIdx = 1
	}
	s.lastIdx = s.snapshot.Metadata.Index
	if err := s.persistUint64(keyFirst, s.firstIdx); err != nil {
		return err
	}
	if err := s.persistUint64(keyLast, s.lastIdx); err != nil {
		return err
	}
	return nil
}

func (s *Storage) CreateSnapshot(index uint64, data []byte, cs *raftpb.ConfState) (*raftpb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index < s.snapshot.Metadata.Index {
		return nil, raft.ErrSnapOutOfDate
	}
	if index > s.lastIdx {
		return nil, raft.ErrUnavailable
	}
	term, err := s.termAtLocked(index)
	if err != nil {
		return nil, err
	}
	conf := proto.Clone(&s.confState).(*raftpb.ConfState)
	if cs != nil {
		conf = proto.Clone(cs).(*raftpb.ConfState)
	}
	snap := raftpb.Snapshot{
		Data: append([]byte(nil), data...),
		Metadata: raftpb.SnapshotMetadata{
			Index:     index,
			Term:      term,
			ConfState: *conf,
		},
	}
	s.snapshot = cloneSnapshot(snap)
	s.confState = *conf
	if err := s.persistProto(keySnapshot, &s.snapshot); err != nil {
		return nil, err
	}
	if err := s.persistProto(keyConfState, &s.confState); err != nil {
		return nil, err
	}
	if err := s.persistUint64(keyFirst, s.firstIdx); err != nil {
		return nil, err
	}
	if err := s.persistUint64(keyLast, s.lastIdx); err != nil {
		return nil, err
	}
	return &snap, nil
}

func (s *Storage) Compact(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index < s.firstIdx-1 {
		return raft.ErrCompacted
	}
	if index > s.lastIdx {
		return raft.ErrUnavailable
	}

	start := entryKey(s.firstIdx)
	end := entryKey(index + 1)
	batch := s.db.NewBatch()
	defer batch.Close()

	if err := batch.DeleteRange(start, end, nil); err != nil {
		return err
	}
	if err := batch.Commit(syncWrite); err != nil {
		return err
	}
	return s.refreshBoundsLocked()
}

func (s *Storage) Append(ents []raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	firstNew := ents[0].Index
	lastNew := ents[len(ents)-1].Index

	if lastNew < s.firstIdx {
		return nil
	}
	if firstNew < s.firstIdx {
		offset := s.firstIdx - firstNew
		ents = cloneEntries(ents[offset:])
		firstNew = ents[0].Index
	}
	if s.lastIdx != 0 && firstNew > s.lastIdx+1 {
		return fmt.Errorf("raft storage: gap detected appending entries")
	}

	batch := s.db.NewBatch()
	defer batch.Close()

	if firstNew <= s.lastIdx {
		start := entryKey(firstNew)
		end := entryKey(s.lastIdx + 1)
		if err := batch.DeleteRange(start, end, nil); err != nil {
			return err
		}
	}

	for _, ent := range ents {
		data, err := ent.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(entryKey(ent.Index), data, nil); err != nil {
			return err
		}
	}

	if err := batch.Commit(syncWrite); err != nil {
		return err
	}

	if s.firstIdx == 0 || firstNew < s.firstIdx {
		s.firstIdx = firstNew
	}
	if lastNew > s.lastIdx {
		s.lastIdx = lastNew
	}

	if err := s.persistUint64(keyFirst, s.firstIdx); err != nil {
		return err
	}
	if err := s.persistUint64(keyLast, s.lastIdx); err != nil {
		return err
	}
	return nil
}

func (s *Storage) getEntryLocked(index uint64) (*raftpb.Entry, error) {
	val, closer, err := s.db.Get(entryKey(index))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, raft.ErrUnavailable
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var ent raftpb.Entry
	if err := ent.Unmarshal(val); err != nil {
		return nil, err
	}
	return &ent, nil
}

func (s *Storage) termAtLocked(index uint64) (uint64, error) {
	if snapIdx := s.snapshot.Metadata.Index; index == snapIdx {
		return s.snapshot.Metadata.Term, nil
	}
	if index < s.firstIdx {
		return 0, raft.ErrCompacted
	}
	if index > s.lastIdx {
		return 0, raft.ErrUnavailable
	}
	ent, err := s.getEntryLocked(index)
	if err != nil {
		return 0, err
	}
	return ent.Term, nil
}

func cloneEntries(entries []raftpb.Entry) []raftpb.Entry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]raftpb.Entry, len(entries))
	for i := range entries {
		out[i] = entries[i]
		if entries[i].Data != nil {
			out[i].Data = append([]byte(nil), entries[i].Data...)
		}
	}
	return out
}

func cloneSnapshot(snap raftpb.Snapshot) raftpb.Snapshot {
	cp := snap
	if snap.Data != nil {
		cp.Data = append([]byte(nil), snap.Data...)
	}
	return cp
}

// SetHardStateAndConf applies new hard state and conf state atomically.
// Deprecated: kept for backward compatibility.
func (s *Storage) SetHardStateAndConf(hs raftpb.HardState, cs *raftpb.ConfState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hardState = hs
	if err := s.persistProto(keyHardState, &hs); err != nil {
		return err
	}
	if cs != nil {
		cloned := proto.Clone(cs).(*raftpb.ConfState)
		s.confState = *cloned
		if err := s.persistProto(keyConfState, cloned); err != nil {
			return err
		}
	}
	return nil
}
