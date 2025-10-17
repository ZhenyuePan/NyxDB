package raftstorage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Storage implements raft.Storage with simple file-backed persistence.
type Storage struct {
	mu          sync.RWMutex
	path        string
	entryOffset uint64

	hardState raftpb.HardState
	confState raftpb.ConfState
	snapshot  raftpb.Snapshot
	entries   []raftpb.Entry
}

// Close releases resources held by the storage. Currently no-op.
func (s *Storage) Close() error {
	return nil
}

// New constructs a storage rooted at dir. The directory will be created if needed.
func New(dir string) (*Storage, error) {
	if dir == "" {
		return nil, fmt.Errorf("raft storage dir is empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	st := &Storage{
		path:        filepath.Join(dir, "raft_state.bin"),
		entryOffset: 1,
	}
	if err := st.load(); err != nil {
		return nil, err
	}
	return st, nil
}

func (s *Storage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hardState, s.confState, nil
}

func (s *Storage) SetHardState(hs raftpb.HardState) error {
	s.mu.Lock()
	s.hardState = hs
	err := s.persistLocked()
	s.mu.Unlock()
	return err
}

func (s *Storage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	first := s.firstIndexLocked()
	if lo < first {
		return nil, raft.ErrCompacted
	}
	last := s.lastIndexLocked()
	if hi > last+1 {
		return nil, raft.ErrUnavailable
	}
	if len(s.entries) == 0 {
		return nil, nil
	}

	offset := s.entryOffset
	start := lo - offset
	end := hi - offset
	if end > uint64(len(s.entries)) {
		end = uint64(len(s.entries))
	}
	ents := cloneEntries(s.entries[start:end])
	if maxSize > 0 {
		return limitSize(ents, maxSize), nil
	}
	return ents, nil
}

func (s *Storage) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if snapIndex := s.snapshot.Metadata.Index; i == snapIndex {
		return s.snapshot.Metadata.Term, nil
	} else if i < snapIndex {
		return 0, raft.ErrCompacted
	}

	if len(s.entries) == 0 {
		if i == s.snapshot.Metadata.Index {
			return s.snapshot.Metadata.Term, nil
		}
		return 0, raft.ErrUnavailable
	}

	if i < s.entryOffset {
		return 0, raft.ErrCompacted
	}
	idx := i - s.entryOffset
	if idx >= uint64(len(s.entries)) {
		return 0, raft.ErrUnavailable
	}
	return s.entries[idx].Term, nil
}

func (s *Storage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndexLocked(), nil
}

func (s *Storage) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.firstIndexLocked(), nil
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

	s.snapshot = cloneSnapshot(snap)
	s.confState = snap.Metadata.ConfState
	newOffset := snap.Metadata.Index + 1
	if len(s.entries) > 0 {
		if snap.Metadata.Index >= s.entries[len(s.entries)-1].Index {
			s.entries = nil
		} else if newOffset > s.entryOffset {
			cut := newOffset - s.entryOffset
			if cut >= uint64(len(s.entries)) {
				s.entries = nil
			} else {
				s.entries = cloneEntries(s.entries[cut:])
			}
		}
	}
	s.entryOffset = newOffset
	return s.persistLocked()
}

func (s *Storage) CreateSnapshot(index uint64, data []byte, cs *raftpb.ConfState) (*raftpb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index < s.snapshot.Metadata.Index {
		return nil, raft.ErrSnapOutOfDate
	}
	if index > s.lastIndexLocked() {
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
	if err := s.persistLocked(); err != nil {
		return nil, err
	}
	return &snap, nil
}

func (s *Storage) Compact(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	first := s.firstIndexLocked()
	if index < first-1 {
		return raft.ErrCompacted
	}
	if index >= s.lastIndexLocked() {
		s.entries = nil
		s.entryOffset = index + 1
		return s.persistLocked()
	}
	offset := index + 1 - s.entryOffset
	if offset > uint64(len(s.entries)) {
		return raft.ErrUnavailable
	}
	s.entries = cloneEntries(s.entries[offset:])
	s.entryOffset = index + 1
	return s.persistLocked()
}

func (s *Storage) SetConfState(cs *raftpb.ConfState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cs == nil {
		return nil
	}
	cloned := proto.Clone(cs).(*raftpb.ConfState)
	s.confState = *cloned
	return s.persistLocked()
}

func (s *Storage) ConfState() raftpb.ConfState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return *proto.Clone(&s.confState).(*raftpb.ConfState)
}

func (s *Storage) Append(ents []raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	firstIndex := s.firstIndexLocked()
	lastNew := ents[len(ents)-1].Index

	if lastNew < firstIndex {
		return nil
	}
	if ents[0].Index < firstIndex {
		ents = cloneEntries(ents[firstIndex-ents[0].Index:])
	}

	if len(s.entries) == 0 {
		s.entryOffset = ents[0].Index
		s.entries = cloneEntries(ents)
		return s.persistLocked()
	}

	offset := ents[0].Index - s.entryOffset
	switch {
	case offset == uint64(len(s.entries)):
		s.entries = append(s.entries, cloneEntries(ents)...)
	case offset < uint64(len(s.entries)):
		s.entries = append(append([]raftpb.Entry{}, s.entries[:offset]...), cloneEntries(ents)...)
	default:
		return fmt.Errorf("raft storage: gap detected appending entries")
	}
	return s.persistLocked()
}

func (s *Storage) termAtLocked(i uint64) (uint64, error) {
	if snapIndex := s.snapshot.Metadata.Index; i == snapIndex {
		return s.snapshot.Metadata.Term, nil
	} else if i < snapIndex {
		return 0, raft.ErrCompacted
	}
	if len(s.entries) == 0 {
		if i == s.snapshot.Metadata.Index {
			return s.snapshot.Metadata.Term, nil
		}
		return 0, raft.ErrUnavailable
	}
	if i < s.entryOffset {
		return 0, raft.ErrCompacted
	}
	idx := i - s.entryOffset
	if idx >= uint64(len(s.entries)) {
		return 0, raft.ErrUnavailable
	}
	return s.entries[idx].Term, nil
}

func (s *Storage) firstIndexLocked() uint64 {
	if s.snapshot.Metadata.Index != 0 {
		return s.snapshot.Metadata.Index + 1
	}
	if len(s.entries) > 0 {
		return s.entryOffset
	}
	return 1
}

func (s *Storage) lastIndexLocked() uint64 {
	if len(s.entries) > 0 {
		return s.entries[len(s.entries)-1].Index
	}
	return s.snapshot.Metadata.Index
}

func (s *Storage) persistLocked() error {
	tmpPath := s.path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := writeUint64(f, s.entryOffset); err != nil {
		return err
	}
	if err := writeMessage(f, &s.hardState); err != nil {
		return err
	}
	if err := writeMessage(f, &s.confState); err != nil {
		return err
	}
	if err := writeMessage(f, &s.snapshot); err != nil {
		return err
	}
	if err := writeUint64(f, uint64(len(s.entries))); err != nil {
		return err
	}
	for _, e := range s.entries {
		if err := writeMessage(f, &e); err != nil {
			return err
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return err
	}
	return nil
}

func (s *Storage) load() error {
	f, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	offset, err := readUint64(f)
	if err != nil {
		return err
	}
	s.entryOffset = offset

	if err := readMessage(f, &s.hardState); err != nil {
		return err
	}
	if err := readMessage(f, &s.confState); err != nil {
		return err
	}
	if err := readMessage(f, &s.snapshot); err != nil {
		return err
	}
	count, err := readUint64(f)
	if err != nil {
		return err
	}
	s.entries = make([]raftpb.Entry, 0, count)
	for i := uint64(0); i < count; i++ {
		var entry raftpb.Entry
		if err := readMessage(f, &entry); err != nil {
			return err
		}
		s.entries = append(s.entries, entry)
	}
	return nil
}

func writeUint64(w io.Writer, v uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

func readUint64(r io.Reader) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

func writeMessage(w io.Writer, msg proto.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	if err := writeUint64(w, uint64(len(data))); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func readMessage(r io.Reader, msg proto.Message) error {
	size, err := readUint64(r)
	if err != nil {
		return err
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	return proto.Unmarshal(data, msg)
}

func cloneEntries(entries []raftpb.Entry) []raftpb.Entry {
	if len(entries) == 0 {
		return nil
	}
	cp := make([]raftpb.Entry, len(entries))
	for i := range entries {
		cp[i] = entries[i]
		if entries[i].Data != nil {
			cp[i].Data = append([]byte(nil), entries[i].Data...)
		}
	}
	return cp
}

func limitSize(entries []raftpb.Entry, maxSize uint64) []raftpb.Entry {
	if maxSize == 0 {
		return entries
	}
	var size uint64
	for i, e := range entries {
		size += uint64(e.Size())
		if size > maxSize {
			return entries[:i]
		}
	}
	return entries
}

func cloneSnapshot(snap raftpb.Snapshot) raftpb.Snapshot {
	cp := snap
	if snap.Data != nil {
		cp.Data = append([]byte(nil), snap.Data...)
	}
	return cp
}
