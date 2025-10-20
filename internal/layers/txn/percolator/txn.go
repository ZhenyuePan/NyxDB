package percolator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	db "nyxdb/internal/layers/engine"
)

// MutationType distinguishes mutation operations.
type MutationType int

const (
	MutationPut MutationType = iota
	MutationDelete
)

// Mutation captures a single key/value change.
type Mutation struct {
	Key   []byte
	Value []byte
	Type  MutationType
}

// LockInfo describes an outstanding lock.
type LockInfo struct {
	Key     []byte
	Primary []byte
	StartTS uint64
}

// LockError reports that a key is locked by another transaction.
type LockError struct {
	Key     []byte
	Primary []byte
	StartTS uint64
}

func (e *LockError) Error() string {
	return fmt.Sprintf("percolator: key %x locked by %x at ts %d", e.Key, e.Primary, e.StartTS)
}

// Manager coordinates transactions using a Store and a TSO.
type Manager struct {
	store  Store
	tso    TSO
	lockMu sync.Mutex
	locks  map[string]*LockInfo
}

// NewManager constructs a transaction manager.
func NewManager(store Store, tso TSO) *Manager {
	return &Manager{
		store: store,
		tso:   tso,
		locks: make(map[string]*LockInfo),
	}
}

// Transaction represents a client transaction following the Percolator protocol.
type Transaction struct {
	manager   *Manager
	startTS   uint64
	primary   []byte
	mutations []Mutation

	mu         sync.Mutex
	committed  bool
	rolledBack bool
	prewritten bool
}

var (
	// ErrTxnFinished indicates the transaction has already completed.
	ErrTxnFinished = errors.New("percolator: transaction already finished")
	// ErrWriteConflict indicates a committed version conflicts with the current transaction.
	ErrWriteConflict = errors.New("percolator: write conflict")
)

// Begin starts a new transaction and assigns a start timestamp.
func (m *Manager) Begin(ctx context.Context) (*Transaction, error) {
	base, _, err := m.tso.Allocate(1)
	if err != nil {
		return nil, err
	}
	return &Transaction{
		manager: m,
		startTS: base,
	}, nil
}

// StartTS returns the start timestamp for the transaction.
func (tx *Transaction) StartTS() uint64 {
	return tx.startTS
}

// Put schedules an upsert mutation within the transaction.
func (tx *Transaction) Put(key, value []byte) {
	tx.addMutation(Mutation{
		Key:   append([]byte(nil), key...),
		Value: append([]byte(nil), value...),
		Type:  MutationPut,
	})
}

// Delete schedules a delete mutation for the given key.
func (tx *Transaction) Delete(key []byte) {
	tx.addMutation(Mutation{
		Key:  append([]byte(nil), key...),
		Type: MutationDelete,
	})
}

func (tx *Transaction) addMutation(m Mutation) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.committed || tx.rolledBack || tx.prewritten {
		return
	}
	if len(tx.primary) == 0 {
		tx.primary = append([]byte(nil), m.Key...)
	}
	tx.mutations = append(tx.mutations, m)
}

func (tx *Transaction) ensureMutable() error {
	if tx.committed || tx.rolledBack {
		return ErrTxnFinished
	}
	return nil
}

// Prewrite acquires locks for all mutations of the transaction.
func (tx *Transaction) Prewrite(ctx context.Context) error {
	tx.mu.Lock()
	if err := tx.ensureMutable(); err != nil {
		tx.mu.Unlock()
		return err
	}
	if tx.prewritten {
		tx.mu.Unlock()
		return nil
	}
	startTS := tx.startTS
	primary := append([]byte(nil), tx.primary...)
	mutations := tx.cloneMutationsLocked()
	tx.mu.Unlock()

	if err := tx.manager.prewrite(startTS, primary, mutations); err != nil {
		return err
	}

	tx.mu.Lock()
	tx.prewritten = true
	tx.mu.Unlock()
	return nil
}

// Commit executes a simplified 2PC commit for the accumulated mutations.
func (tx *Transaction) Commit(ctx context.Context) error {
	tx.mu.Lock()
	if tx.committed || tx.rolledBack {
		tx.mu.Unlock()
		return ErrTxnFinished
	}
	if len(tx.mutations) == 0 {
		tx.committed = true
		tx.mu.Unlock()
		return nil
	}
	tx.mu.Unlock()

	if err := tx.Prewrite(ctx); err != nil {
		return err
	}

	commitTs, _, err := tx.manager.tso.Allocate(1)
	if err != nil {
		tx.manager.releaseLocks(tx.startTS, tx.cloneMutations())
		return err
	}

	ops := tx.prepareOps()
	if err := tx.manager.store.Apply(commitTs, ops); err != nil {
		tx.manager.releaseLocks(tx.startTS, tx.cloneMutations())
		tx.mu.Lock()
		tx.rolledBack = true
		tx.mu.Unlock()
		return err
	}

	tx.manager.releaseLocks(tx.startTS, tx.cloneMutations())

	tx.mu.Lock()
	tx.committed = true
	tx.mutations = nil
	tx.mu.Unlock()
	return nil
}

// Rollback releases locks without applying mutations.
func (tx *Transaction) Rollback(ctx context.Context) error {
	tx.mu.Lock()
	if err := tx.ensureMutable(); err != nil {
		tx.mu.Unlock()
		return err
	}
	tx.mu.Unlock()

	tx.manager.releaseLocks(tx.startTS, tx.cloneMutations())

	tx.mu.Lock()
	tx.rolledBack = true
	tx.mutations = nil
	tx.mu.Unlock()
	return nil
}

// Get reads the value of key at the transaction's start timestamp.
func (tx *Transaction) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	tx.mu.Lock()
	if err := tx.ensureMutable(); err != nil {
		tx.mu.Unlock()
		return nil, false, err
	}
	startTS := tx.startTS
	tx.mu.Unlock()

	return tx.manager.readValue(startTS, key)
}

func (tx *Transaction) cloneMutations() []Mutation {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.cloneMutationsLocked()
}

func (tx *Transaction) cloneMutationsLocked() []Mutation {
	muts := make([]Mutation, len(tx.mutations))
	copy(muts, tx.mutations)
	return muts
}

func (tx *Transaction) prepareOps() []db.ReplicatedOp {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	ops := make([]db.ReplicatedOp, 0, len(tx.mutations))
	for _, mut := range tx.mutations {
		op := db.ReplicatedOp{Key: append([]byte(nil), mut.Key...)}
		if mut.Type == MutationDelete {
			op.Delete = true
		} else {
			op.Value = append([]byte(nil), mut.Value...)
		}
		ops = append(ops, op)
	}
	return ops
}

func (m *Manager) prewrite(startTS uint64, primary []byte, mutations []Mutation) error {
	for _, mut := range mutations {
		conflict, err := m.hasWriteConflict(mut.Key, startTS)
		if err != nil {
			return err
		}
		if conflict {
			return ErrWriteConflict
		}
	}

	m.lockMu.Lock()
	defer m.lockMu.Unlock()

	for _, mut := range mutations {
		keyStr := string(mut.Key)
		if lock := m.locks[keyStr]; lock != nil && lock.StartTS != startTS {
			return &LockError{
				Key:     append([]byte(nil), mut.Key...),
				Primary: append([]byte(nil), lock.Primary...),
				StartTS: lock.StartTS,
			}
		}
	}

	for _, mut := range mutations {
		keyStr := string(mut.Key)
		m.locks[keyStr] = &LockInfo{
			Key:     append([]byte(nil), mut.Key...),
			Primary: append([]byte(nil), primary...),
			StartTS: startTS,
		}
	}
	return nil
}

func (m *Manager) releaseLocks(startTS uint64, mutations []Mutation) {
	if len(mutations) == 0 {
		return
	}
	m.lockMu.Lock()
	defer m.lockMu.Unlock()
	for _, mut := range mutations {
		keyStr := string(mut.Key)
		if lock := m.locks[keyStr]; lock != nil && lock.StartTS == startTS {
			delete(m.locks, keyStr)
		}
	}
}

func (m *Manager) hasWriteConflict(key []byte, startTS uint64) (bool, error) {
	commitTs, ok, err := m.store.LatestCommitTs(key)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return commitTs >= startTS, nil
}

func (m *Manager) readValue(startTS uint64, key []byte) ([]byte, bool, error) {
	m.lockMu.Lock()
	if lock := m.locks[string(key)]; lock != nil && lock.StartTS != startTS {
		err := &LockError{
			Key:     append([]byte(nil), key...),
			Primary: append([]byte(nil), lock.Primary...),
			StartTS: lock.StartTS,
		}
		m.lockMu.Unlock()
		return nil, false, err
	}
	m.lockMu.Unlock()

	value, ok, err := m.store.GetValue(key, startTS)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}
