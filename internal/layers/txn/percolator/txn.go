package percolator

import (
	"context"
	"errors"
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

// Manager coordinates transactions using a Store and a TSO.
type Manager struct {
	store Store
	tso   TSO
}

// NewManager constructs a transaction manager.
func NewManager(store Store, tso TSO) *Manager {
	return &Manager{store: store, tso: tso}
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
}

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
	if len(tx.primary) == 0 {
		tx.primary = append([]byte(nil), m.Key...)
	}
	tx.mutations = append(tx.mutations, m)
}

var (
	// ErrTxnFinished indicates the transaction has already completed.
	ErrTxnFinished = errors.New("percolator: transaction already finished")
)

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

	commitTs, _, err := tx.manager.tso.Allocate(1)
	if err != nil {
		return err
	}

	ops := make([]db.ReplicatedOp, 0, len(tx.mutations))
	tx.mu.Lock()
	for _, mut := range tx.mutations {
		op := db.ReplicatedOp{
			Key: append([]byte(nil), mut.Key...),
		}
		if mut.Type == MutationDelete {
			op.Delete = true
		} else {
			op.Value = append([]byte(nil), mut.Value...)
		}
		ops = append(ops, op)
	}
	tx.mu.Unlock()

	if err := tx.manager.store.Apply(commitTs, ops); err != nil {
		return err
	}

	tx.mu.Lock()
	tx.committed = true
	tx.mu.Unlock()
	return nil
}

// Rollback marks the transaction as finished without applying mutations.
func (tx *Transaction) Rollback(ctx context.Context) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.committed || tx.rolledBack {
		return ErrTxnFinished
	}
	tx.rolledBack = true
	tx.mutations = nil
	return nil
}
