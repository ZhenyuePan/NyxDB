package percolator_test

import (
	"context"
	"errors"
	"os"
	"testing"

	db "nyxdb/internal/layers/engine"
	"nyxdb/internal/layers/txn/percolator"
)

type tsoStub struct {
	next uint64
}

func (t *tsoStub) Allocate(count uint32) (uint64, uint32, error) {
	if count == 0 {
		count = 1
	}
	base := t.next + 1
	t.next = base + uint64(count) - 1
	return base, count, nil
}

type engineStore struct {
	db *db.DB
}

func (s *engineStore) Apply(commitTs uint64, ops []db.ReplicatedOp) error {
	return s.db.ApplyReplicated(commitTs, ops)
}

func openTestDB(t *testing.T) *db.DB {
	t.Helper()
	opts := db.DefaultOptions
	dir, err := os.MkdirTemp("", "nyxdb-txn-test")
	if err != nil {
		t.Fatalf("mkdir temp: %v", err)
	}
	opts.DirPath = dir
	db, err := db.Open(opts)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	})
	return db
}

func TestTxnCommitPut(t *testing.T) {
	database := openTestDB(t)
	mgr := percolator.NewManager(&engineStore{db: database}, &tsoStub{})

	txn, err := mgr.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin txn: %v", err)
	}
	txn.Put([]byte("key"), []byte("value"))
	if err := txn.Commit(context.Background()); err != nil {
		t.Fatalf("commit txn: %v", err)
	}

	got, err := database.Get([]byte("key"))
	if err != nil {
		t.Fatalf("get committed key: %v", err)
	}
	if string(got) != "value" {
		t.Fatalf("unexpected value %q", got)
	}
}

func TestTxnCommitDelete(t *testing.T) {
	database := openTestDB(t)
	if err := database.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("seed value: %v", err)
	}

	mgr := percolator.NewManager(&engineStore{db: database}, &tsoStub{next: database.MaxCommittedTs()})

	txn, err := mgr.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin txn: %v", err)
	}
	txn.Delete([]byte("key"))
	if err := txn.Commit(context.Background()); err != nil {
		t.Fatalf("commit txn: %v", err)
	}

	if _, err := database.Get([]byte("key")); !errors.Is(err, db.ErrKeyNotFound) {
		t.Fatalf("expected key not found, got %v", err)
	}
}
