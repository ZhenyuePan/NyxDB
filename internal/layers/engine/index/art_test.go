package index

import (
	"testing"

	"nyxdb/internal/layers/engine/data"
)

func TestARTBasicCRUD(t *testing.T) {
	artIdx := NewART()
	defer artIdx.Close()

	key := []byte("art-key")
	pos := &data.LogRecordPos{Fid: 1, Offset: 42}

	if prev := artIdx.Put(key, pos); prev != nil {
		t.Fatalf("unexpected previous value: %v", prev)
	}
	if got := artIdx.Get(key); got == nil || got.Offset != pos.Offset {
		t.Fatalf("get mismatch: %+v", got)
	}

	newPos := &data.LogRecordPos{Fid: 2, Offset: 99}
	if prev := artIdx.Put(key, newPos); prev == nil || prev.Offset != pos.Offset {
		t.Fatalf("update returned wrong previous pos: %+v", prev)
	}

	if removed, ok := artIdx.Delete(key); !ok || removed.Offset != newPos.Offset {
		t.Fatalf("delete mismatch: %v, %v", removed, ok)
	}
	if got := artIdx.Get(key); got != nil {
		t.Fatalf("expected nil after delete, got %+v", got)
	}
}
