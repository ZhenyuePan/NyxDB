package replication

import (
    db "nyxdb/internal/layers/engine"
    "os"
    "testing"

    "github.com/stretchr/testify/assert"
)

func TestApplierApply(t *testing.T) {
    opts := db.DefaultOptions
    dir, _ := os.MkdirTemp("", "bitcask-go-applier")
    opts.DirPath = dir
    engine, err := db.Open(opts)
    assert.Nil(t, err)
    defer func() { _ = engine.Close(); _ = os.RemoveAll(dir) }()

    applier := NewApplier(engine)

    cmd := &Command{
        CommitTs: 10,
        Operations: []Operation{
            {Key: []byte("ak"), Value: []byte("av"), Type: OpPut},
        },
    }
    data, err := cmd.Marshal()
    assert.Nil(t, err)

    ts, err := applier.Apply(data)
    assert.Nil(t, err)
    assert.Equal(t, uint64(10), ts)

    val, err := engine.Get([]byte("ak"))
    assert.Nil(t, err)
    assert.Equal(t, []byte("av"), val)

    cmd2 := &Command{
        CommitTs: 12,
        Operations: []Operation{
            {Key: []byte("ak"), Type: OpDelete},
        },
    }
    data2, err := cmd2.Marshal()
    assert.Nil(t, err)
    ts, err = applier.Apply(data2)
    assert.Nil(t, err)
    assert.Equal(t, uint64(12), ts)

    _, err = engine.Get([]byte("ak"))
    assert.Equal(t, db.ErrKeyNotFound, err)
}
