package replication

import (
    "fmt"

    db "nyxdb/internal/engine"
)

// Applier applies replicated commands to the local engine instance.
type Applier struct {
    engine *db.DB
}

// NewApplier constructs an applier bound to the given engine.
func NewApplier(engine *db.DB) *Applier {
    return &Applier{engine: engine}
}

// Apply consumes a serialized command, applies it to the engine, and returns the commit timestamp.
func (a *Applier) Apply(data []byte) (uint64, error) {
    if len(data) == 0 {
        return 0, nil
    }
    cmd, err := UnmarshalCommand(data)
    if err != nil {
        return 0, err
    }
    if cmd.CommitTs == 0 {
        return 0, fmt.Errorf("command missing commit timestamp")
    }

    ops := make([]db.ReplicatedOp, 0, len(cmd.Operations))
    for _, op := range cmd.Operations {
        repl := db.ReplicatedOp{Key: append([]byte(nil), op.Key...)}
        switch op.Type {
        case OpPut:
            repl.Value = append([]byte(nil), op.Value...)
        case OpDelete:
            repl.Delete = true
        default:
            return 0, fmt.Errorf("unknown operation type: %d", op.Type)
        }
        ops = append(ops, repl)
    }

    if err := a.engine.ApplyReplicated(cmd.CommitTs, ops); err != nil {
        return 0, err
    }
    return cmd.CommitTs, nil
}
