package replication

import (
    "encoding/json"
    "fmt"
)

// OpType describes the kind of mutation carried in a distributed command.
type OpType int8

const (
    OpPut OpType = iota
    OpDelete
)

// Operation represents a single key mutation inside a Raft command.
type Operation struct {
    Key   []byte `json:"key"`
    Value []byte `json:"value,omitempty"`
    Type  OpType `json:"type"`
}

// Command encapsulates the batch of operations that must be applied atomically.
// CommitTs is assigned by the leader before proposing to Raft to preserve MVCC ordering.
type Command struct {
    CommitTs  uint64       `json:"commit_ts"`
    Operations []Operation `json:"operations"`
}

// Marshal serialises the command using JSON for simplicity and debugging friendliness.
func (c *Command) Marshal() ([]byte, error) {
    if c == nil {
        return nil, fmt.Errorf("nil command")
    }
    return json.Marshal(c)
}

// UnmarshalCommand deserialises command bytes into a Command structure.
func UnmarshalCommand(data []byte) (*Command, error) {
    if len(data) == 0 {
        return nil, fmt.Errorf("empty command payload")
    }
    var cmd Command
    if err := json.Unmarshal(data, &cmd); err != nil {
        return nil, err
    }
    return &cmd, nil
}
