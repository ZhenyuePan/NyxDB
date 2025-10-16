package cluster

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

// Operation represents a single key mutation.
type Operation struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value,omitempty"`
	Type  OpType `json:"type"`
}

// Command is the structure replicated through RAFT.
// CommitTs is assigned by the leader before proposal to maintain MVCC ordering.
type Command struct {
	CommitTs   uint64      `json:"commit_ts"`
	Operations []Operation `json:"operations"`
}

// Marshal serialises the command.
func (c *Command) Marshal() ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("nil command")
	}
	return json.Marshal(c)
}

// UnmarshalCommand deserialises command bytes.
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
