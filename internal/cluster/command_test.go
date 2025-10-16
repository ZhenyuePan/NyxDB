package cluster

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommandMarshalUnmarshal(t *testing.T) {
	original := &Command{
		CommitTs: 42,
		Operations: []Operation{
			{Key: []byte("k1"), Value: []byte("v1"), Type: OpPut},
			{Key: []byte("k2"), Type: OpDelete},
		},
	}

	data, err := original.Marshal()
	assert.Nil(t, err)

	var raw map[string]any
	err = json.Unmarshal(data, &raw)
	assert.Nil(t, err)
	assert.Equal(t, float64(42), raw["commit_ts"])

	restored, err := UnmarshalCommand(data)
	assert.Nil(t, err)
	assert.Equal(t, original.CommitTs, restored.CommitTs)
	assert.Equal(t, len(original.Operations), len(restored.Operations))
	assert.Equal(t, OpPut, restored.Operations[0].Type)
	assert.Equal(t, []byte("v1"), restored.Operations[0].Value)
	assert.Equal(t, OpDelete, restored.Operations[1].Type)
	assert.True(t, restored.Operations[1].Value == nil || len(restored.Operations[1].Value) == 0)
}
