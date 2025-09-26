package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirectHashRouter_Route(t *testing.T) {
	router := NewDirectHashRouter()
	nodes := []string{"node1", "node2", "node3"}

	// Test with different keys
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
	}

	// Run multiple times to ensure consistency
	for _, key := range keys {
		result1 := router.Route(key, nodes)
		result2 := router.Route(key, nodes)

		// Should return the same node for the same key
		assert.Equal(t, result1, result2, "Route should be consistent for key %s", string(key))

		// Should return a valid node
		found := false
		for _, node := range nodes {
			if result1 == node {
				found = true
				break
			}
		}
		if result1 != "" {
			assert.True(t, found, "Route should return a valid node or empty string, got %s", result1)
		}
	}

	// Test with empty nodes
	result := router.Route([]byte("key"), []string{})
	assert.Equal(t, "", result, "Expected empty string when nodes is empty, got %s", result)
}

func TestConsistentHashRouter_Route(t *testing.T) {
	router := NewConsistentHashRouter(20) // 20 replicas
	nodes := []string{"node1", "node2", "node3"}

	// Test with different keys
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
	}

	// Run multiple times to ensure consistency
	for _, key := range keys {
		result1 := router.Route(key, nodes)
		result2 := router.Route(key, nodes)

		// Should return the same node for the same key
		assert.Equal(t, result1, result2, "Route should be consistent for key %s", string(key))

		// Should return a valid node
		found := false
		for _, node := range nodes {
			if result1 == node {
				found = true
				break
			}
		}
		if result1 != "" {
			assert.True(t, found, "Route should return a valid node or empty string, got %s", result1)
		}
	}

	// Test with empty nodes
	result := router.Route([]byte("key"), []string{})
	assert.Equal(t, "", result, "Expected empty string when nodes is empty, got %s", result)
}

func TestConsistentHashRouter_ReplicaConsistency(t *testing.T) {
	// Test that changing number of replicas doesn't change the basic functionality
	router10 := NewConsistentHashRouter(10)
	router20 := NewConsistentHashRouter(20)

	nodes := []string{"node1", "node2", "node3", "node4"}
	key := []byte("test-key")

	result10 := router10.Route(key, nodes)
	result20 := router20.Route(key, nodes)

	// Both should return a valid node from the nodes list
	valid10 := false
	valid20 := false
	for _, node := range nodes {
		if result10 == node {
			valid10 = true
		}
		if result20 == node {
			valid20 = true
		}
	}

	assert.True(t, valid10, "router10 returned invalid node: %s", result10)
	assert.True(t, valid20, "router20 returned invalid node: %s", result20)
}

func TestRouterInterface(t *testing.T) {
	// Test that both routers implement the Router interface
	var router Router

	router = NewDirectHashRouter()
	assert.NotNil(t, router, "DirectHashRouter should implement Router interface")

	router = NewConsistentHashRouter(10)
	assert.NotNil(t, router, "ConsistentHashRouter should implement Router interface")
}
