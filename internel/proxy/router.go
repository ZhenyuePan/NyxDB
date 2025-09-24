package proxy

import (
	"hash/crc32"
	"hash/fnv"
)

// Router defines the interface for key-based routing
type Router interface {
	// Route determines which node should handle the given key
	Route(key []byte, nodes []string) string
}

// DirectHashRouter implements direct hash routing using CRC32
type DirectHashRouter struct{}

// NewDirectHashRouter creates a new direct hash router
func NewDirectHashRouter() *DirectHashRouter {
	return &DirectHashRouter{}
}

// Route implements direct hash routing
func (r *DirectHashRouter) Route(key []byte, nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}

	hash := crc32.ChecksumIEEE(key)
	nodeIndex := hash % uint32(len(nodes))
	return nodes[nodeIndex]
}

// ConsistentHashRouter implements consistent hash routing
type ConsistentHashRouter struct {
	replicas int
}

// NewConsistentHashRouter creates a new consistent hash router
func NewConsistentHashRouter(replicas int) *ConsistentHashRouter {
	return &ConsistentHashRouter{
		replicas: replicas,
	}
}

// Route implements consistent hash routing
func (r *ConsistentHashRouter) Route(key []byte, nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}

	// Simple consistent hash implementation
	// In a production environment, you might want to use a more sophisticated library
	hash := fnv.New32()
	hash.Write(key)
	keyHash := hash.Sum32()

	// Create virtual nodes
	var minHash uint32 = 0xFFFFFFFF
	var targetNode string

	for _, node := range nodes {
		for i := 0; i < r.replicas; i++ {
			hash.Reset()
			hash.Write([]byte(node + string(i)))
			nodeHash := hash.Sum32()

			// Find the node with the closest hash value
			if nodeHash >= keyHash && nodeHash < minHash {
				minHash = nodeHash
				targetNode = node
			}
		}
	}

	// If no node was found with a higher hash, wrap around to the first node
	if targetNode == "" && len(nodes) > 0 {
		targetNode = nodes[0]
	}

	return targetNode
}
