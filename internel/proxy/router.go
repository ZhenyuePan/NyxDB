package proxy

import (
	"hash/crc32"
	"hash/fnv"
	"log"
	"strconv"
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
	log.Printf("DirectHashRouter: Routing key %s", string(key))

	if len(nodes) == 0 {
		log.Println("DirectHashRouter: No nodes available")
		return ""
	}

	if len(key) == 0 {
		log.Println("DirectHashRouter: Warning - empty key provided")
	}

	hash := crc32.ChecksumIEEE(key)
	nodeIndex := hash % uint32(len(nodes))
	selectedNode := nodes[nodeIndex]

	log.Printf("DirectHashRouter: Key %s hashed to %d, selected node %s (index %d)",
		string(key), hash, selectedNode, nodeIndex)

	return selectedNode
}

// ConsistentHashRouter implements consistent hash routing
type ConsistentHashRouter struct {
	replicas int
}

// NewConsistentHashRouter creates a new consistent hash router
func NewConsistentHashRouter(replicas int) *ConsistentHashRouter {
	if replicas <= 0 {
		log.Panicf("ConsistentHashRouter: replicas must be positive, got %d", replicas)
	}

	log.Printf("ConsistentHashRouter: Creating router with %d replicas", replicas)
	return &ConsistentHashRouter{
		replicas: replicas,
	}
}

// Route implements consistent hash routing
func (r *ConsistentHashRouter) Route(key []byte, nodes []string) string {
	log.Printf("ConsistentHashRouter: Routing key %s with %d nodes", string(key), len(nodes))

	if len(nodes) == 0 {
		log.Println("ConsistentHashRouter: No nodes available")
		return ""
	}

	if len(key) == 0 {
		log.Println("ConsistentHashRouter: Warning - empty key provided")
	}

	// Simple consistent hash implementation
	// In a production environment, you might want to use a more sophisticated library
	hash := fnv.New32()
	_, err := hash.Write(key)
	if err != nil {
		log.Printf("ConsistentHashRouter: Error writing key to hash: %v", err)
		// Fallback to first node
		return nodes[0]
	}
	keyHash := hash.Sum32()

	log.Printf("ConsistentHashRouter: Key %s hashed to %d", string(key), keyHash)

	// Create virtual nodes
	var minHash uint32 = 0xFFFFFFFF
	var targetNode string

	// Check if we can find a node with hash greater than keyHash
	var smallestHash uint32 = 0xFFFFFFFF
	var smallestNode string

	processedNodes := 0
	for _, node := range nodes {
		for i := 0; i < r.replicas; i++ {
			hash.Reset()
			_, err := hash.Write([]byte(node + strconv.Itoa(i)))
			if err != nil {
				log.Printf("ConsistentHashRouter: Error hashing node %s replica %d: %v", node, i, err)
				continue
			}
			nodeHash := hash.Sum32()

			// Track the node with smallest hash value for wrap-around
			if nodeHash < smallestHash {
				smallestHash = nodeHash
				smallestNode = node
			}

			// Find the node with the closest hash value (greater than keyHash)
			if nodeHash >= keyHash && nodeHash < minHash {
				minHash = nodeHash
				targetNode = node
			}

			processedNodes++
		}
	}

	log.Printf("ConsistentHashRouter: Processed %d virtual nodes", processedNodes)

	// If no node was found with a higher hash, wrap around to the node with smallest hash
	if targetNode == "" {
		log.Printf("ConsistentHashRouter: Wrapping around to smallest hash node: %s", smallestNode)
		targetNode = smallestNode
	} else {
		log.Printf("ConsistentHashRouter: Found target node: %s", targetNode)
	}

	return targetNode
}
