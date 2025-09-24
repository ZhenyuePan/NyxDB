package proxy

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"nyxdb/internel/db"
	"sync"
)

// Proxy represents a cluster proxy that routes requests to appropriate nodes
type Proxy struct {
	options     db.Options
	nodes       map[string]*NodeClient
	mu          sync.RWMutex
	currentNode string
	router      Router
}

// NodeClient represents a client connection to a database node
type NodeClient struct {
	Address string
	Client  *rpc.Client
}

// RouterType defines the type of routing algorithm to use
// DirectHash and ConsistentHash are supported

const (
	DirectHash = iota
	ConsistentHash
)

// NewProxy creates a new proxy instance
func NewProxy(options db.Options) *Proxy {
	proxy := &Proxy{
		options:     options,
		nodes:       make(map[string]*NodeClient),
		currentNode: options.NodeAddress,
	}

	// Initialize router based on options or default to direct hash
	switch options.RouterType {
	case ConsistentHash:
		proxy.router = NewConsistentHashRouter(20) // 20 replicas by default
	default:
		proxy.router = NewDirectHashRouter()
	}

	return proxy
}

// Start initializes the proxy and connects to cluster nodes
func (p *Proxy) Start() error {
	if !p.options.ClusterMode {
		return errors.New("cluster mode is not enabled")
	}

	// Connect to all cluster nodes
	for _, addr := range p.options.ClusterAddresses {
		client, err := rpc.DialHTTP("tcp", addr)
		if err != nil {
			return fmt.Errorf("failed to connect to node %s: %v", addr, err)
		}

		p.nodes[addr] = &NodeClient{
			Address: addr,
			Client:  client,
		}
	}

	return nil
}

// routeRequest routes a request to the appropriate node
func (p *Proxy) routeRequest(key []byte, method string, args interface{}, reply interface{}) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Determine which node should handle this key
	nodeAddr := p.router.Route(key, p.options.ClusterAddresses)

	// If the node is this current node, handle locally
	if nodeAddr == p.currentNode || p.currentNode == "" {
		return errors.New("local handling not implemented in proxy")
	}

	// Get the client for the target node
	node, exists := p.nodes[nodeAddr]
	if !exists {
		return fmt.Errorf("node %s not found in cluster", nodeAddr)
	}

	// Call the remote method
	return node.Client.Call(method, args, reply)
}

// Put routes a Put request to the appropriate node
func (p *Proxy) Put(key []byte, value []byte) error {
	args := &PutArgs{Key: key, Value: value}
	reply := &PutReply{}

	return p.routeRequest(key, "Node.Put", args, reply)
}

// Get routes a Get request to the appropriate node
func (p *Proxy) Get(key []byte) ([]byte, error) {
	args := &GetArgs{Key: key}
	reply := &GetReply{}

	err := p.routeRequest(key, "Node.Get", args, reply)
	if err != nil {
		return nil, err
	}

	return reply.Value, nil
}

// Delete routes a Delete request to the appropriate node
func (p *Proxy) Delete(key []byte) error {
	args := &DeleteArgs{Key: key}
	reply := &DeleteReply{}

	return p.routeRequest(key, "Node.Delete", args, reply)
}

// PutArgs represents arguments for Put RPC call
type PutArgs struct {
	Key   []byte
	Value []byte
}

// PutReply represents reply for Put RPC call
type PutReply struct {
	Error string
}

// GetArgs represents arguments for Get RPC call
type GetArgs struct {
	Key []byte
}

// GetReply represents reply for Get RPC call
type GetReply struct {
	Value []byte
	Error string
}

// DeleteArgs represents arguments for Delete RPC call
type DeleteArgs struct {
	Key []byte
}

// DeleteReply represents reply for Delete RPC call
type DeleteReply struct {
	Error string
}

// Node represents a database node that can be called via RPC
type Node struct {
	db *db.DB
}

// NewNode creates a new node instance
func NewNode(database *db.DB) *Node {
	return &Node{db: database}
}

// Put handles Put requests on the node
func (n *Node) Put(args *PutArgs, reply *PutReply) error {
	err := n.db.Put(args.Key, args.Value)
	if err != nil {
		reply.Error = err.Error()
		return err
	}
	reply.Error = ""
	return nil
}

// Get handles Get requests on the node
func (n *Node) Get(args *GetArgs, reply *GetReply) error {
	value, err := n.db.Get(args.Key)
	if err != nil {
		reply.Error = err.Error()
		return err
	}
	reply.Value = value
	reply.Error = ""
	return nil
}

// Delete handles Delete requests on the node
func (n *Node) Delete(args *DeleteArgs, reply *DeleteReply) error {
	err := n.db.Delete(args.Key)
	if err != nil {
		reply.Error = err.Error()
		return err
	}
	reply.Error = ""
	return nil
}

// StartServer starts the RPC server for this node
func (n *Node) StartServer(address string) error {
	rpc.Register(n)
	rpc.HandleHTTP()

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	fmt.Printf("Node RPC server started on %s\n", address)

	// This would typically run in a goroutine in a real implementation
	// http.Serve(listener, nil)
	_ = listener
	return nil
}
