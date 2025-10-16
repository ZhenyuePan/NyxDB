package proxy

import (
	"fmt"
	db "nyxdb/internal/engine"
	"sync"
)

// Proxy 代理结构体
type Proxy struct {
	options     db.Options
	router      Router
	db          *db.DB
	clusterDBs  map[string]*db.DB // 模拟集群中的其他节点
	clusterLock sync.RWMutex
}

// NewProxy 创建新的代理实例
func NewProxy(options db.Options, database *db.DB) *Proxy {
	proxy := &Proxy{
		options:    options,
		db:         database,
		clusterDBs: make(map[string]*db.DB),
	}

	// 根据配置初始化路由算法
	if options.ClusterConfig != nil && options.ClusterConfig.ClusterMode {
		switch options.ClusterConfig.RouterType {
		case db.DirectHash:
			proxy.router = NewDirectHashRouter()
		case db.ConsistentHash:
			proxy.router = NewConsistentHashRouter(10) // 默认10个虚拟节点
		default:
			proxy.router = NewDirectHashRouter() // 默认使用直接哈希
		}
	}

	return proxy
}

// Put 根据路由将Put请求发送到对应的节点
func (p *Proxy) Put(key []byte, value []byte) error {
	// 如果未启用集群模式，直接在本地执行
	if p.options.ClusterConfig == nil || !p.options.ClusterConfig.ClusterMode {
		return p.db.Put(key, value)
	}

	// 根据路由算法确定目标节点
	nodes := p.options.ClusterConfig.ClusterAddresses
	targetNode := p.router.Route(key, nodes)

	// 如果路由到当前节点，则在本地执行
	if targetNode == "" || targetNode == p.options.ClusterConfig.NodeAddress {
		return p.db.Put(key, value)
	}

	// TODO: 在实际实现中，这里应该通过网络将请求发送到目标节点
	// 这里暂时模拟直接在本地执行
	return p.db.Put(key, value)
}

// Delete 根据路由将Delete请求发送到对应的节点
func (p *Proxy) Delete(key []byte) error {
	// 如果未启用集群模式，直接在本地执行
	if p.options.ClusterConfig == nil || !p.options.ClusterConfig.ClusterMode {
		return p.db.Delete(key)
	}

	// 根据路由算法确定目标节点
	nodes := p.options.ClusterConfig.ClusterAddresses
	targetNode := p.router.Route(key, nodes)

	// 如果路由到当前节点，则在本地执行
	if targetNode == "" || targetNode == p.options.ClusterConfig.NodeAddress {
		return p.db.Delete(key)
	}

	// TODO: 在实际实现中，这里应该通过网络将请求发送到目标节点
	// 这里暂时模拟直接在本地执行
	return p.db.Delete(key)
}

// Get 根据路由将Get请求发送到对应的节点
func (p *Proxy) Get(key []byte) ([]byte, error) {
	// 如果未启用集群模式，直接在本地执行
	if p.options.ClusterConfig == nil || !p.options.ClusterConfig.ClusterMode {
		return p.db.Get(key)
	}

	// 根据路由算法确定目标节点
	nodes := p.options.ClusterConfig.ClusterAddresses
	targetNode := p.router.Route(key, nodes)

	// 如果路由到当前节点，则在本地执行
	if targetNode == "" || targetNode == p.options.ClusterConfig.NodeAddress {
		return p.db.Get(key)
	}

	// TODO: 在实际实现中，这里应该通过网络将请求发送到目标节点
	// 这里暂时模拟直接在本地执行
	return p.db.Get(key)
}

// AddNode 添加集群节点（用于模拟）
func (p *Proxy) AddNode(address string, database *db.DB) {
	p.clusterLock.Lock()
	defer p.clusterLock.Unlock()
	p.clusterDBs[address] = database
}

// RemoveNode 移除集群节点（用于模拟）
func (p *Proxy) RemoveNode(address string) {
	p.clusterLock.Lock()
	defer p.clusterLock.Unlock()
	delete(p.clusterDBs, address)
}

// GetNode 获取指定节点的数据库实例（用于模拟）
func (p *Proxy) GetNode(address string) (*db.DB, error) {
	p.clusterLock.RLock()
	defer p.clusterLock.RUnlock()

	if db, exists := p.clusterDBs[address]; exists {
		return db, nil
	}

	return nil, fmt.Errorf("node %s not found", address)
}
