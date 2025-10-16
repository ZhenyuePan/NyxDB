package cluster

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	db "nyxdb/internal/engine"
	rafttransport "nyxdb/internal/raft"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestClusterLinearizableRead(t *testing.T) {
	dir := t.TempDir()
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = false
	opts.ClusterConfig = &db.ClusterOptions{
		ClusterMode:      true,
		NodeAddress:      "127.0.0.1:9001",
		ClusterAddresses: []string{"1@127.0.0.1:9001"},
	}

	engine, err := db.Open(opts)
	require.NoError(t, err)

	cl, err := NewClusterWithTransport(1, opts, engine, rafttransport.NewDefaultTransport())
	require.NoError(t, err)
	require.NoError(t, cl.Start())
	defer func() { _ = cl.Stop() }()

	require.Eventually(t, func() bool { return cl.IsLeader() }, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, cl.Put([]byte("key"), []byte("value")))
	require.Eventually(t, func() bool {
		val, err := engine.Get([]byte("key"))
		return err == nil && bytes.Equal(val, []byte("value"))
	}, 5*time.Second, 50*time.Millisecond)

	handle, readTs, err := cl.BeginReadTxn()
	require.NoError(t, err)
	require.NotZero(t, readTs)

	txnVal, found, err := cl.ReadTxnGet(handle, []byte("key"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("value"), txnVal)

	require.NoError(t, cl.Put([]byte("key"), []byte("value-new")))
	require.Eventually(t, func() bool {
		v, err := cl.GetLinearizable(context.Background(), []byte("key"))
		return err == nil && bytes.Equal(v, []byte("value-new"))
	}, 5*time.Second, 50*time.Millisecond)

	txnVal, found, err = cl.ReadTxnGet(handle, []byte("key"))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("value"), txnVal)

	require.NoError(t, cl.EndReadTxn(handle))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	val, err := cl.GetLinearizable(ctx, []byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-new"), val)
}

func TestClusterMembershipPersistence(t *testing.T) {
	dir := t.TempDir()
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = false
	opts.ClusterConfig = &db.ClusterOptions{
		ClusterMode:      true,
		NodeAddress:      "127.0.0.1:9001",
		ClusterAddresses: []string{"1@127.0.0.1:9001"},
	}

	engine, err := db.Open(opts)
	require.NoError(t, err)

	cl, err := NewClusterWithTransport(1, opts, engine, rafttransport.NewDefaultTransport())
	require.NoError(t, err)
	require.NoError(t, cl.Start())

	require.Eventually(t, func() bool { return cl.IsLeader() }, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, cl.AddMember(2, "127.0.0.1:9002"))
	require.Eventually(t, func() bool {
		return cl.Members()[2] == "127.0.0.1:9002"
	}, 5*time.Second, 50*time.Millisecond)

	membersFile := filepath.Join(opts.DirPath, "cluster", membersFileName)
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(membersFile)
		if err != nil {
			return false
		}
		return bytes.Contains(data, []byte("127.0.0.1:9002"))
	}, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, cl.Stop())

	engine2, err := db.Open(opts)
	require.NoError(t, err)
	defer engine2.Close()

	cl2, err := NewClusterWithTransport(1, opts, engine2, rafttransport.NewDefaultTransport())
	require.NoError(t, err)
	require.NoError(t, cl2.Start())
	defer func() { _ = cl2.Stop() }()

	require.Eventually(t, func() bool {
		return cl2.Members()[2] == "127.0.0.1:9002"
	}, 5*time.Second, 50*time.Millisecond)
}

func TestClusterChaosResilience(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	baseDir := t.TempDir()
	network := newChaosNetwork()

	clusterAddrs := []string{
		"1@127.0.0.1:19001",
		"2@127.0.0.1:19002",
		"3@127.0.0.1:19003",
	}

	nodes := []*nodeHarness{}
	for i := 1; i <= 3; i++ {
		opts := db.DefaultOptions
		opts.DirPath = filepath.Join(baseDir, fmt.Sprintf("node-%d", i))
		opts.EnableDiagnostics = false
		opts.ClusterConfig = &db.ClusterOptions{
			ClusterMode:      true,
			NodeAddress:      fmt.Sprintf("127.0.0.1:1900%d", i),
			ClusterAddresses: clusterAddrs,
		}

		transport := newChaosTransport(network, uint64(i))
		h := &nodeHarness{
			id:        uint64(i),
			opts:      opts,
			transport: transport,
			network:   network,
		}
		startNodeHarness(t, h)
		nodes = append(nodes, h)
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			stopNodeHarness(n)
		}
	})

    leader := waitForHealthyCluster(t, nodes)
    require.NotNil(t, leader)

    for _, n := range nodes {
        if n.id == leader.id {
            continue
        }
        require.NoError(t, leader.cluster.AddMember(n.id, n.opts.ClusterConfig.NodeAddress))
        leader = waitForHealthyCluster(t, nodes)
    }

	require.NoError(t, leader.cluster.Put([]byte("chaos"), []byte("v1")))
	waitForValue(t, nodes, []byte("chaos"), []byte("v1"))

	follower := pickFollower(nodes)
	require.NotNil(t, follower)
	network.setPartition(leader.id, follower.id, true)
	time.Sleep(300 * time.Millisecond)
	require.NoError(t, leader.cluster.Put([]byte("chaos"), []byte("v2")))
	waitForValue(t, nodes, []byte("chaos"), []byte("v2"))
	network.setPartition(leader.id, follower.id, false)

	waitForValue(t, nodes, []byte("chaos"), []byte("v2"))

	leader = waitForHealthyCluster(t, nodes)

	network.setDelay(150 * time.Millisecond)
	require.NoError(t, leader.cluster.Put([]byte("chaos"), []byte("v3")))
	network.setDelay(0)
	waitForValue(t, nodes, []byte("chaos"), []byte("v3"))

	// restart a follower (randomly choose one of the non-leader nodes)
	victims := collectFollowers(nodes)
	require.NotEmpty(t, victims)
	victim := victims[rand.Intn(len(victims))]
	stopNodeHarness(victim)
	time.Sleep(300 * time.Millisecond)
	startNodeHarness(t, victim)

	leader = waitForHealthyCluster(t, nodes)
	require.NotNil(t, leader)
	waitForValue(t, nodes, []byte("chaos"), []byte("v3"))

	require.NoError(t, leader.cluster.Put([]byte("chaos"), []byte("v4")))
	waitForValue(t, nodes, []byte("chaos"), []byte("v4"))
}

type chaosNetwork struct {
	mu         sync.RWMutex
	nodes      map[uint64]*Cluster
	partitions map[[2]uint64]struct{}
	delay      time.Duration
}

func newChaosNetwork() *chaosNetwork {
	return &chaosNetwork{
		nodes:      make(map[uint64]*Cluster),
		partitions: make(map[[2]uint64]struct{}),
	}
}

func (n *chaosNetwork) attach(id uint64, cl *Cluster) {
	n.mu.Lock()
	n.nodes[id] = cl
	n.mu.Unlock()
}

func (n *chaosNetwork) detach(id uint64) {
	n.mu.Lock()
	delete(n.nodes, id)
	n.mu.Unlock()
}

func (n *chaosNetwork) setPartition(a, b uint64, blocked bool) {
	key := pairKey(a, b)
	n.mu.Lock()
	if blocked {
		n.partitions[key] = struct{}{}
	} else {
		delete(n.partitions, key)
	}
	n.mu.Unlock()
}

func (n *chaosNetwork) setDelay(d time.Duration) {
	n.mu.Lock()
	n.delay = d
	n.mu.Unlock()
}

func (n *chaosNetwork) send(from, to uint64, messages []raftpb.Message) error {
	n.mu.RLock()
	cl := n.nodes[to]
	delay := n.delay
	_, blocked := n.partitions[pairKey(from, to)]
	n.mu.RUnlock()

	if blocked || cl == nil {
		return nil
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	for _, msg := range messages {
		if err := cl.raftNode.Step(context.Background(), msg); err != nil {
			return err
		}
	}
	return nil
}

type chaosTransport struct {
	id  uint64
	net *chaosNetwork
}

func newChaosTransport(net *chaosNetwork, id uint64) *chaosTransport {
	return &chaosTransport{id: id, net: net}
}

func (t *chaosTransport) Send(to uint64, messages []raftpb.Message) error {
	return t.net.send(t.id, to, messages)
}

func (t *chaosTransport) SendSnapshot(to uint64, snapshot raftpb.Snapshot) error {
	msg := raftpb.Message{To: to, Type: raftpb.MsgSnap, Snapshot: snapshot}
	return t.net.send(t.id, to, []raftpb.Message{msg})
}

func (t *chaosTransport) AddMember(id uint64, peerURLs []string) error {
	return nil
}

func (t *chaosTransport) RemoveMember(id uint64) error {
	return nil
}

type nodeHarness struct {
	id        uint64
	opts      db.Options
	engine    *db.DB
	cluster   *Cluster
	transport *chaosTransport
	network   *chaosNetwork
}

func startNodeHarness(t *testing.T, h *nodeHarness) {
    require.NotNil(t, h.opts.ClusterConfig)
    require.Len(t, h.opts.ClusterConfig.ClusterAddresses, 3)
    parsed := parsePeerAddresses(h.opts.ClusterConfig.ClusterAddresses)
    require.Len(t, parsed, 3)
    engine, err := db.Open(h.opts)
    require.NoError(t, err)
    h.engine = engine

    cl, err := NewClusterWithTransport(h.id, h.opts, engine, h.transport)
    require.NoError(t, err)
    h.cluster = cl
    h.network.attach(h.id, cl)
    require.NoError(t, cl.Start())
}

func stopNodeHarness(h *nodeHarness) {
	if h.cluster != nil {
		h.network.detach(h.id)
		_ = h.cluster.Stop()
		h.cluster = nil
		h.engine = nil
	}
}

func waitForLeader(t *testing.T, nodes []*nodeHarness) *nodeHarness {
	var leader *nodeHarness
	require.Eventually(t, func() bool {
		leader = currentLeader(nodes)
		return leader != nil
	}, 5*time.Second, 50*time.Millisecond)
	return leader
}

func waitForHealthyCluster(t *testing.T, nodes []*nodeHarness) *nodeHarness {
	var leader *nodeHarness
	require.Eventually(t, func() bool {
		leader = currentLeader(nodes)
		if leader == nil {
			return false
		}
		followers := collectFollowers(nodes)
		return len(followers) >= 1
	}, 10*time.Second, 50*time.Millisecond)
	return leader
}

func currentLeader(nodes []*nodeHarness) *nodeHarness {
	for _, n := range nodes {
		if n.cluster != nil && n.cluster.IsLeader() {
			return n
		}
	}
	return nil
}

func pickFollower(nodes []*nodeHarness) *nodeHarness {
	followers := collectFollowers(nodes)
	if len(followers) == 0 {
		return nil
	}
	return followers[0]
}

func collectFollowers(nodes []*nodeHarness) []*nodeHarness {
	followers := []*nodeHarness{}
	for _, n := range nodes {
		if n.cluster != nil && !n.cluster.IsLeader() {
			followers = append(followers, n)
		}
	}
	return followers
}

func waitForValue(t *testing.T, nodes []*nodeHarness, key, expected []byte) {
	require.Eventually(t, func() bool {
		leader := currentLeader(nodes)
		if leader == nil {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		val, err := leader.cluster.GetLinearizable(ctx, key)
		return err == nil && bytes.Equal(val, expected)
	}, 5*time.Second, 50*time.Millisecond)
}

func pairKey(a, b uint64) [2]uint64 {
	if a < b {
		return [2]uint64{a, b}
	}
	return [2]uint64{b, a}
}
