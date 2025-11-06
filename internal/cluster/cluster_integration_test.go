package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	regionmgr "nyxdb/internal/cluster/regions"
	db "nyxdb/internal/layers/engine"
	pd "nyxdb/internal/layers/pd"
	rafttransport "nyxdb/internal/layers/raft/transport"
	replication "nyxdb/internal/layers/txn/replication"
	regionpkg "nyxdb/internal/region"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	log.SetOutput(io.Discard)
	raft.SetLogger(&raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)})
	cleanupResidualClusterDirs()
	os.Exit(m.Run())
}

func cleanupResidualClusterDirs() {
	tmp := os.TempDir()
	entries, err := os.ReadDir(tmp)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "TestCluster") || strings.HasPrefix(name, "nyxdb-cluster") {
			_ = os.RemoveAll(filepath.Join(tmp, name))
		}
	}
}

type testClusterHandle struct {
	Cluster *Cluster
	Engine  *db.DB
	grpcSrv *grpc.Server
	lis     net.Listener
	once    sync.Once
}

func (h *testClusterHandle) Close() {
	h.once.Do(func() {
		if h.grpcSrv != nil {
			h.grpcSrv.GracefulStop()
		}
		if h.lis != nil {
			_ = h.lis.Close()
		}
		if h.Cluster != nil {
			_ = h.Cluster.Stop()
		}
		if h.Engine != nil {
			_ = h.Engine.Close()
		}
	})
}

func startClusterWithGRPC(t *testing.T, nodeID uint64, opts db.Options) *testClusterHandle {
	if opts.ClusterConfig == nil {
		t.Fatalf("ClusterConfig must be set for gRPC transport")
	}
	engine, err := db.Open(opts)
	require.NoError(t, err)

	cl, err := NewClusterWithTransport(nodeID, opts, engine, nil)
	require.NoError(t, err)

	lis, err := net.Listen("tcp", opts.ClusterConfig.NodeAddress)
	require.NoError(t, err)
	grpcSrv := grpc.NewServer()
	rafttransport.RegisterGRPCTransportServer(grpcSrv, cl.RaftRouter())
	go func() {
		_ = grpcSrv.Serve(lis)
	}()
	require.NoError(t, cl.Start())

	h := &testClusterHandle{
		Cluster: cl,
		Engine:  engine,
		grpcSrv: grpcSrv,
		lis:     lis,
	}
	// Ensure cleanup even if test exits early.
	t.Cleanup(h.Close)
	return h
}

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

	clusterHandle := startClusterWithGRPC(t, 1, opts)
	cl := clusterHandle.Cluster
	engine := clusterHandle.Engine

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

func TestClusterRegistersRegionsWithPD(t *testing.T) {
	dir := t.TempDir()
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = false
	opts.ClusterConfig = &db.ClusterOptions{
		ClusterMode:      true,
		NodeAddress:      "127.0.0.1:9101",
		ClusterAddresses: []string{"1@127.0.0.1:9101"},
	}

	clusterHandle := startClusterWithGRPC(t, 1, opts)
	cl := clusterHandle.Cluster

	require.Eventually(t, func() bool { return cl.IsLeader() }, 5*time.Second, 50*time.Millisecond)

	svc := pd.NewService()
	var err error
	cl.AttachPD(svc, time.Second)

	require.Eventually(t, func() bool {
		snapshot, ok := svc.RegionSnapshot(1)
		if !ok {
			return false
		}
		return snapshot.Region.ID == regionmgr.DefaultRegionID
	}, 2*time.Second, 50*time.Millisecond)

	newRegion, err := cl.CreateStaticRegion(regionpkg.KeyRange{Start: []byte("a"), End: []byte("b")})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		snapshot, ok := svc.RegionSnapshot(uint64(newRegion.ID))
		if !ok {
			return false
		}
		return snapshot.Region.State == regionpkg.StateActive && len(snapshot.Region.Peers) > 0
	}, 2*time.Second, 50*time.Millisecond)

	snapshots := svc.RegionsByStore(cl.nodeID)
	require.GreaterOrEqual(t, len(snapshots), 1)

	require.NoError(t, cl.RemoveRegion(newRegion.ID))
	require.Eventually(t, func() bool {
		snapshot, ok := svc.RegionSnapshot(uint64(newRegion.ID))
		if !ok {
			return false
		}
		return snapshot.Region.State == regionpkg.StateTombstone
	}, 2*time.Second, 50*time.Millisecond)
}

func TestClusterSingleNodeMultipleRegions(t *testing.T) {
	dir := t.TempDir()
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = false
	opts.ClusterConfig = &db.ClusterOptions{
		ClusterMode:      true,
		NodeAddress:      "127.0.0.1:9301",
		ClusterAddresses: []string{"1@127.0.0.1:9301"},
	}

	clusterHandle := startClusterWithGRPC(t, 1, opts)
	cl := clusterHandle.Cluster

	require.Eventually(t, func() bool { return cl.IsLeader() }, 5*time.Second, 50*time.Millisecond)

	region, err := cl.CreateStaticRegion(regionpkg.KeyRange{Start: []byte("a"), End: []byte("m")})
	require.NoError(t, err)
	require.NotNil(t, region)

	regionBMeta := cl.RegionForKey([]byte("b"))
	require.NotNil(t, regionBMeta)
	require.Equal(t, region.ID, regionBMeta.ID)

	regionZMeta := cl.RegionForKey([]byte("z"))
	require.NotNil(t, regionZMeta)
	require.Equal(t, regionmgr.DefaultRegionID, regionZMeta.ID)

	require.Eventually(t, func() bool {
		return cl.Put([]byte("b"), []byte("value-b")) == nil
	}, 5*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		val, err := cl.GetLinearizable(ctx, []byte("b"))
		return err == nil && bytes.Equal(val, []byte("value-b"))
	}, 5*time.Second, 50*time.Millisecond)

	require.Eventually(t, func() bool {
		return cl.Put([]byte("z"), []byte("value-z")) == nil
	}, 5*time.Second, 50*time.Millisecond)
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		val, err := cl.GetLinearizable(ctx, []byte("z"))
		return err == nil && bytes.Equal(val, []byte("value-z"))
	}, 5*time.Second, 50*time.Millisecond)

	regionB := cl.RegionForKey([]byte("b"))
	require.NotNil(t, regionB)
	require.Equal(t, region.ID, regionB.ID)

	regionZ := cl.RegionForKey([]byte("z"))
	require.NotNil(t, regionZ)
	require.Equal(t, regionmgr.DefaultRegionID, regionZ.ID)
}

func TestClusterMultiNodeMultipleRegions(t *testing.T) {
	baseDir := t.TempDir()
	network := newChaosNetwork()

	clusterAddrs := []string{
		"1@127.0.0.1:20001",
		"2@127.0.0.1:20002",
		"3@127.0.0.1:20003",
	}

	nodes := []*nodeHarness{}
	for i := 1; i <= 3; i++ {
		opts := db.DefaultOptions
		opts.DirPath = filepath.Join(baseDir, fmt.Sprintf("node-%d", i))
		opts.EnableDiagnostics = false
		opts.ClusterConfig = &db.ClusterOptions{
			ClusterMode:      true,
			NodeAddress:      fmt.Sprintf("127.0.0.1:2000%d", i),
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

	region, err := leader.cluster.CreateStaticRegion(regionpkg.KeyRange{Start: []byte("a"), End: []byte("m")})
	require.NoError(t, err)
	require.NotNil(t, region)

	for _, n := range nodes {
		if n == leader {
			continue
		}
		require.NoError(t, n.cluster.EnsureRegionReplica(region.Clone()))
	}

	regionLeader := waitForRegionLeader(t, nodes, region.ID)
	require.NotNil(t, regionLeader)

	require.Eventually(t, func() bool {
		return regionLeader.cluster.Put([]byte("b"), []byte("value-b")) == nil
	}, 5*time.Second, 50*time.Millisecond)

	for _, n := range nodes {
		require.Eventually(t, func() bool {
			val, err := n.cluster.Get([]byte("b"))
			return err == nil && bytes.Equal(val, []byte("value-b"))
		}, 5*time.Second, 50*time.Millisecond)
	}

	follower := findRegionFollower(nodes, region.ID, regionLeader.id)
	require.NotNil(t, follower)
	err = follower.cluster.Put([]byte("b"), []byte("value-b2"))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNotLeader)

	require.Eventually(t, func() bool {
		leader := currentRegionLeader(nodes, regionmgr.DefaultRegionID)
		if leader == nil {
			return false
		}
		return leader.cluster.Put([]byte("z"), []byte("value-z")) == nil
	}, 5*time.Second, 50*time.Millisecond)

	for _, n := range nodes {
		require.Eventually(t, func() bool {
			val, err := n.cluster.Get([]byte("z"))
			return err == nil && bytes.Equal(val, []byte("value-z"))
		}, 5*time.Second, 50*time.Millisecond)
	}
}

func TestClusterSyncRegionsFromPD(t *testing.T) {
	dir := t.TempDir()
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = false
	opts.ClusterConfig = &db.ClusterOptions{
		ClusterMode:      true,
		NodeAddress:      "127.0.0.1:9201",
		ClusterAddresses: []string{"1@127.0.0.1:9201"},
	}

	clusterHandle := startClusterWithGRPC(t, 1, opts)
	cl := clusterHandle.Cluster

	svc := pd.NewService()
	var err error

	defaultPeerID := peerIDFor(regionmgr.DefaultRegionID, 1)
	defaultRegion := regionpkg.Region{
		ID:     regionmgr.DefaultRegionID,
		Range:  regionpkg.KeyRange{},
		Epoch:  regionpkg.Epoch{Version: 5, ConfVersion: 4},
		State:  regionpkg.StateActive,
		Leader: defaultPeerID,
		Peers: []regionpkg.Peer{
			{ID: defaultPeerID, StoreID: 1, Role: regionpkg.Voter},
		},
	}
	_, err = svc.RegisterRegion(defaultRegion)
	require.NoError(t, err)

	newPeerID := peerIDFor(2, 1)
	regionTwo := regionpkg.Region{
		ID: 2,
		Range: regionpkg.KeyRange{
			Start: []byte("m"),
			End:   []byte("t"),
		},
		Epoch:  regionpkg.Epoch{Version: 3, ConfVersion: 2},
		State:  regionpkg.StateActive,
		Leader: newPeerID,
		Peers: []regionpkg.Peer{
			{ID: newPeerID, StoreID: 1, Role: regionpkg.Voter},
			{ID: peerIDFor(2, 2), StoreID: 2, Role: regionpkg.Learner},
		},
	}
	_, err = svc.RegisterRegion(regionTwo)
	require.NoError(t, err)

	cl.AttachPD(svc, time.Second)

	// PD metadata should override核心结构化信息（范围/Epoch）。
	localDefault := cl.regionMgr.Region(regionmgr.DefaultRegionID)
	require.NotNil(t, localDefault)
	require.Equal(t, uint64(5), localDefault.Epoch.Version)

	// Region 2 should now exist locally even though it wasn't persisted before.
	localRegionTwo := cl.regionMgr.Region(2)
	require.NotNil(t, localRegionTwo)
	require.Equal(t, []byte("m"), localRegionTwo.Range.Start)
	require.Equal(t, []byte("t"), localRegionTwo.Range.End)
	require.Equal(t, uint64(3), localRegionTwo.Epoch.Version)
	require.Len(t, localRegionTwo.Peers, 2)

	require.NoError(t, cl.Start())
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

	clusterHandle := startClusterWithGRPC(t, 1, opts)
	cl := clusterHandle.Cluster

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

	clusterHandle.Close()

	clusterHandle2 := startClusterWithGRPC(t, 1, opts)
	cl2 := clusterHandle2.Cluster

	require.Eventually(t, func() bool {
		return cl2.Members()[2] == "127.0.0.1:9002"
	}, 5*time.Second, 50*time.Millisecond)
}

func TestClusterTriggerSnapshot(t *testing.T) {
	dir := t.TempDir()
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = false
	opts.ClusterConfig = &db.ClusterOptions{
		ClusterMode:      true,
		NodeAddress:      "127.0.0.1:19001",
		ClusterAddresses: []string{"1@127.0.0.1:19001"},
	}

	clusterHandle := startClusterWithGRPC(t, 1, opts)
	cl := clusterHandle.Cluster

	require.Eventually(t, func() bool { return cl.IsLeader() }, 5*time.Second, 50*time.Millisecond)

	for i := 0; i < 8; i++ {
		key := []byte(fmt.Sprintf("snap-%d", i))
		require.NoError(t, cl.Put(key, []byte("value")))
	}

	require.NoError(t, cl.TriggerSnapshot(true))
	snapshot, err := cl.storage.Snapshot()
	require.NoError(t, err)
	require.Greater(t, snapshot.Metadata.Index, uint64(0))
	first, err := cl.storage.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, snapshot.Metadata.Index+1, first)
}

func TestClusterChaosResilience(t *testing.T) {
	rand.Seed(1)

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

func TestClusterSnapshotCatchUp(t *testing.T) {
	baseDir := t.TempDir()
	network := newChaosNetwork()

	clusterAddrs := []string{
		"1@127.0.0.1:19101",
		"2@127.0.0.1:19102",
		"3@127.0.0.1:19103",
	}

	nodes := []*nodeHarness{}
	for i := 1; i <= 3; i++ {
		opts := db.DefaultOptions
		opts.DirPath = filepath.Join(baseDir, fmt.Sprintf("node-%d", i))
		opts.EnableDiagnostics = false
		opts.ClusterConfig = &db.ClusterOptions{
			ClusterMode:            true,
			NodeAddress:            fmt.Sprintf("127.0.0.1:1910%d", i),
			ClusterAddresses:       clusterAddrs,
			AutoSnapshot:           false,
			SnapshotThreshold:      16,
			SnapshotCatchUpEntries: 8,
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

	lagger := pickFollower(nodes)
	require.NotNil(t, lagger)

	network.setPartition(leader.id, lagger.id, true)
	defer network.setPartition(leader.id, lagger.id, false)

	const totalWrites = 200
	for i := 0; i < totalWrites; i++ {
		key := []byte(fmt.Sprintf("snap-key-%d", i))
		require.NoError(t, leader.cluster.Put(key, []byte("value")))
	}

	require.Eventually(t, func() bool {
		return leader.cluster.raftNode.AppliedIndex() >= uint64(totalWrites)
	}, 5*time.Second, 50*time.Millisecond)

	require.True(t, lagger.cluster.raftNode.AppliedIndex() < uint64(totalWrites))

	require.NoError(t, leader.cluster.TriggerSnapshot(true))

	first, err := leader.cluster.storage.FirstIndex()
	require.NoError(t, err)
	catchUp := leader.cluster.snapshotCatchUpEntries
	require.Greater(t, first, uint64(totalWrites)-catchUp)

	stopNodeHarness(lagger)
	network.setPartition(leader.id, lagger.id, false)

	require.NoError(t, leader.cluster.Put([]byte("post-snap"), []byte("v2")))

	startNodeHarness(t, lagger)

	leader = waitForHealthyCluster(t, nodes)
	require.NotNil(t, leader)

	waitForValue(t, nodes, []byte("snap-key-199"), []byte("value"))

	require.Eventually(t, func() bool {
		if lagger.cluster == nil {
			return false
		}
		val, err := lagger.cluster.db.Get([]byte("snap-key-199"))
		if err != nil {
			return false
		}
		return bytes.Equal(val, []byte("value"))
	}, 5*time.Second, 50*time.Millisecond)

	require.NoError(t, leader.cluster.Put([]byte("after-recover"), []byte("ok")))
	waitForValue(t, nodes, []byte("after-recover"), []byte("ok"))
}

func TestClusterPDHeartbeatWithRegions(t *testing.T) {
	baseDir := t.TempDir()
	opts := db.DefaultOptions
	opts.DirPath = baseDir
	opts.EnableDiagnostics = false
	opts.ClusterConfig = &db.ClusterOptions{
		ClusterMode:      true,
		NodeAddress:      "127.0.0.1:19001",
		ClusterAddresses: []string{"1@127.0.0.1:19001"},
	}

	engine, err := db.Open(opts)
	require.NoError(t, err)

	cl, err := NewClusterWithTransport(1, opts, engine, nil)
	require.NoError(t, err)

	lis, err := net.Listen("tcp", opts.ClusterConfig.NodeAddress)
	require.NoError(t, err)
	grpcSrv := grpc.NewServer()
	rafttransport.RegisterGRPCTransportServer(grpcSrv, cl.RaftRouter())
	go func() {
		_ = grpcSrv.Serve(lis)
	}()
	clusterHandle := &testClusterHandle{
		Cluster: cl,
		Engine:  engine,
		grpcSrv: grpcSrv,
		lis:     lis,
	}
	t.Cleanup(clusterHandle.Close)

	// Add a second static region before start so it boots with its own Raft replica.
	region, err := cl.CreateStaticRegion(regionpkg.KeyRange{Start: []byte("m"), End: []byte("z")})
	require.NoError(t, err)
	require.Equal(t, regionmgr.DefaultRegionID+1, region.ID)

	svc := pd.NewService()
	cl.AttachPD(svc, 20*time.Millisecond)

	require.NoError(t, cl.Start())

	require.Eventually(t, func() bool {
		hb, ok := svc.Store(1)
		return ok && len(hb.Regions) >= 2
	}, time.Second, 20*time.Millisecond)

	hb, ok := svc.Store(1)
	require.True(t, ok)
	require.Equal(t, "127.0.0.1:19001", hb.Address)
	require.GreaterOrEqual(t, len(hb.Regions), 2)

	var seen []regionpkg.ID
	for _, r := range hb.Regions {
		seen = append(seen, r.Region.ID)
	}
	require.ElementsMatch(t, []regionpkg.ID{regionmgr.DefaultRegionID, region.ID}, seen)

	regionSnaps := svc.RegionsSnapshot()
	require.GreaterOrEqual(t, len(regionSnaps), 2)
	snapshot, ok := svc.RegionSnapshot(uint64(region.ID))
	require.True(t, ok)
	require.Equal(t, region.ID, snapshot.Region.ID)
	require.NotEmpty(t, snapshot.Peers)

	byKey, ok := svc.RegionByKey([]byte("m"))
	require.True(t, ok)
	require.Equal(t, region.ID, byKey.Region.ID)
}

func TestClusterMultiRegionRouting(t *testing.T) {
	dir := t.TempDir()
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = false
	opts.ClusterConfig = &db.ClusterOptions{
		ClusterMode:      true,
		NodeAddress:      "127.0.0.1:29001",
		ClusterAddresses: []string{"1@127.0.0.1:29001"},
	}

	clusterHandle := startClusterWithGRPC(t, 1, opts)
	cl := clusterHandle.Cluster

	require.Eventually(t, func() bool { return cl.IsLeader() }, 5*time.Second, 50*time.Millisecond)

	region, err := cl.CreateStaticRegion(regionpkg.KeyRange{Start: []byte("m")})
	require.NoError(t, err)
	replica := cl.replica(region.ID)
	require.NotNil(t, replica)

	require.Eventually(t, func() bool {
		return cl.router.RegionByPeer(replica.PeerID) == region.ID
	}, 5*time.Second, 50*time.Millisecond)

	commitTs := cl.allocateCommitTs()
	cmd := &replication.Command{
		CommitTs: commitTs,
		Operations: []replication.Operation{
			{Key: []byte("multi-region-key"), Value: []byte("value"), Type: replication.OpPut},
		},
	}
	data, err := cmd.Marshal()
	require.NoError(t, err)
	require.NoError(t, replica.Node.Propose(data))

	require.Eventually(t, func() bool {
		val, err := cl.Get([]byte("multi-region-key"))
		return err == nil && bytes.Equal(val, []byte("value"))
	}, 5*time.Second, 50*time.Millisecond)

	regionsFile := filepath.Join(opts.DirPath, "cluster", regionsFileName)
	require.Eventually(t, func() bool {
		content, err := os.ReadFile(regionsFile)
		if err != nil {
			return false
		}
		return bytes.Contains(content, []byte("\"id\": 2"))
	}, 5*time.Second, 50*time.Millisecond)
}

func TestClusterDiagnostics(t *testing.T) {
	dir := t.TempDir()
	opts := db.DefaultOptions
	opts.DirPath = dir
	opts.EnableDiagnostics = true
	opts.ClusterConfig = &db.ClusterOptions{
		ClusterMode:      true,
		NodeAddress:      "127.0.0.1:19501",
		ClusterAddresses: []string{"1@127.0.0.1:19501"},
	}

	engine, err := db.Open(opts)
	require.NoError(t, err)

	cl, err := NewClusterWithTransport(1, opts, engine, nil)
	require.NoError(t, err)
	cl.diagnosticsInterval = 20 * time.Millisecond
	updates := make(chan Diagnostics, 2)
	cl.RegisterDiagnosticsObserver(func(d Diagnostics) {
		select {
		case updates <- d:
		default:
		}
	})

	lis, err := net.Listen("tcp", opts.ClusterConfig.NodeAddress)
	require.NoError(t, err)
	grpcSrv := grpc.NewServer()
	rafttransport.RegisterGRPCTransportServer(grpcSrv, cl.RaftRouter())
	go func() {
		_ = grpcSrv.Serve(lis)
	}()
	clusterHandle := &testClusterHandle{
		Cluster: cl,
		Engine:  engine,
		grpcSrv: grpcSrv,
		lis:     lis,
	}
	t.Cleanup(clusterHandle.Close)
	require.NoError(t, cl.Start())

	require.Eventually(t, func() bool { return cl.IsLeader() }, 5*time.Second, 50*time.Millisecond)
	require.NoError(t, cl.Put([]byte("diag"), []byte("value")))

	require.Eventually(t, func() bool {
		d := cl.Diagnostics()
		return d.CommittedIndex >= 1 && d.AppliedIndex >= 1
	}, 5*time.Second, 50*time.Millisecond)

	require.Eventually(t, func() bool {
		d := cl.Diagnostics()
		return d.LastRaftIndex >= d.CommittedIndex
	}, 5*time.Second, 50*time.Millisecond)

	require.Eventually(t, func() bool {
		select {
		case <-updates:
			return true
		default:
			return false
		}
	}, 2*time.Second, 20*time.Millisecond)

	d := cl.Diagnostics()
	require.Equal(t, 1, d.MemberCount)
	require.GreaterOrEqual(t, d.LastRaftIndex, d.CommittedIndex)
	require.False(t, d.SnapshotInProgress)
	require.Equal(t, 0, d.ReadTxnCount)
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
	storeID := storeIDFromPeer(to)
	n.mu.RLock()
	cl := n.nodes[storeID]
	delay := n.delay
	_, blocked := n.partitions[pairKey(from, storeID)]
	n.mu.RUnlock()

	if blocked || cl == nil {
		return nil
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	for _, msg := range messages {
		if router := cl.RaftRouter(); router != nil {
			if err := router.Step(context.Background(), msg); err != nil {
				return err
			}
			continue
		}
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
	engine, err := db.Open(h.opts)
	require.NoError(t, err)
	h.engine = engine

	cl, err := NewClusterWithTransport(h.id, h.opts, engine, h.transport)
	require.NoError(t, err)
	h.cluster = cl
	h.network.attach(h.id, cl)
	require.NoError(t, cl.Start())
	cl.membersMu.RLock()
	count := len(cl.members)
	cl.membersMu.RUnlock()
	require.Equal(t, 3, count)
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

func waitForRegionLeader(t *testing.T, nodes []*nodeHarness, regionID regionpkg.ID) *nodeHarness {
	var leader *nodeHarness
	require.Eventually(t, func() bool {
		leader = currentRegionLeader(nodes, regionID)
		return leader != nil
	}, 5*time.Second, 50*time.Millisecond)
	return leader
}

func currentRegionLeader(nodes []*nodeHarness, regionID regionpkg.ID) *nodeHarness {
	for _, n := range nodes {
		if n.cluster == nil {
			continue
		}
		rep := n.cluster.replica(regionID)
		if rep != nil && rep.Node != nil && rep.Node.IsLeader() {
			return n
		}
	}
	return nil
}

func findRegionFollower(nodes []*nodeHarness, regionID regionpkg.ID, leaderID uint64) *nodeHarness {
	for _, n := range nodes {
		if n.cluster == nil || n.id == leaderID {
			continue
		}
		rep := n.cluster.replica(regionID)
		if rep != nil && rep.Node != nil && !rep.Node.IsLeader() {
			return n
		}
	}
	return nil
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
