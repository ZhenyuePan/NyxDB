package grpcserver

import (
	"context"
	"fmt"
	"testing"

	"nyxdb/internal/cluster"
	db "nyxdb/internal/layers/engine"
	api "nyxdb/pkg/api"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeCluster struct {
	store        map[string][]byte
	members      map[uint64]string
	snapshots    map[string]map[string][]byte
	nextHandleID uint64
	mergeCount   int
	leader       string
	getErr       error
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{
		store:     make(map[string][]byte),
		members:   make(map[uint64]string),
		snapshots: make(map[string]map[string][]byte),
	}
}

func (f *fakeCluster) Put(key, value []byte) error {
	f.store[string(key)] = append([]byte(nil), value...)
	return nil
}

func (f *fakeCluster) Get(key []byte) ([]byte, error) {
	val, ok := f.store[string(key)]
	if !ok {
		return nil, db.ErrKeyNotFound
	}
	return append([]byte(nil), val...), nil
}

func (f *fakeCluster) GetLinearizable(ctx context.Context, key []byte) ([]byte, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.Get(key)
}

func (f *fakeCluster) Delete(key []byte) error {
	delete(f.store, string(key))
	return nil
}

func (f *fakeCluster) AddMember(id uint64, addr string) error {
	f.members[id] = addr
	return nil
}

func (f *fakeCluster) RemoveMember(id uint64) error {
	delete(f.members, id)
	return nil
}

func (f *fakeCluster) Members() map[uint64]string {
	cp := make(map[uint64]string)
	for k, v := range f.members {
		cp[k] = v
	}
	return cp
}

func (f *fakeCluster) BeginReadTxn() ([]byte, uint64, error) {
	f.nextHandleID++
	handle := []byte(fmt.Sprintf("h-%d", f.nextHandleID))
	snap := make(map[string][]byte)
	for k, v := range f.store {
		snap[k] = append([]byte(nil), v...)
	}
	f.snapshots[string(handle)] = snap
	return append([]byte(nil), handle...), uint64(f.nextHandleID), nil
}

func (f *fakeCluster) ReadTxnGet(handle []byte, key []byte) ([]byte, bool, error) {
	snap, ok := f.snapshots[string(handle)]
	if !ok {
		return nil, false, cluster.ErrReadTxnNotFound
	}
	val, found := snap[string(key)]
	if !found {
		return nil, false, nil
	}
	return append([]byte(nil), val...), true, nil
}

func (f *fakeCluster) EndReadTxn(handle []byte) error {
	if _, ok := f.snapshots[string(handle)]; !ok {
		return cluster.ErrReadTxnNotFound
	}
	delete(f.snapshots, string(handle))
	return nil
}

func (f *fakeCluster) TriggerMerge(force bool) error {
	if force {
		f.mergeCount++
	}
	return nil
}

func (f *fakeCluster) TriggerSnapshot(force bool) error {
	return nil
}

func (f *fakeCluster) LeaderAddress() string {
	return f.leader
}

func (f *fakeCluster) SnapshotStatus() cluster.SnapshotStatus {
	return cluster.SnapshotStatus{}
}

func TestKVService_PutGetDelete(t *testing.T) {
	cl := newFakeCluster()
	svc := NewKVService(cl)

	_, err := svc.Put(context.Background(), &api.PutRequest{Key: []byte("k"), Value: []byte("v")})
	assert.Nil(t, err)

	resp, err := svc.Get(context.Background(), &api.GetRequest{Key: []byte("k")})
	assert.Nil(t, err)
	assert.True(t, resp.Found)
	assert.Equal(t, []byte("v"), resp.Value)

	_, err = svc.Delete(context.Background(), &api.DeleteRequest{Key: []byte("k")})
	assert.Nil(t, err)

	resp, err = svc.Get(context.Background(), &api.GetRequest{Key: []byte("k")})
	assert.Nil(t, err)
	assert.False(t, resp.Found)
}

func TestKVService_GetNotLeader(t *testing.T) {
	cl := newFakeCluster()
	cl.getErr = fmt.Errorf("%w: leader=%s", cluster.ErrNotLeader, "127.0.0.1:9002")
	cl.leader = "127.0.0.1:9002"
	svc := NewKVService(cl)

	_, err := svc.Get(context.Background(), &api.GetRequest{Key: []byte("k")})
	assert.Error(t, err)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Contains(t, st.Message(), "leader=127.0.0.1:9002")
}

func TestKVService_ReadTxn(t *testing.T) {
	cl := newFakeCluster()
	_ = cl.Put([]byte("foo"), []byte("bar"))
	svc := NewKVService(cl)

	beginResp, err := svc.BeginReadTxn(context.Background(), &api.BeginReadTxnRequest{})
	assert.NoError(t, err)
	assert.NotEmpty(t, beginResp.Handle)
	assert.NotZero(t, beginResp.ReadTs)

	getResp, err := svc.ReadTxnGet(context.Background(), &api.ReadTxnGetRequest{
		Handle: beginResp.Handle,
		Key:    []byte("foo"),
	})
	assert.NoError(t, err)
	assert.True(t, getResp.Found)
	assert.Equal(t, []byte("bar"), getResp.Value)

	_, err = svc.ReadTxnGet(context.Background(), &api.ReadTxnGetRequest{
		Handle: beginResp.Handle,
		Key:    []byte("missing"),
	})
	assert.NoError(t, err)

	_, err = svc.EndReadTxn(context.Background(), &api.EndReadTxnRequest{Handle: beginResp.Handle})
	assert.NoError(t, err)

	_, err = svc.EndReadTxn(context.Background(), &api.EndReadTxnRequest{Handle: beginResp.Handle})
	assert.Error(t, err)
}

func TestAdminService_JoinMembers(t *testing.T) {
	cl := newFakeCluster()
	adm := NewAdminService(cl)

	_, err := adm.Join(context.Background(), &api.JoinRequest{NodeId: 1, Address: "127.0.0.1:9001"})
	assert.Nil(t, err)

	resp, err := adm.Members(context.Background(), &api.MembersRequest{})
	assert.Nil(t, err)
	assert.Len(t, resp.Members, 1)
	assert.Equal(t, uint64(1), resp.Members[0].NodeId)

	_, err = adm.Leave(context.Background(), &api.LeaveRequest{NodeId: 1})
	assert.Nil(t, err)

	resp, err = adm.Members(context.Background(), &api.MembersRequest{})
	assert.Nil(t, err)
	assert.Len(t, resp.Members, 0)
}

func TestAdminService_TriggerMerge(t *testing.T) {
	cl := newFakeCluster()
	adm := NewAdminService(cl)

	_, err := adm.TriggerMerge(context.Background(), &api.TriggerMergeRequest{Force: true})
	assert.NoError(t, err)
	assert.Equal(t, 1, cl.mergeCount)

	_, err = adm.TriggerMerge(context.Background(), &api.TriggerMergeRequest{Force: false})
	assert.NoError(t, err)
	assert.Equal(t, 1, cl.mergeCount)
}

func TestAdminService_TriggerSnapshot(t *testing.T) {
	cl := newFakeCluster()
	adm := NewAdminService(cl)

	_, err := adm.TriggerSnapshot(context.Background(), &api.TriggerSnapshotRequest{Force: true})
	assert.NoError(t, err)
}

func TestAdminService_SnapshotStatus(t *testing.T) {
	cl := newFakeCluster()
	adm := NewAdminService(cl)
	resp, err := adm.SnapshotStatus(context.Background(), &api.SnapshotStatusRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}
