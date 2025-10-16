package grpcserver

import (
	"context"
	"testing"

	db "nyxdb/internal/engine"
	api "nyxdb/pkg/api"

	"github.com/stretchr/testify/assert"
)

type fakeCluster struct {
	store   map[string][]byte
	members map[uint64]string
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{store: make(map[string][]byte), members: make(map[uint64]string)}
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
