package grpcserver

import (
	"context"
	"errors"
	"fmt"

	"nyxdb/internal/cluster"
	db "nyxdb/internal/engine"
	api "nyxdb/pkg/api"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KVService adapts Cluster operations to gRPC KV service.
type kvCluster interface {
	Put([]byte, []byte) error
	Get([]byte) ([]byte, error)
	GetLinearizable(context.Context, []byte) ([]byte, error)
	Delete([]byte) error
	AddMember(uint64, string) error
	RemoveMember(uint64) error
	Members() map[uint64]string
	BeginReadTxn() ([]byte, uint64, error)
	ReadTxnGet([]byte, []byte) ([]byte, bool, error)
	EndReadTxn([]byte) error
	TriggerMerge(force bool) error
	LeaderAddress() string
	TriggerSnapshot(force bool) error
	SnapshotStatus() cluster.SnapshotStatus
}

type KVService struct {
	api.UnimplementedKVServer
	cluster kvCluster
}

func NewKVService(cl kvCluster) *KVService {
	return &KVService{cluster: cl}
}

func (s *KVService) Put(ctx context.Context, req *api.PutRequest) (*api.PutResponse, error) {
	if s.cluster == nil {
		return nil, fmt.Errorf("cluster not available")
	}
	if err := s.cluster.Put(req.Key, req.Value); err != nil {
		return nil, err
	}
	return &api.PutResponse{}, nil
}

func (s *KVService) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	if s.cluster == nil {
		return nil, fmt.Errorf("cluster not available")
	}
	val, err := s.cluster.GetLinearizable(ctx, req.Key)
	if err != nil {
		switch {
		case errors.Is(err, db.ErrKeyNotFound):
			return &api.GetResponse{Found: false}, nil
		case errors.Is(err, cluster.ErrNotLeader):
			leader := s.cluster.LeaderAddress()
			if leader != "" {
				return nil, status.Errorf(codes.FailedPrecondition, "not leader; leader=%s", leader)
			}
			return nil, status.Error(codes.FailedPrecondition, "not leader")
		default:
			return nil, err
		}
	}
	return &api.GetResponse{Value: val, Found: true}, nil
}

func (s *KVService) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	if s.cluster == nil {
		return nil, fmt.Errorf("cluster not available")
	}
	if err := s.cluster.Delete(req.Key); err != nil {
		return nil, err
	}
	return &api.DeleteResponse{}, nil
}

func (s *KVService) BeginReadTxn(context.Context, *api.BeginReadTxnRequest) (*api.BeginReadTxnResponse, error) {
	if s.cluster == nil {
		return nil, fmt.Errorf("cluster not available")
	}
	handle, readTs, err := s.cluster.BeginReadTxn()
	if err != nil {
		return nil, err
	}
	return &api.BeginReadTxnResponse{
		ReadTs: readTs,
		Handle: handle,
	}, nil
}

func (s *KVService) ReadTxnGet(ctx context.Context, req *api.ReadTxnGetRequest) (*api.ReadTxnGetResponse, error) {
	if s.cluster == nil {
		return nil, fmt.Errorf("cluster not available")
	}
	if len(req.Handle) == 0 {
		return nil, fmt.Errorf("read transaction handle is empty")
	}
	value, found, err := s.cluster.ReadTxnGet(req.Handle, req.Key)
	if err != nil {
		return nil, err
	}
	resp := &api.ReadTxnGetResponse{Found: found}
	if found {
		resp.Value = value
	}
	return resp, nil
}

func (s *KVService) EndReadTxn(ctx context.Context, req *api.EndReadTxnRequest) (*api.EndReadTxnResponse, error) {
	if s.cluster == nil {
		return nil, fmt.Errorf("cluster not available")
	}
	if len(req.Handle) == 0 {
		return nil, fmt.Errorf("read transaction handle is empty")
	}
	if err := s.cluster.EndReadTxn(req.Handle); err != nil {
		return nil, err
	}
	return &api.EndReadTxnResponse{}, nil
}

// AdminService exposes cluster administration commands.
type AdminService struct {
	api.UnimplementedAdminServer
	cluster kvCluster
}

func NewAdminService(cl kvCluster) *AdminService {
	return &AdminService{cluster: cl}
}

func (s *AdminService) Join(ctx context.Context, req *api.JoinRequest) (*api.JoinResponse, error) {
	if s.cluster == nil {
		return nil, fmt.Errorf("cluster not available")
	}
	if err := s.cluster.AddMember(req.NodeId, req.Address); err != nil {
		return nil, err
	}
	return &api.JoinResponse{}, nil
}

func (s *AdminService) Leave(ctx context.Context, req *api.LeaveRequest) (*api.LeaveResponse, error) {
	if s.cluster == nil {
		return nil, fmt.Errorf("cluster not available")
	}
	if err := s.cluster.RemoveMember(req.NodeId); err != nil {
		return nil, err
	}
	return &api.LeaveResponse{}, nil
}

func (s *AdminService) Members(ctx context.Context, req *api.MembersRequest) (*api.MembersResponse, error) {
	if s.cluster == nil {
		return nil, fmt.Errorf("cluster not available")
	}
	members := s.cluster.Members()
	resp := &api.MembersResponse{}
	for id, addr := range members {
		resp.Members = append(resp.Members, &api.Member{NodeId: id, Address: addr})
	}
	return resp, nil
}

func (s *AdminService) TriggerMerge(ctx context.Context, req *api.TriggerMergeRequest) (*api.TriggerMergeResponse, error) {
	if s.cluster == nil {
		return nil, fmt.Errorf("cluster not available")
	}
	if err := s.cluster.TriggerMerge(req.Force); err != nil {
		return nil, err
	}
	return &api.TriggerMergeResponse{}, nil
}

func (s *AdminService) TriggerSnapshot(ctx context.Context, req *api.TriggerSnapshotRequest) (*api.TriggerSnapshotResponse, error) {
	if s.cluster == nil {
		return nil, fmt.Errorf("cluster not available")
	}
	if err := s.cluster.TriggerSnapshot(req.Force); err != nil {
		switch {
		case errors.Is(err, cluster.ErrNotLeader):
			leader := s.cluster.LeaderAddress()
			if leader != "" {
				return nil, status.Errorf(codes.FailedPrecondition, "not leader; leader=%s", leader)
			}
			return nil, status.Error(codes.FailedPrecondition, "not leader")
		case errors.Is(err, cluster.ErrSnapshotInProgress):
			return nil, status.Error(codes.Aborted, "snapshot in progress")
		case errors.Is(err, cluster.ErrSnapshotNotNeeded):
			return nil, status.Error(codes.FailedPrecondition, "snapshot not needed")
		default:
			return nil, err
		}
	}
	return &api.TriggerSnapshotResponse{}, nil
}

func (s *AdminService) SnapshotStatus(ctx context.Context, req *api.SnapshotStatusRequest) (*api.SnapshotStatusResponse, error) {
	if s.cluster == nil {
		return nil, fmt.Errorf("cluster not available")
	}
	st := s.cluster.SnapshotStatus()
	lastSnapshotUnix := st.LastSnapshotTime.Unix()
	if st.LastSnapshotTime.IsZero() {
		lastSnapshotUnix = 0
	}
	inProgressUnix := st.InProgressSince.Unix()
	if st.InProgressSince.IsZero() {
		inProgressUnix = 0
	}
	var durationMs int64
	if st.LastSnapshotDuration > 0 {
		durationMs = st.LastSnapshotDuration.Milliseconds()
	}
	resp := &api.SnapshotStatusResponse{
		InProgress:             st.InProgress,
		LastSnapshotIndex:      st.LastSnapshotIndex,
		EntriesSince:           st.EntriesSince,
		LastSnapshotTimeUnix:   lastSnapshotUnix,
		InProgressSinceUnix:    inProgressUnix,
		Leader:                 st.Leader,
		AppliedIndex:           st.AppliedIndex,
		LastRaftIndex:          st.LastRaftIndex,
		LastSnapshotDurationMs: durationMs,
		LastSnapshotSizeBytes:  st.LastSnapshotSizeBytes,
	}
	return resp, nil
}

func registerKVAdminServers(s *grpc.Server, cl *cluster.Cluster) {
	if cl == nil {
		return
	}
	api.RegisterKVServer(s, NewKVService(cl))
	api.RegisterAdminServer(s, NewAdminService(cl))
}
