package grpcserver

import (
	"context"
	"fmt"

	"nyxdb/internal/cluster"
	db "nyxdb/internal/engine"
	api "nyxdb/pkg/api"

	"google.golang.org/grpc"
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
		if err == db.ErrKeyNotFound {
			return &api.GetResponse{Found: false}, nil
		}
		return nil, err
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

func registerKVAdminServers(s *grpc.Server, cl *cluster.Cluster) {
	if cl == nil {
		return
	}
	registerKVServer(s, NewKVService(cl))
	registerAdminServer(s, NewAdminService(cl))
}

type kvServerWrapper interface {
	api.KVServer
}

var kvServiceDesc = grpc.ServiceDesc{
	ServiceName: "nyxdb.api.KV",
	HandlerType: (*kvServerWrapper)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Put", Handler: _KV_Put_Handler},
		{MethodName: "Get", Handler: _KV_Get_Handler},
		{MethodName: "Delete", Handler: _KV_Delete_Handler},
	},
}

func registerKVServer(s *grpc.Server, srv *KVService) {
	s.RegisterService(&kvServiceDesc, srv)
}

func _KV_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(api.PutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(*KVService).Put(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.KV/Put"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(*KVService).Put(ctx, req.(*api.PutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(api.GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(*KVService).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.KV/Get"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(*KVService).Get(ctx, req.(*api.GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(api.DeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(*KVService).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.KV/Delete"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(*KVService).Delete(ctx, req.(*api.DeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

type adminServerWrapper interface {
	api.AdminServer
}

var adminServiceDesc = grpc.ServiceDesc{
	ServiceName: "nyxdb.api.Admin",
	HandlerType: (*adminServerWrapper)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Join", Handler: _Admin_Join_Handler},
		{MethodName: "Leave", Handler: _Admin_Leave_Handler},
		{MethodName: "Members", Handler: _Admin_Members_Handler},
		{MethodName: "TriggerMerge", Handler: _Admin_TriggerMerge_Handler},
	},
}

func registerAdminServer(s *grpc.Server, srv *AdminService) {
	s.RegisterService(&adminServiceDesc, srv)
}

func _Admin_Join_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(api.JoinRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(*AdminService).Join(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.Admin/Join"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(*AdminService).Join(ctx, req.(*api.JoinRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Admin_Leave_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(api.LeaveRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(*AdminService).Leave(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.Admin/Leave"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(*AdminService).Leave(ctx, req.(*api.LeaveRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Admin_Members_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(api.MembersRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(*AdminService).Members(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.Admin/Members"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(*AdminService).Members(ctx, req.(*api.MembersRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Admin_TriggerMerge_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(api.TriggerMergeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(*AdminService).TriggerMerge(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.Admin/TriggerMerge"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(*AdminService).TriggerMerge(ctx, req.(*api.TriggerMergeRequest))
	}
	return interceptor(ctx, in, info, handler)
}
