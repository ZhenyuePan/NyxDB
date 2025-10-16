package api

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
)

// --- Raft transport placeholders ---

type RaftMessage struct {
	To      uint64
	Message []byte
}

type RaftAck struct{}

type RaftTransport_SendClient interface {
	Send(*RaftMessage) error
	CloseSend() error
}

type RaftTransport_SendServer interface {
	Recv() (*RaftMessage, error)
	Context() context.Context
}

type RaftTransportClient interface {
	Send(ctx context.Context) (RaftTransport_SendClient, error)
}

type RaftTransportServer interface {
	Send(RaftTransport_SendServer) error
}

type UnimplementedRaftTransportServer struct{}

func (UnimplementedRaftTransportServer) Send(RaftTransport_SendServer) error { return nil }

func RegisterRaftTransportServer(*grpc.Server, RaftTransportServer) {}

// --- KV service placeholders ---

type PutRequest struct {
	Key   []byte
	Value []byte
}

type PutResponse struct{}

type GetRequest struct {
	Key []byte
}

type GetResponse struct {
	Value []byte
	Found bool
}

type DeleteRequest struct {
	Key []byte
}

type DeleteResponse struct{}

type BeginReadTxnRequest struct{}

type BeginReadTxnResponse struct {
	ReadTs uint64
	Handle []byte
}

type ReadTxnGetRequest struct {
	Handle []byte
	Key    []byte
}

type ReadTxnGetResponse struct {
	Value []byte
	Found bool
}

type EndReadTxnRequest struct {
	Handle []byte
}

type EndReadTxnResponse struct{}

// --- Admin service placeholders ---

type JoinRequest struct {
	NodeId  uint64
	Address string
}

type JoinResponse struct{}

type LeaveRequest struct {
	NodeId uint64
}

type LeaveResponse struct{}

type Member struct {
	NodeId  uint64
	Address string
}

type MembersRequest struct{}

type MembersResponse struct {
	Members []*Member
}

type TriggerMergeRequest struct {
	Force bool
}

type TriggerMergeResponse struct{}

// --- Interfaces ---

type KVServer interface {
	Put(context.Context, *PutRequest) (*PutResponse, error)
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Delete(context.Context, *DeleteRequest) (*DeleteResponse, error)
	BeginReadTxn(context.Context, *BeginReadTxnRequest) (*BeginReadTxnResponse, error)
	ReadTxnGet(context.Context, *ReadTxnGetRequest) (*ReadTxnGetResponse, error)
	EndReadTxn(context.Context, *EndReadTxnRequest) (*EndReadTxnResponse, error)
}

type AdminServer interface {
	Join(context.Context, *JoinRequest) (*JoinResponse, error)
	Leave(context.Context, *LeaveRequest) (*LeaveResponse, error)
	Members(context.Context, *MembersRequest) (*MembersResponse, error)
	TriggerMerge(context.Context, *TriggerMergeRequest) (*TriggerMergeResponse, error)
}

// Unimplemented helpers
type UnimplementedKVServer struct{}

func (UnimplementedKVServer) Put(context.Context, *PutRequest) (*PutResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
func (UnimplementedKVServer) Get(context.Context, *GetRequest) (*GetResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
func (UnimplementedKVServer) Delete(context.Context, *DeleteRequest) (*DeleteResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
func (UnimplementedKVServer) BeginReadTxn(context.Context, *BeginReadTxnRequest) (*BeginReadTxnResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
func (UnimplementedKVServer) ReadTxnGet(context.Context, *ReadTxnGetRequest) (*ReadTxnGetResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
func (UnimplementedKVServer) EndReadTxn(context.Context, *EndReadTxnRequest) (*EndReadTxnResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

type UnimplementedAdminServer struct{}

func (UnimplementedAdminServer) Join(context.Context, *JoinRequest) (*JoinResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
func (UnimplementedAdminServer) Leave(context.Context, *LeaveRequest) (*LeaveResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
func (UnimplementedAdminServer) Members(context.Context, *MembersRequest) (*MembersResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
func (UnimplementedAdminServer) TriggerMerge(context.Context, *TriggerMergeRequest) (*TriggerMergeResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// Service registration
type kvServerWrapper interface {
	KVServer
}

type adminServerWrapper interface {
	AdminServer
}

var kvServiceDesc = grpc.ServiceDesc{
	ServiceName: "nyxdb.api.KV",
	HandlerType: (*kvServerWrapper)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Put", Handler: _KV_Put_Handler},
		{MethodName: "Get", Handler: _KV_Get_Handler},
		{MethodName: "Delete", Handler: _KV_Delete_Handler},
		{MethodName: "BeginReadTxn", Handler: _KV_BeginReadTxn_Handler},
		{MethodName: "ReadTxnGet", Handler: _KV_ReadTxnGet_Handler},
		{MethodName: "EndReadTxn", Handler: _KV_EndReadTxn_Handler},
	},
}

func RegisterKVServer(s *grpc.Server, srv KVServer) {
	s.RegisterService(&kvServiceDesc, srv)
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

func RegisterAdminServer(s *grpc.Server, srv AdminServer) {
	s.RegisterService(&adminServiceDesc, srv)
}

// Handler helpers
func _KV_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).Put(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.KV/Put"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).Put(ctx, req.(*PutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.KV/Get"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_Delete_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).Delete(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.KV/Delete"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).Delete(ctx, req.(*DeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_BeginReadTxn_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(BeginReadTxnRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).BeginReadTxn(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.KV/BeginReadTxn"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).BeginReadTxn(ctx, req.(*BeginReadTxnRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_ReadTxnGet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReadTxnGetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).ReadTxnGet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.KV/ReadTxnGet"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).ReadTxnGet(ctx, req.(*ReadTxnGetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _KV_EndReadTxn_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(EndReadTxnRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KVServer).EndReadTxn(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.KV/EndReadTxn"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KVServer).EndReadTxn(ctx, req.(*EndReadTxnRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Admin_Join_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JoinRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServer).Join(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.Admin/Join"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServer).Join(ctx, req.(*JoinRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Admin_Leave_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LeaveRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServer).Leave(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.Admin/Leave"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServer).Leave(ctx, req.(*LeaveRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Admin_Members_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MembersRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServer).Members(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.Admin/Members"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServer).Members(ctx, req.(*MembersRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Admin_TriggerMerge_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TriggerMergeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(AdminServer).TriggerMerge(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/nyxdb.api.Admin/TriggerMerge"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(AdminServer).TriggerMerge(ctx, req.(*TriggerMergeRequest))
	}
	return interceptor(ctx, in, info, handler)
}
