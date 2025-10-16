package grpcserver

import (
    "context"
    "fmt"

    api "nyxdb/pkg/api"
    "google.golang.org/grpc"
)

type KVServer struct {
    api.UnimplementedKVServer
}

type AdminServer struct {
    api.UnimplementedAdminServer
}

func (s *KVServer) Put(ctx context.Context, req *api.PutRequest) (*api.PutResponse, error) {
    return nil, fmt.Errorf("not implemented")
}

func RegisterKVAdminServers(s *grpc.Server) {}
