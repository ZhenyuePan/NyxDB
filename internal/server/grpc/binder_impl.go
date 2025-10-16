package grpcserver

import (
    "nyxdb/internal/cluster"
    rafttransport "nyxdb/internal/raft"

    "google.golang.org/grpc"
)

// DefaultBinder registers built-in services (Raft transport, later KV/Admin).
type DefaultBinder struct{}

func (DefaultBinder) Register(s *grpc.Server, cl *cluster.Cluster) {
    if cl == nil {
        return
    }
    if node := cl.RaftNode(); node != nil {
        rafttransport.RegisterGRPCTransportServer(s, node)
    }
    registerKVAdminServers(s, cl)
}
