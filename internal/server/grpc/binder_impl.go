package grpcserver

import (
	"nyxdb/internal/cluster"
	rafttransport "nyxdb/internal/layers/raft/transport"

	"google.golang.org/grpc"
)

// DefaultBinder registers built-in services (Raft transport, later KV/Admin).
type DefaultBinder struct{}

func (DefaultBinder) Register(s *grpc.Server, cl *cluster.Cluster) {
	if cl == nil {
		return
	}
	if router := cl.RaftRouter(); router != nil {
		rafttransport.RegisterGRPCTransportServer(s, router)
	}
	registerKVAdminServers(s, cl)
}
