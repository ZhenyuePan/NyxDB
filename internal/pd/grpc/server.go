package pdgrpc

import (
	"context"

	"google.golang.org/grpc"

	pd "nyxdb/internal/pd"
	api "nyxdb/pkg/api"
)

// Server adapts pd.Service to the PD gRPC API.
type Server struct {
	api.UnimplementedPDServer
	service *pd.Service
}

func NewServer(service *pd.Service) *Server {
	return &Server{service: service}
}

func (s *Server) StoreHeartbeat(ctx context.Context, req *api.StoreHeartbeatRequest) (*api.StoreHeartbeatResponse, error) {
	heartbeat, err := pd.ProtoToStoreHeartbeat(req.GetHeartbeat())
	if err != nil {
		return nil, err
	}
	s.service.HandleHeartbeat(heartbeat)
	return &api.StoreHeartbeatResponse{}, nil
}

func Register(server *grpc.Server, service *pd.Service) {
	api.RegisterPDServer(server, NewServer(service))
}
