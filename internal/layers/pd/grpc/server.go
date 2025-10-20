package pdgrpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pd "nyxdb/internal/layers/pd"
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

func (s *Server) ListStores(ctx context.Context, req *api.ListStoresRequest) (*api.ListStoresResponse, error) {
	stores := s.service.Stores()
	resp := &api.ListStoresResponse{
		Stores: make([]*api.StoreHeartbeatProto, 0, len(stores)),
	}
	for _, st := range stores {
		resp.Stores = append(resp.Stores, pd.StoreHeartbeatToProto(st))
	}
	return resp, nil
}

func (s *Server) GetRegionByKey(ctx context.Context, req *api.GetRegionByKeyRequest) (*api.GetRegionByKeyResponse, error) {
	key := req.GetKey()
	if len(key) == 0 {
		return nil, status.Error(codes.InvalidArgument, "key is empty")
	}

	snapshot, ok := s.service.RegionByKey(key)
	if !ok {
		return nil, status.Error(codes.NotFound, "region not found")
	}

	return &api.GetRegionByKeyResponse{
		Region:   pd.RegionToProto(snapshot.Region),
		Replicas: regionHeartbeatsToProto(snapshot.Peers),
	}, nil
}

func (s *Server) GetRegion(ctx context.Context, req *api.GetRegionRequest) (*api.GetRegionResponse, error) {
	snapshot, ok := s.service.RegionSnapshot(req.GetRegionId())
	if !ok {
		return nil, status.Error(codes.NotFound, "region not found")
	}
	return &api.GetRegionResponse{Snapshot: snapshotToProto(snapshot)}, nil
}

func (s *Server) ListRegions(ctx context.Context, req *api.ListRegionsRequest) (*api.ListRegionsResponse, error) {
	snapshots := s.service.RegionsSnapshot()
	resp := &api.ListRegionsResponse{
		Snapshots: make([]*api.RegionSnapshot, 0, len(snapshots)),
	}
	for _, snap := range snapshots {
		resp.Snapshots = append(resp.Snapshots, snapshotToProto(snap))
	}
	return resp, nil
}

func Register(server *grpc.Server, service *pd.Service) {
	api.RegisterPDServer(server, NewServer(service))
}

func snapshotToProto(snapshot pd.RegionSnapshot) *api.RegionSnapshot {
	return &api.RegionSnapshot{
		Region:   pd.RegionToProto(snapshot.Region),
		Replicas: regionHeartbeatsToProto(snapshot.Peers),
	}
}

func regionHeartbeatsToProto(peers []pd.RegionHeartbeat) []*api.RegionReplicaDescriptor {
	replicas := make([]*api.RegionReplicaDescriptor, 0, len(peers))
	for _, peer := range peers {
		replicas = append(replicas, &api.RegionReplicaDescriptor{
			RegionId:     uint64(peer.Region.ID),
			StoreId:      peer.StoreID,
			PeerId:       peer.PeerID,
			Role:         pd.PeerRoleToProto(peer.Role),
			AppliedIndex: peer.AppliedIndex,
			Region:       pd.RegionToProto(peer.Region),
		})
	}
	return replicas
}
