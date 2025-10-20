package pdgrpc

import (
	"context"
	"errors"

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

func (s *Server) RegisterRegion(ctx context.Context, req *api.RegisterRegionRequest) (*api.RegisterRegionResponse, error) {
	region, err := pd.ProtoToRegionMetadata(req.GetMetadata())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse region metadata: %v", err)
	}
	registered, err := s.service.RegisterRegion(region)
	if err != nil {
		switch {
		case errors.Is(err, pd.ErrRegionExists):
			return nil, status.Error(codes.AlreadyExists, err.Error())
		default:
			return nil, status.Errorf(codes.Internal, "register region: %v", err)
		}
	}
	return &api.RegisterRegionResponse{
		Metadata: pd.RegionMetadataToProto(registered),
	}, nil
}

func (s *Server) UpdateRegion(ctx context.Context, req *api.UpdateRegionRequest) (*api.UpdateRegionResponse, error) {
	region, err := pd.ProtoToRegionMetadata(req.GetMetadata())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse region metadata: %v", err)
	}
	updated, err := s.service.UpdateRegion(region)
	if err != nil {
		switch {
		case errors.Is(err, pd.ErrRegionNotFound):
			return nil, status.Error(codes.NotFound, err.Error())
		default:
			return nil, status.Errorf(codes.Internal, "update region: %v", err)
		}
	}
	return &api.UpdateRegionResponse{
		Metadata: pd.RegionMetadataToProto(updated),
	}, nil
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
		Replicas: pd.RegionHeartbeatsToProto(snapshot.Peers),
	}, nil
}

func (s *Server) GetRegion(ctx context.Context, req *api.GetRegionRequest) (*api.GetRegionResponse, error) {
	snapshot, ok := s.service.RegionSnapshot(req.GetRegionId())
	if !ok {
		return nil, status.Error(codes.NotFound, "region not found")
	}
	return &api.GetRegionResponse{Snapshot: pd.RegionSnapshotToProto(snapshot)}, nil
}

func (s *Server) ListRegions(ctx context.Context, req *api.ListRegionsRequest) (*api.ListRegionsResponse, error) {
	snapshots := s.service.RegionsSnapshot()
	resp := &api.ListRegionsResponse{
		Snapshots: make([]*api.RegionSnapshot, 0, len(snapshots)),
	}
	for _, snap := range snapshots {
		resp.Snapshots = append(resp.Snapshots, pd.RegionSnapshotToProto(snap))
	}
	return resp, nil
}

func (s *Server) GetRegionsByStore(ctx context.Context, req *api.GetRegionsByStoreRequest) (*api.GetRegionsByStoreResponse, error) {
	snapshots, err := s.service.RegionsByStore(req.GetStoreId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get regions by store: %v", err)
	}
	resp := &api.GetRegionsByStoreResponse{
		Snapshots: make([]*api.RegionSnapshot, 0, len(snapshots)),
	}
	for _, snap := range snapshots {
		resp.Snapshots = append(resp.Snapshots, pd.RegionSnapshotToProto(snap))
	}
	return resp, nil
}

func Register(server *grpc.Server, service *pd.Service) {
	api.RegisterPDServer(server, NewServer(service))
}
