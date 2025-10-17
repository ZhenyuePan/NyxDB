package pdgrpc

import (
	"bytes"
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

	stores := s.service.Stores()
	var (
		targetRegion *api.RegionDescriptor
		targetID     uint64
		replicas     []*api.RegionReplicaDescriptor
	)

	for _, st := range stores {
		storeProto := pd.StoreHeartbeatToProto(st)
		for _, replica := range storeProto.GetRegions() {
			desc := replica.GetRegion()
			if targetRegion == nil {
				if desc == nil {
					continue
				}
				if !regionContainsKey(desc, key) {
					continue
				}
				targetRegion = cloneRegionDescriptor(desc)
				targetID = replica.GetRegionId()
			}
			if replica.GetRegionId() == targetID {
				replicas = append(replicas, cloneReplicaDescriptor(replica))
			}
		}
	}

	if targetRegion == nil {
		return nil, status.Error(codes.NotFound, "region not found")
	}

	return &api.GetRegionByKeyResponse{
		Region:   targetRegion,
		Replicas: replicas,
	}, nil
}

func Register(server *grpc.Server, service *pd.Service) {
	api.RegisterPDServer(server, NewServer(service))
}

func regionContainsKey(desc *api.RegionDescriptor, key []byte) bool {
	if desc == nil {
		return false
	}
	if len(desc.StartKey) > 0 && bytes.Compare(key, desc.StartKey) < 0 {
		return false
	}
	if len(desc.EndKey) > 0 && bytes.Compare(key, desc.EndKey) >= 0 {
		return false
	}
	return true
}

func cloneRegionDescriptor(desc *api.RegionDescriptor) *api.RegionDescriptor {
	if desc == nil {
		return nil
	}
	cp := *desc
	cp.StartKey = append([]byte(nil), desc.GetStartKey()...)
	cp.EndKey = append([]byte(nil), desc.GetEndKey()...)
	return &cp
}

func cloneReplicaDescriptor(replica *api.RegionReplicaDescriptor) *api.RegionReplicaDescriptor {
	if replica == nil {
		return nil
	}
	cp := *replica
	if replica.Region != nil {
		cp.Region = cloneRegionDescriptor(replica.Region)
	}
	return &cp
}
