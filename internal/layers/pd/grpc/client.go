package pdgrpc

import (
	"context"
	"time"

	"google.golang.org/grpc"

	pd "nyxdb/internal/layers/pd"
	regionpkg "nyxdb/internal/region"
	api "nyxdb/pkg/api"
)

// Client implements pd.Heartbeater over gRPC.
type Client struct {
	conn   *grpc.ClientConn
	client api.PDClient
}

func NewClient(ctx context.Context, target string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = append(opts, grpc.WithInsecure())
	}
	conn, err := grpc.DialContext(ctx, target, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, client: api.NewPDClient(conn)}, nil
}

func (c *Client) HandleHeartbeat(hb pd.StoreHeartbeat) pd.StoreHeartbeatResponse {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = c.client.StoreHeartbeat(ctx, &api.StoreHeartbeatRequest{Heartbeat: pd.StoreHeartbeatToProto(hb)})
	return pd.StoreHeartbeatResponse{}
}

func (c *Client) RegisterRegion(region regionpkg.Region) (regionpkg.Region, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.client.RegisterRegion(ctx, &api.RegisterRegionRequest{
		Metadata: pd.RegionMetadataToProto(region),
	})
	if err != nil {
		return regionpkg.Region{}, err
	}
	registered, err := pd.ProtoToRegionMetadata(resp.GetMetadata())
	if err != nil {
		return regionpkg.Region{}, err
	}
	return registered, nil
}

func (c *Client) UpdateRegion(region regionpkg.Region) (regionpkg.Region, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.client.UpdateRegion(ctx, &api.UpdateRegionRequest{
		Metadata: pd.RegionMetadataToProto(region),
	})
	if err != nil {
		return regionpkg.Region{}, err
	}
	updated, err := pd.ProtoToRegionMetadata(resp.GetMetadata())
	if err != nil {
		return regionpkg.Region{}, err
	}
	return updated, nil
}

func (c *Client) RegionsByStore(storeID uint64) ([]pd.RegionSnapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.client.GetRegionsByStore(ctx, &api.GetRegionsByStoreRequest{StoreId: storeID})
	if err != nil {
		return nil, err
	}
	protos := resp.GetSnapshots()
	snapshots := make([]pd.RegionSnapshot, 0, len(protos))
	for _, protoSnap := range protos {
		snapshot, err := pd.ProtoToRegionSnapshot(protoSnap)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}
