package pdgrpc

import (
	"context"
	"time"

	"google.golang.org/grpc"

	pd "nyxdb/internal/pd"
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

func (c *Client) Close() error {
	return c.conn.Close()
}
