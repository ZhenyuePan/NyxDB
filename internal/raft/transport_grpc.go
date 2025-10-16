package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

const (
	transportServiceName  = "nyxdb.raft.Transport"
	transportSendFullName = "/nyxdb.raft.Transport/Send"
)

func init() {
	encoding.RegisterCodec(jsonCodec{})
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v interface{}) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v interface{}) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                               { return "json" }

// RaftMessage is the payload exchanged over gRPC transport.
type RaftMessage struct {
	To      uint64 `json:"to"`
	Message []byte `json:"message"`
}

// RaftAck is returned when a stream finishes successfully.
type RaftAck struct{}

// GRPCDialer abstracts dialing so tests can inject custom behaviour.
type GRPCDialer interface {
	Dial(ctx context.Context, target string) (*grpc.ClientConn, error)
}

// DefaultDialer uses grpc.DialContext with insecure credentials.
type DefaultDialer struct{}

func (DefaultDialer) Dial(ctx context.Context, target string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})))
}

type clientStream struct {
	conn   *grpc.ClientConn
	stream grpc.ClientStream
}

// GRPCTransport implements Transport using gRPC streaming.
type GRPCTransport struct {
	mu        sync.RWMutex
	nodeID    uint64
	addresses map[uint64]string
	streams   map[uint64]*clientStream
	dialer    GRPCDialer
}

// NewGRPCTransport creates a gRPC transport for the given node.
func NewGRPCTransport(nodeID uint64, dialer GRPCDialer) *GRPCTransport {
	if dialer == nil {
		dialer = DefaultDialer{}
	}
	return &GRPCTransport{
		nodeID:    nodeID,
		addresses: make(map[uint64]string),
		streams:   make(map[uint64]*clientStream),
		dialer:    dialer,
	}
}

func (t *GRPCTransport) AddMember(id uint64, peerURLs []string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(peerURLs) == 0 {
		return fmt.Errorf("no address provided for member %d", id)
	}
	t.addresses[id] = peerURLs[0]
	return nil
}

func (t *GRPCTransport) RemoveMember(id uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.addresses, id)
	if cs, ok := t.streams[id]; ok {
		_ = cs.stream.CloseSend()
		_ = cs.conn.Close()
		delete(t.streams, id)
	}
	return nil
}

func (t *GRPCTransport) Send(to uint64, messages []raftpb.Message) error {
	if len(messages) == 0 {
		return nil
	}
	cs, err := t.ensureStream(to)
	if err != nil {
		return err
	}
	for _, msg := range messages {
		data, err := msg.Marshal()
		if err != nil {
			return err
		}
		if err := cs.stream.SendMsg(&RaftMessage{To: to, Message: data}); err != nil {
			t.closeStream(to)
			return err
		}
	}
	return nil
}

func (t *GRPCTransport) SendSnapshot(to uint64, snapshot raftpb.Snapshot) error {
	msg := raftpb.Message{To: to, Type: raftpb.MsgSnap, Snapshot: snapshot}
	return t.Send(to, []raftpb.Message{msg})
}

func (t *GRPCTransport) ensureStream(to uint64) (*clientStream, error) {
	t.mu.RLock()
	cs, ok := t.streams[to]
	addr := t.addresses[to]
	t.mu.RUnlock()
	if ok {
		return cs, nil
	}
	if addr == "" {
		return nil, fmt.Errorf("unknown address for member %d", to)
	}
	conn, err := t.dialer.Dial(context.Background(), addr)
	if err != nil {
		return nil, err
	}
	stream, err := conn.NewStream(context.Background(), &raftTransportServiceDesc.Streams[0], transportSendFullName, grpc.CallContentSubtype(jsonCodec{}.Name()))
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	cs = &clientStream{conn: conn, stream: stream}
	t.mu.Lock()
	t.streams[to] = cs
	t.mu.Unlock()
	return cs, nil
}

func (t *GRPCTransport) closeStream(to uint64) {
	t.mu.Lock()
	if cs, ok := t.streams[to]; ok {
		_ = cs.stream.CloseSend()
		_ = cs.conn.Close()
		delete(t.streams, to)
	}
	t.mu.Unlock()
}

// GRPCTransportServer receives raft messages via gRPC.
type GRPCTransportServer struct {
	node raftStepNode
}

type raftStepNode interface {
	Step(ctx context.Context, msg raftpb.Message) error
}

// NewGRPCTransportServer constructs a server bound to a raft node.
func NewGRPCTransportServer(node raftStepNode) *GRPCTransportServer {
	return &GRPCTransportServer{node: node}
}

func (s *GRPCTransportServer) Send(stream grpc.ServerStream) error {
	for {
		msg := new(RaftMessage)
		if err := stream.RecvMsg(msg); err != nil {
			if errors.Is(err, io.EOF) {
				return stream.SendMsg(&RaftAck{})
			}
			return err
		}
		var m raftpb.Message
		if err := m.Unmarshal(msg.Message); err != nil {
			return err
		}
		if err := s.node.Step(stream.Context(), m); err != nil {
			return err
		}
	}
}

func RegisterGRPCTransportServer(s *grpc.Server, srv *GRPCTransportServer) {
	s.RegisterService(&raftTransportServiceDesc, srv)
}

var raftTransportServiceDesc = grpc.ServiceDesc{
	ServiceName: transportServiceName,
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Send",
			Handler:       _RaftTransport_Send_Handler,
			ClientStreams: true,
		},
	},
}

func _RaftTransport_Send_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(*GRPCTransportServer).Send(stream)
}
