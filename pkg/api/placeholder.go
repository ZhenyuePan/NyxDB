package api

import "context"

type RaftMessage struct {
	To      uint64
	Message []byte
}

type RaftAck struct{}

type RaftTransport_SendClient interface {
	Send(*RaftMessage) error
	CloseSend() error
}

type RaftTransport_SendServer interface {
	Recv() (*RaftMessage, error)
	Context() context.Context
}

type RaftTransportServer interface {
	Send(RaftTransport_SendServer) error
}

type UnimplementedRaftTransportServer struct{}

func (UnimplementedRaftTransportServer) Send(RaftTransport_SendServer) error { return nil }

func NewRaftTransportClient(conn interface{}) RaftTransportClient {
	return nil
}

type RaftTransportClient interface {
	Send(ctx context.Context) (RaftTransport_SendClient, error)
}
