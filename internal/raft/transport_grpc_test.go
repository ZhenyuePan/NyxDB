package raft

import (
    "context"
    "net"
    "testing"
    "time"

    "go.etcd.io/etcd/raft/v3/raftpb"
    "google.golang.org/grpc"
)

type stepRecorder struct {
    ch chan raftpb.Message
}

func newStepRecorder() *stepRecorder {
    return &stepRecorder{ch: make(chan raftpb.Message, 1)}
}

func (s *stepRecorder) Step(ctx context.Context, msg raftpb.Message) error {
    select {
    case s.ch <- msg:
    default:
    }
    return nil
}

func TestGRPCTransport_Send(t *testing.T) {
    recorder := newStepRecorder()
    server := grpc.NewServer(grpc.ForceServerCodec(jsonCodec{}))
    RegisterGRPCTransportServer(server, NewGRPCTransportServer(recorder))

    lis, err := net.Listen("tcp", "127.0.0.1:0")
    if err != nil {
        t.Fatalf("listen failed: %v", err)
    }
    defer lis.Close()

    go func() {
        _ = server.Serve(lis)
    }()
    defer server.GracefulStop()

    transport := NewGRPCTransport(1, DefaultDialer{})
    if err := transport.AddMember(2, []string{lis.Addr().String()}); err != nil {
        t.Fatalf("add member failed: %v", err)
    }

    msg := raftpb.Message{From: 1, To: 2, Type: raftpb.MsgApp}
    if err := transport.Send(2, []raftpb.Message{msg}); err != nil {
        t.Fatalf("send failed: %v", err)
    }
    _ = transport.RemoveMember(2)

    select {
    case received := <-recorder.ch:
        if received.Type != raftpb.MsgApp || received.From != 1 || received.To != 2 {
            t.Fatalf("unexpected message: %+v", received)
        }
    case <-time.After(2 * time.Second):
        t.Fatalf("timeout waiting for message")
    }
}
