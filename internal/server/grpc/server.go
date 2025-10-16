package grpcserver

import (
	"context"
	"fmt"
	"net"

	"nyxdb/internal/cluster"

	"google.golang.org/grpc"
)

// Config holds gRPC server configuration.
type Config struct {
	Address string
}

// Server wraps the gRPC services that expose KV/Admin APIs.
type Server struct {
	cfg     Config
	cluster *cluster.Cluster
	srv     *grpc.Server
}

// New constructs a Server.
func New(cfg Config, cl *cluster.Cluster) *Server {
	return &Server{
		cfg:     cfg,
		cluster: cl,
		srv:     grpc.NewServer(),
	}
}

// Start begins listening on the configured address. Currently services are not registered;
// this is a placeholder for future implementation.
func (s *Server) Start(ctx context.Context) error {
	if s.cfg.Address == "" {
		return fmt.Errorf("grpc address is empty")
	}
	lis, err := net.Listen("tcp", s.cfg.Address)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		s.srv.GracefulStop()
		_ = lis.Close()
	}()
	go func() {
		_ = s.srv.Serve(lis)
	}()
	return nil
}

// Stop shuts down the server.
func (s *Server) Stop() {
	if s.srv != nil {
		s.srv.GracefulStop()
	}
}
