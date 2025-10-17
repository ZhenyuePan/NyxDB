package grpcserver

import (
	"context"
	"fmt"
	"net"

	"nyxdb/internal/cluster"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
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
	binder  ServiceBinder
	health  *health.Server
}

// New constructs a Server.
func New(cfg Config, cl *cluster.Cluster, binder ServiceBinder) *Server {
	if binder == nil {
		binder = &noopBinder{}
	}
	s := &Server{
		cfg:     cfg,
		cluster: cl,
		srv:     grpc.NewServer(),
		binder:  binder,
		health:  health.NewServer(),
	}
	binder.Register(s.srv, cl)
	healthpb.RegisterHealthServer(s.srv, s.health)
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	return s
}

// NewDefault creates a server using the default binder.
func NewDefault(cfg Config, cl *cluster.Cluster) *Server {
	return New(cfg, cl, DefaultBinder{})
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
	s.setServing(true)
	go func() {
		<-ctx.Done()
		s.setServing(false)
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
		s.setServing(false)
		s.srv.GracefulStop()
	}
}

func (s *Server) setServing(serving bool) {
	if s.health == nil {
		return
	}
	status := healthpb.HealthCheckResponse_NOT_SERVING
	if serving {
		status = healthpb.HealthCheckResponse_SERVING
	}
	s.health.SetServingStatus("", status)
}

// ServiceBinder allows injection of generated gRPC services without tying this package
// to the generated code (facilitates optional module generation).
type ServiceBinder interface {
	Register(*grpc.Server, *cluster.Cluster)
}

type noopBinder struct{}

func (noopBinder) Register(*grpc.Server, *cluster.Cluster) {}
