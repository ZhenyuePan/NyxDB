package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	pd "nyxdb/internal/pd"
	pdgrpc "nyxdb/internal/pd/grpc"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:18080", "gRPC listen address")
	dataDir := flag.String("data", "/tmp/nyxdb-pd", "PD data directory")
	flag.Parse()

	service, err := pd.NewPersistentService(*dataDir)
	if err != nil {
		log.Fatalf("failed to create PD service: %v", err)
	}
	defer service.Close()

	grpcServer := grpc.NewServer()
	pdgrpc.Register(grpcServer, service)

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("PD server listening on %s", *addr)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	grpcServer.GracefulStop()
	_ = service.Close()
	log.Println("PD server stopped")
}
