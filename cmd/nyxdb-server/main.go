package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"nyxdb/internal/cluster"
	"nyxdb/internal/config"
	db "nyxdb/internal/engine"
	grpcserver "nyxdb/internal/server/grpc"
)

func main() {
	configPath := flag.String("config", "configs/server.example.yaml", "path to server config")
	flag.Parse()

	cfg, err := config.LoadServerConfig(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	engine, err := db.Open(cfg.EngineOptions())
	if err != nil {
		log.Fatalf("failed to open engine: %v", err)
	}

	cl, err := cluster.NewClusterWithTransport(cfg.NodeID, cfg.EngineOptions(), engine, nil)
	if err != nil {
		log.Fatalf("failed to create cluster: %v", err)
	}

	if err := cl.Start(); err != nil {
		log.Fatalf("failed to start cluster: %v", err)
	}

	grpcSrv := grpcserver.NewDefault(cfg.GRPCConfig(), cl)
	ctx, cancel := context.WithCancel(context.Background())
	if err := grpcSrv.Start(ctx); err != nil {
		log.Fatalf("failed to start grpc server: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	cancel()
	grpcSrv.Stop()
	if err := cl.Stop(); err != nil {
		log.Printf("cluster stop error: %v", err)
	}
	if err := engine.Close(); err != nil {
		log.Printf("engine close error: %v", err)
	}
}
