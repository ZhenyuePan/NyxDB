package config

import (
	db "nyxdb/internal/engine"
	grpcserver "nyxdb/internal/server/grpc"
)

type ServerConfig struct {
	NodeID  uint64        `yaml:"nodeID"`
	Engine  EngineConfig  `yaml:"engine"`
	Cluster ClusterConfig `yaml:"cluster"`
	GRPC    GRPCConfig    `yaml:"grpc"`
}

type EngineConfig struct {
	Dir               string `yaml:"dir"`
	EnableDiagnostics bool   `yaml:"enableDiagnostics"`
}

type ClusterConfig struct {
	ClusterMode      bool     `yaml:"clusterMode"`
	NodeAddress      string   `yaml:"nodeAddress"`
	ClusterAddresses []string `yaml:"clusterAddresses"`
}

type GRPCConfig struct {
	Address string `yaml:"address"`
}

func (c *ServerConfig) EngineOptions() db.Options {
	opts := db.DefaultOptions
	if c.Engine.Dir != "" {
		opts.DirPath = c.Engine.Dir
	}
	opts.EnableDiagnostics = c.Engine.EnableDiagnostics
	if c.Cluster.ClusterMode {
		nodeAddr := c.Cluster.NodeAddress
		if nodeAddr == "" && c.GRPC.Address != "" {
			nodeAddr = c.GRPC.Address
		}
		opts.ClusterConfig = &db.ClusterOptions{
			ClusterMode:      true,
			NodeAddress:      nodeAddr,
			RouterType:       db.DirectHash,
			ClusterAddresses: c.Cluster.ClusterAddresses,
		}
	} else {
		opts.ClusterConfig = nil
	}
	return opts
}

func (c *ServerConfig) GRPCConfig() grpcserver.Config {
	return grpcserver.Config{Address: c.GRPC.Address}
}
