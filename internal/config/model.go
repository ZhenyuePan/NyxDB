package config

import (
	"time"

	db "nyxdb/internal/layers/engine"
	grpcserver "nyxdb/internal/server/grpc"
)

type ServerConfig struct {
	NodeID        uint64              `yaml:"nodeID"`
	Engine        EngineConfig        `yaml:"engine"`
	Cluster       ClusterConfig       `yaml:"cluster"`
	GRPC          GRPCConfig          `yaml:"grpc"`
	PD            PDConfig            `yaml:"pd"`
	Observability ObservabilityConfig `yaml:"observability"`
}

type EngineConfig struct {
	Dir               string `yaml:"dir"`
	EnableDiagnostics bool   `yaml:"enableDiagnostics"`
}

type ClusterConfig struct {
	ClusterMode              bool          `yaml:"clusterMode"`
	NodeAddress              string        `yaml:"nodeAddress"`
	ClusterAddresses         []string      `yaml:"clusterAddresses"`
	AutoSnapshot             bool          `yaml:"autoSnapshot"`
	SnapshotInterval         time.Duration `yaml:"snapshotInterval"`
	SnapshotThreshold        uint64        `yaml:"snapshotThreshold"`
	SnapshotCatchUpEntries   uint64        `yaml:"snapshotCatchUpEntries"`
	SnapshotDirSizeThreshold uint64        `yaml:"snapshotDirSizeThreshold"`
	SnapshotMaxDuration      time.Duration `yaml:"snapshotMaxDuration"`
	SnapshotMinInterval      time.Duration `yaml:"snapshotMinInterval"`
	SnapshotMaxAppliedLag    uint64        `yaml:"snapshotMaxAppliedLag"`
}

type ObservabilityConfig struct {
	MetricsAddress string        `yaml:"metricsAddress"`
	Tracing        TracingConfig `yaml:"tracing"`
}

type GRPCConfig struct {
	Address       string `yaml:"address"`
	EnableTracing bool   `yaml:"-"`
}

type TracingConfig struct {
	Endpoint    string  `yaml:"endpoint"`
	Insecure    bool    `yaml:"insecure"`
	ServiceName string  `yaml:"serviceName"`
	SampleRatio float64 `yaml:"sampleRatio"`
}

type PDConfig struct {
	Address           string        `yaml:"address"`
	HeartbeatInterval time.Duration `yaml:"heartbeatInterval"`
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
		interval := c.Cluster.SnapshotInterval
		if interval <= 0 {
			interval = 5 * time.Minute
		}
		threshold := c.Cluster.SnapshotThreshold
		if threshold == 0 {
			threshold = 1024
		}
		catchUp := c.Cluster.SnapshotCatchUpEntries
		if catchUp == 0 {
			catchUp = 64
		}
		maxDur := c.Cluster.SnapshotMaxDuration
		if maxDur <= 0 {
			maxDur = 2 * time.Minute
		}
		minInterval := c.Cluster.SnapshotMinInterval
		if minInterval <= 0 {
			minInterval = interval / 2
			if minInterval <= 0 {
				minInterval = time.Minute
			}
		}
		maxLag := c.Cluster.SnapshotMaxAppliedLag
		if maxLag == 0 && c.Cluster.SnapshotCatchUpEntries > 0 {
			maxLag = 2 * c.Cluster.SnapshotCatchUpEntries
		}
		opts.ClusterConfig = &db.ClusterOptions{
			ClusterMode:              true,
			NodeAddress:              nodeAddr,
			ClusterAddresses:         c.Cluster.ClusterAddresses,
			AutoSnapshot:             c.Cluster.AutoSnapshot,
			SnapshotInterval:         interval,
			SnapshotThreshold:        threshold,
			SnapshotCatchUpEntries:   catchUp,
			SnapshotDirSizeThreshold: c.Cluster.SnapshotDirSizeThreshold,
			SnapshotMaxDuration:      maxDur,
			SnapshotMinInterval:      minInterval,
			SnapshotMaxAppliedLag:    maxLag,
		}
	} else {
		opts.ClusterConfig = nil
	}
	return opts
}

func (c *ServerConfig) GRPCConfig() grpcserver.Config {
	tracingCfg := c.Observability.Tracing
	tracingEnabled := tracingCfg.Endpoint != "" || tracingCfg.SampleRatio > 0
	return grpcserver.Config{Address: c.GRPC.Address, EnableTracing: tracingEnabled}
}

func (c *ServerConfig) MetricsAddress() string {
	return c.Observability.MetricsAddress
}

func (c *ServerConfig) TracingConfig() TracingConfig {
	return c.Observability.Tracing
}

func (c *ServerConfig) PDAddress() string {
	return c.PD.Address
}

func (c *ServerConfig) PDHeartbeatInterval() time.Duration {
	return c.PD.HeartbeatInterval
}
