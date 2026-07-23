package app

import (
	"fmt"
	"time"

	"github.com/vaishnav-sp/cluster-db/internal/cluster"
	"github.com/vaishnav-sp/cluster-db/internal/cluster/gossip"
	"github.com/vaishnav-sp/cluster-db/internal/cluster/handoff"
	clusterRPC "github.com/vaishnav-sp/cluster-db/internal/cluster/rpc"
	"github.com/vaishnav-sp/cluster-db/internal/config"
	"github.com/vaishnav-sp/cluster-db/internal/logger"
	"github.com/vaishnav-sp/cluster-db/internal/server"
	"github.com/vaishnav-sp/cluster-db/internal/storage/manager"
)

// New creates and initializes a new Application.
func New(version string) (*Application, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrConfigurationInitialization, err)
	}

	log, err := logger.New(cfg.Logging)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrLoggerInitialization, err)
	}

	startedAt := time.Now()
	storageManager, err := manager.New(cfg.Storage, log)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrStorageInitialization, err)
	}

	clusterManager := cluster.NewManager(
	cluster.NewMembership(),
	log,
	cfg.Cluster.HeartbeatInterval,
	cfg.Cluster.FailureTimeout,
	cfg.Cluster.NodeID,
	cfg.Cluster.NodeAddress,
)

rf := cfg.Storage.ReplicationFactor
if rf <= 0 {
	rf = cfg.Cluster.ReplicationFactor
}

w := cfg.Storage.WriteQuorum
if w <= 0 {
	w = (rf / 2) + 1
}

r := cfg.Storage.ReadQuorum
if r <= 0 {
	r = (rf / 2) + 1
}

clusterManager.ReplicationFactor = rf
clusterManager.WriteQuorum = w
clusterManager.ReadQuorum = r
	hintManager := handoff.NewManager()
	gossipEngine := gossip.NewEngine(gossip.Config{
		LocalNodeID:    cfg.Cluster.NodeID,
		LocalAddress:   cfg.Cluster.NodeAddress,
		Membership:     clusterManager.Membership(),
		Client:         clusterRPC.NewClient(5 * time.Second),
		Logger:         log,
		Interval:       cfg.Cluster.GossipInterval,
		Fanout:         cfg.Cluster.GossipFanout,
		FailureTimeout: cfg.Cluster.FailureTimeout,
		HintManager:    hintManager,
	})
	clusterManager.SetGossipEngine(gossipEngine)

	httpServer, err := server.New(cfg.Server, log, version, startedAt, storageManager, clusterManager, hintManager)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrServerInitialization, err)
	}

	app := &Application{
		Config:      cfg,
		Logger:      log,
		Server:      httpServer,
		Storage:     storageManager,
		Cluster:     clusterManager,
		StartedAt:   startedAt,
		Version:     version,
		Environment: cfg.Server.Environment,
	}

	return app, nil
}
