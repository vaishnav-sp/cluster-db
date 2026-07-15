package app

import (
	"time"

	"go.uber.org/zap"

	"github.com/vaishnav-sp/cluster-db/internal/config"
	"github.com/vaishnav-sp/cluster-db/internal/server"
	"github.com/vaishnav-sp/cluster-db/internal/storage/manager"
)

// Application holds the shared infrastructure for the ClusterDB application.
type Application struct {
	Config      *config.Config
	Logger      *zap.Logger
	Server      *server.Server
	Storage     *manager.Manager
	StartedAt   time.Time
	Version     string
	Environment string
}
