package app

import (
	"fmt"
	"time"

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

	httpServer, err := server.New(cfg.Server, log, version, startedAt, storageManager)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrServerInitialization, err)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrStorageInitialization, err)
	}

	app := &Application{
		Config:      cfg,
		Logger:      log,
		Server:      httpServer,
		Storage:     storageManager,
		StartedAt:   startedAt,
		Version:     version,
		Environment: cfg.Server.Environment,
	}

	return app, nil
}
