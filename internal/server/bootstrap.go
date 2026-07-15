package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/vaishnav-sp/cluster-db/internal/config"
	"github.com/vaishnav-sp/cluster-db/internal/server/handlers"
	"github.com/vaishnav-sp/cluster-db/internal/server/middleware"
	"github.com/vaishnav-sp/cluster-db/internal/storage/manager"
)

// New creates and initializes a new HTTP Server.
func New(cfg config.ServerConfig, log *zap.Logger, version string, startedAt time.Time, storage *manager.Manager) (*Server, error) {
	address := net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))

	mux := http.NewServeMux()
	healthHandler := handlers.NewHealthHandler("clusterdb", version, startedAt.UTC())
	mux.Handle("/health", healthHandler)
	mux.Handle("/live", healthHandler)
	mux.Handle("/ready", healthHandler)

	kvHandler := handlers.NewKVHandler(storage)
	mux.Handle("/v1/kv/", kvHandler)

	chain := middleware.Chain(
		mux,
		middleware.Recovery(log),
		middleware.RequestID(),
		middleware.Logging(log),
	)

	httpServer := &http.Server{
		Addr:         address,
		Handler:      chain,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	return &Server{
		httpServer: httpServer,
		logger:     log,
		config:     cfg,
	}, nil
}

// Start begins listening for HTTP connections.
func (s *Server) Start() error {
	s.logger.Info("Starting HTTP server", zap.String("address", s.httpServer.Addr))

	err := s.httpServer.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("%w: %s", ErrServerStart, err)
	}

	return nil
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down HTTP server")
	if err := s.httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("%w: %s", ErrServerShutdown, err)
	}
	return nil
}
