package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/vaishnav-sp/cluster-db/internal/cluster"
	"github.com/vaishnav-sp/cluster-db/internal/cluster/handoff"
	clusterRPC "github.com/vaishnav-sp/cluster-db/internal/cluster/rpc"
	"github.com/vaishnav-sp/cluster-db/internal/config"
	docservice "github.com/vaishnav-sp/cluster-db/internal/document/service"
	"github.com/vaishnav-sp/cluster-db/internal/server/handlers"
	"github.com/vaishnav-sp/cluster-db/internal/server/middleware"
	"github.com/vaishnav-sp/cluster-db/internal/storage"
	"github.com/vaishnav-sp/cluster-db/internal/storage/manager"
)

// New creates and initializes a new HTTP Server.
func New(cfg config.ServerConfig, log *zap.Logger, version string, startedAt time.Time, store *manager.Manager, clusterManager *cluster.Manager, hintManager *handoff.Manager) (*Server, error) {
	address := net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))

	mux := http.NewServeMux()
	healthHandler := handlers.NewHealthHandler("clusterdb", version, startedAt.UTC())
	mux.Handle("/health", healthHandler)
	mux.Handle("/live", healthHandler)
	mux.Handle("/ready", healthHandler)

	kvHandler := handlers.NewKVHandler(store, clusterManager, hintManager)
	mux.Handle("/v1/kv/", kvHandler)

	documentService := docservice.New(store)
	documentHandler := handlers.NewDocumentHandler(documentService, clusterManager)
	mux.Handle("/v1/documents", documentHandler)
	mux.Handle("/v1/documents/", documentHandler)

	rpcServer := clusterRPC.NewServer()
	if clusterManager != nil {
		rpcServer.HeartbeatHandler = clusterManager.HandleHeartbeat
		rpcServer.JoinHandler = clusterManager.HandleJoin
		rpcServer.LeaveHandler = clusterManager.HandleLeave
		rpcServer.GossipHandler = clusterManager.HandleGossip
	} else {
		rpcServer.HeartbeatHandler = func(req clusterRPC.HeartbeatRequest) (clusterRPC.HeartbeatResponse, error) {
			return clusterRPC.HeartbeatResponse{Accepted: true, Message: "ok"}, nil
		}
		rpcServer.JoinHandler = func(req clusterRPC.JoinRequest) (clusterRPC.JoinResponse, error) {
			return clusterRPC.JoinResponse{Accepted: true, Message: "ok"}, nil
		}
		rpcServer.LeaveHandler = func(req clusterRPC.LeaveRequest) (clusterRPC.LeaveResponse, error) {
			return clusterRPC.LeaveResponse{Accepted: true, Message: "ok"}, nil
		}
	}
	rpcServer.AppendHandler = func(req clusterRPC.AppendEntriesRequest) (clusterRPC.AppendEntriesResponse, error) {
		return clusterRPC.AppendEntriesResponse{Accepted: true, Message: "ok"}, nil
	}
	rpcServer.KVGetHandler = func(req clusterRPC.KVGetRequest) (clusterRPC.KVGetResponse, error) {
		if store == nil {
			return clusterRPC.KVGetResponse{Error: "storage unavailable"}, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rec, err := store.Get(ctx, storage.Key(req.Key))
		if err != nil {
			return clusterRPC.KVGetResponse{Error: err.Error()}, nil
		}
		return clusterRPC.KVGetResponse{Value: rec.Value, Found: true}, nil
	}
	rpcServer.KVPutHandler = func(req clusterRPC.KVPutRequest) (clusterRPC.KVPutResponse, error) {
		if store == nil {
			return clusterRPC.KVPutResponse{Error: "storage unavailable"}, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rec := storage.Record{Key: storage.Key(req.Key), Value: storage.Value(req.Value)}
		if err := store.Put(ctx, rec); err != nil {
			return clusterRPC.KVPutResponse{Error: err.Error()}, nil
		}
		if _, err := store.Get(ctx, storage.Key(req.Key)); err != nil {
			return clusterRPC.KVPutResponse{Error: "storage write verification failed"}, nil
		}
		return clusterRPC.KVPutResponse{Success: true}, nil
	}
	rpcServer.KVDeleteHandler = func(req clusterRPC.KVDeleteRequest) (clusterRPC.KVDeleteResponse, error) {
		if store == nil {
			return clusterRPC.KVDeleteResponse{Error: "storage unavailable"}, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := store.Delete(ctx, storage.Key(req.Key)); err != nil {
			return clusterRPC.KVDeleteResponse{Error: err.Error()}, nil
		}
		return clusterRPC.KVDeleteResponse{Success: true}, nil
	}
	rpcServer.ReplicaPutHandler = func(req clusterRPC.ReplicaPutRequest) (clusterRPC.ReplicaPutResponse, error) {
		if store == nil {
			return clusterRPC.ReplicaPutResponse{Error: "storage unavailable"}, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rec := storage.Record{
			Key:      storage.Key(req.Key),
			Value:    storage.Value(req.Value),
			Metadata: storage.Metadata{Version: req.Version},
		}
		if err := store.Put(ctx, rec); err != nil {
			return clusterRPC.ReplicaPutResponse{Error: err.Error()}, nil
		}
		saved, err := store.GetRaw(ctx, storage.Key(req.Key))
		if err != nil {
			return clusterRPC.ReplicaPutResponse{Success: true, Version: req.Version}, nil
		}
		return clusterRPC.ReplicaPutResponse{Success: true, Version: saved.Metadata.Version}, nil
	}
	rpcServer.ReplicaDeleteHandler = func(req clusterRPC.ReplicaDeleteRequest) (clusterRPC.ReplicaDeleteResponse, error) {
		if store == nil {
			return clusterRPC.ReplicaDeleteResponse{Error: "storage unavailable"}, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := store.Delete(ctx, storage.Key(req.Key)); err != nil {
			return clusterRPC.ReplicaDeleteResponse{Error: err.Error()}, nil
		}
		saved, err := store.GetRaw(ctx, storage.Key(req.Key))
		if err != nil {
			return clusterRPC.ReplicaDeleteResponse{Success: true}, nil
		}
		return clusterRPC.ReplicaDeleteResponse{Success: true, Version: saved.Metadata.Version}, nil
	}
	// ReplicaGet reads the local storage only. No routing, no forwarding.
	rpcServer.ReplicaGetHandler = func(req clusterRPC.ReplicaGetRequest) (clusterRPC.ReplicaGetResponse, error) {
		if store == nil {
			return clusterRPC.ReplicaGetResponse{Error: "storage unavailable"}, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rec, err := store.GetRaw(ctx, storage.Key(req.Key))
		if err != nil {
			return clusterRPC.ReplicaGetResponse{Found: false}, nil
		}
		return clusterRPC.ReplicaGetResponse{
			Found:        true,
			Value:        rec.Value,
			Version:      rec.Metadata.Version,
			DeleteMarker: rec.Metadata.DeleteMarker,
		}, nil
	}
	mux.Handle("/cluster/", rpcServer.Handler())

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
