package server

import (
	"net/http"

	"go.uber.org/zap"

	"github.com/vaishnav-sp/cluster-db/internal/config"
)

// Server encapsulates HTTP server functionality for the application.
type Server struct {
	httpServer *http.Server
	logger     *zap.Logger
	config     config.ServerConfig
}
