package logger

import (
	"go.uber.org/zap"

	"github.com/vaishnav-sp/cluster-db/internal/config"
)

// Logger exposes the shared structured logger interface for the application.
type Logger struct {
	*zap.Logger
}

// NewLogger creates a wrapped logger instance.
func NewLogger(cfg config.LoggingConfig) (*Logger, error) {
	base, err := New(cfg)
	if err != nil {
		return nil, err
	}
	return &Logger{Logger: base}, nil
}
