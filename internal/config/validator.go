package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Validate validates the provided configuration.
func Validate(cfg Config) error {
	if err := validatePort(cfg.Server.Port, "server.port"); err != nil {
		return err
	}
	if err := validatePort(cfg.Cluster.DiscoveryPort, "cluster.discovery_port"); err != nil {
		return err
	}
	if err := validatePort(cfg.Metrics.Port, "metrics.port"); err != nil {
		return err
	}
	if err := validatePort(cfg.Cache.Port, "cache.port"); err != nil {
		return err
	}
	if err := validateTimeout(cfg.Server.ReadTimeout, "server.read_timeout"); err != nil {
		return err
	}
	if err := validateTimeout(cfg.Server.WriteTimeout, "server.write_timeout"); err != nil {
		return err
	}
	if err := validateTimeout(cfg.Server.IdleTimeout, "server.idle_timeout"); err != nil {
		return err
	}
	if err := validateTimeout(cfg.Server.ShutdownTimeout, "server.shutdown_timeout"); err != nil {
		return err
	}
	if err := validateTimeout(cfg.Cluster.HeartbeatInterval, "cluster.heartbeat_interval"); err != nil {
		return err
	}
	if err := validateTimeout(cfg.Cluster.FailureTimeout, "cluster.failure_timeout"); err != nil {
		return err
	}
	if err := validateReplicationFactor(cfg.Cluster.ReplicationFactor); err != nil {
		return err
	}
	if err := validateJWTSecret(cfg.Authentication.JWTSecret); err != nil {
		return err
	}
	if err := validateNodeID(cfg.Cluster.NodeID); err != nil {
		return err
	}
	if err := validateDataDirectory(cfg.Storage.DataDirectory); err != nil {
		return err
	}
	if err := validateLoggingLevel(cfg.Logging.Level); err != nil {
		return err
	}
	return nil
}

func validatePort(port int, field string) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("%w: %s=%d", ErrInvalidPort, field, port)
	}
	return nil
}

func validateTimeout(value time.Duration, field string) error {
	if value <= 0 {
		return fmt.Errorf("%w: %s=%s", ErrInvalidTimeout, field, value)
	}
	return nil
}

func validateReplicationFactor(value int) error {
	if value < 1 || value > 9 {
		return fmt.Errorf("%w: replication_factor=%d", ErrInvalidReplicationFactor, value)
	}
	return nil
}

func validateJWTSecret(secret string) error {
	if strings.TrimSpace(secret) == "" || len(secret) < 32 {
		return fmt.Errorf("%w: jwt_secret length=%d", ErrInvalidJWTSecret, len(secret))
	}
	return nil
}

func validateNodeID(nodeID string) error {
	if strings.TrimSpace(nodeID) == "" {
		return fmt.Errorf("%w: node_id=%q", ErrInvalidNodeID, nodeID)
	}
	return nil
}

func validateDataDirectory(path string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("%w: data_directory=%q", ErrInvalidDataDirectory, path)
	}
	cleanPath := filepath.Clean(path)
	if cleanPath == "." || cleanPath == string(os.PathSeparator) {
		return fmt.Errorf("%w: data_directory=%q", ErrInvalidDataDirectory, path)
	}
	return nil
}

func validateLoggingLevel(level string) error {
	allowed := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !allowed[strings.ToLower(level)] {
		return fmt.Errorf("%w: logging.level=%q", ErrInvalidLoggingLevel, level)
	}
	return nil
}
