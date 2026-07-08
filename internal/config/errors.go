package config

import "errors"

var (
	// ErrConfigFileNotFound indicates that the requested configuration file does not exist.
	ErrConfigFileNotFound = errors.New("configuration file not found")
	// ErrInvalidPort indicates that a port is outside the allowed range.
	ErrInvalidPort = errors.New("invalid port")
	// ErrInvalidTimeout indicates that a timeout value is invalid.
	ErrInvalidTimeout = errors.New("invalid timeout")
	// ErrInvalidReplicationFactor indicates that replication factor is invalid.
	ErrInvalidReplicationFactor = errors.New("invalid replication factor")
	// ErrInvalidJWTSecret indicates that the JWT secret is too short.
	ErrInvalidJWTSecret = errors.New("invalid jwt secret")
	// ErrInvalidNodeID indicates that the node ID is empty.
	ErrInvalidNodeID = errors.New("invalid node id")
	// ErrInvalidDataDirectory indicates that the data directory is empty.
	ErrInvalidDataDirectory = errors.New("invalid data directory")
	// ErrInvalidLoggingLevel indicates that the logging level is invalid.
	ErrInvalidLoggingLevel = errors.New("invalid logging level")
)
