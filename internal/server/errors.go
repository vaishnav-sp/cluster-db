package server

import "errors"

var (
	// ErrServerInitialization indicates the server could not be initialized.
	ErrServerInitialization = errors.New("server initialization failed")
	// ErrServerStart indicates the server could not start listening.
	ErrServerStart = errors.New("server start failed")
	// ErrServerShutdown indicates the server could not shut down gracefully.
	ErrServerShutdown = errors.New("server shutdown failed")
)
