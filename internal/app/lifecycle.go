package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"go.uber.org/zap"
)

// Run starts the application and waits for shutdown signals.
func (a *Application) Run() error {
	a.Logger.Info("Application starting",
		zap.String("version", a.Version),
		zap.String("environment", a.Environment),
	)

	serverErrs := make(chan error, 1)
	go func() {
		serverErrs <- a.Server.Start()
	}()

	a.Logger.Info("HTTP server started",
		zap.String("address", fmt.Sprintf("%s:%d", a.Config.Server.Host, a.Config.Server.Port)),
	)

	if err := a.Storage.Open(context.Background()); err != nil {
		return fmt.Errorf("open storage: %w", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case err := <-serverErrs:
		return err
	case sig := <-sigCh:
		a.Logger.Info("Shutdown signal received", zap.String("signal", sig.String()))
	}

	return a.Shutdown()
}

// Shutdown gracefully shuts down the application.
func (a *Application) Shutdown() error {
	a.Logger.Info("Stopping HTTP server")

	ctx, cancel := context.WithTimeout(context.Background(), a.Config.Server.ShutdownTimeout)
	defer cancel()

	if err := a.Server.Shutdown(ctx); err != nil {
		return fmt.Errorf("%w", err)
	}

	if err := a.Storage.Close(ctx); err != nil {
		a.Logger.Error("Failed to close storage engine", zap.Error(err))
	}

	a.Logger.Info("Application shutdown complete")
	err := a.Logger.Sync()

	if err != nil && strings.Contains(strings.ToLower(err.Error()), "handle is invalid") {
		return nil
	}

	return err
}
