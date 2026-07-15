// Package manager owns the storage.Engine lifecycle and exposes a stable API
// that the application uses for all storage operations. The application never
// interacts with engine implementations directly.
package manager

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/vaishnav-sp/cluster-db/internal/config"
	"github.com/vaishnav-sp/cluster-db/internal/storage"
)

// Manager mediates between the application and a storage.Engine.
// It owns the engine's lifecycle (Open / Close) and delegates all I/O
// operations to the underlying engine.
//
// Manager is safe for concurrent use by multiple goroutines once Open
// has returned without error.
type Manager struct {
	engine storage.Engine
	cfg    config.StorageConfig
	logger *zap.Logger
	open   bool
}

// New constructs a Manager from configuration. The underlying engine is built
// via the factory but is NOT opened. Callers must invoke Open before performing
// any I/O operations.
func New(cfg config.StorageConfig, logger *zap.Logger) (*Manager, error) {
	eng, err := Build(cfg)
	if err != nil {
		return nil, fmt.Errorf("storage manager: build engine: %w", err)
	}

	return &Manager{
		engine: eng,
		cfg:    cfg,
		logger: logger,
	}, nil
}

// Open starts the underlying storage engine and marks the manager as ready.
// It must be called exactly once before any I/O method.
// Calling Open on an already-open manager returns ErrManagerAlreadyOpen.
func (m *Manager) Open(ctx context.Context) error {
	if m.open {
		return ErrManagerAlreadyOpen
	}

	m.logger.Info("Opening storage engine",
		zap.String("engine", m.cfg.Engine),
	)

	if err := m.engine.Open(ctx); err != nil {
		return fmt.Errorf("storage manager: open engine: %w", err)
	}

	m.open = true

	m.logger.Info("Storage engine opened",
		zap.String("engine", m.cfg.Engine),
	)

	return nil
}

// Close shuts down the underlying storage engine and marks the manager as
// closed. It is safe to call Close more than once; subsequent calls are no-ops.
func (m *Manager) Close(ctx context.Context) error {
	if !m.open {
		return nil
	}

	m.logger.Info("Closing storage engine",
		zap.String("engine", m.cfg.Engine),
	)

	if err := m.engine.Close(ctx); err != nil {
		return fmt.Errorf("storage manager: close engine: %w", err)
	}

	m.open = false

	m.logger.Info("Storage engine closed",
		zap.String("engine", m.cfg.Engine),
	)

	return nil
}

// Put writes a record to the engine.
func (m *Manager) Put(ctx context.Context, rec storage.Record) error {
	if err := m.checkOpen(); err != nil {
		return err
	}

	if err := m.engine.Put(ctx, rec); err != nil {
		return fmt.Errorf("storage manager: put: %w", err)
	}

	return nil
}

// Get retrieves the record associated with key.
// Returns storage.ErrKeyNotFound (wrapped) when the key is absent.
func (m *Manager) Get(ctx context.Context, key storage.Key) (storage.Record, error) {
	if err := m.checkOpen(); err != nil {
		return storage.Record{}, err
	}

	rec, err := m.engine.Get(ctx, key)
	if err != nil {
		return storage.Record{}, fmt.Errorf("storage manager: get: %w", err)
	}

	return rec, nil
}

// Delete removes the record associated with key. Delete is idempotent.
func (m *Manager) Delete(ctx context.Context, key storage.Key) error {
	if err := m.checkOpen(); err != nil {
		return err
	}

	if err := m.engine.Delete(ctx, key); err != nil {
		return fmt.Errorf("storage manager: delete: %w", err)
	}

	return nil
}

// Exists reports whether key is present in the engine.
func (m *Manager) Exists(ctx context.Context, key storage.Key) (bool, error) {
	if err := m.checkOpen(); err != nil {
		return false, err
	}

	ok, err := m.engine.Exists(ctx, key)
	if err != nil {
		return false, fmt.Errorf("storage manager: exists: %w", err)
	}

	return ok, nil
}

// Scan returns an iterator over records matching opts.
// The caller must close the iterator when done.
func (m *Manager) Scan(ctx context.Context, opts storage.ScanOptions) (storage.Iterator, error) {
	if err := m.checkOpen(); err != nil {
		return nil, err
	}

	iter, err := m.engine.Scan(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("storage manager: scan: %w", err)
	}

	return iter, nil
}

// Stats returns a point-in-time snapshot of engine metrics.
func (m *Manager) Stats(ctx context.Context) (storage.Stats, error) {
	if err := m.checkOpen(); err != nil {
		return storage.Stats{}, err
	}

	stats, err := m.engine.Stats(ctx)
	if err != nil {
		return storage.Stats{}, fmt.Errorf("storage manager: stats: %w", err)
	}

	return stats, nil
}

// Health returns the current liveness state of the engine.
// This method intentionally bypasses the open check so it can report
// unhealthy state even when the manager is not open.
func (m *Manager) Health(ctx context.Context) (storage.Health, error) {
	health, err := m.engine.Health(ctx)
	if err != nil {
		return storage.Health{}, fmt.Errorf("storage manager: health: %w", err)
	}

	return health, nil
}

// checkOpen returns ErrManagerNotOpen if the manager has not been opened.
func (m *Manager) checkOpen() error {
	if !m.open {
		return ErrManagerNotOpen
	}

	return nil
}
