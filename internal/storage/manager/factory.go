// Package manager provides the factory function that constructs storage
// engines from configuration. Concrete engine packages are imported only here,
// keeping the rest of the application decoupled from specific implementations.
package manager

import (
	"fmt"

	"github.com/vaishnav-sp/cluster-db/internal/config"
	"github.com/vaishnav-sp/cluster-db/internal/storage"
	"github.com/vaishnav-sp/cluster-db/internal/storage/memory"
)

// EngineType enumerates the supported storage backends.
type EngineType string

const (
	// EngineMemory is the in-memory engine (testing / development only).
	EngineMemory EngineType = "memory"

	// Future engines — uncomment when implementations are available.
	// EngineBadger EngineType = "badger"
	// EnginePebble EngineType = "pebble"
	// EngineRocksDB EngineType = "rocksdb"
)

// Build constructs a storage.Engine from the provided StorageConfig.
// The returned interface value is the only reference to the concrete type;
// the concrete package is never exposed to callers of this function.
//
// Currently supported engines:
//
//	"memory" — in-memory, no persistence (default)
//
// Future engines will be added here without any changes to the manager or
// application packages.
func Build(cfg config.StorageConfig) (storage.Engine, error) {
	switch EngineType(cfg.Engine) {
	case EngineMemory:
		return memory.NewEngine(), nil

	// case EngineBadger:
	//     return badger.NewEngine(cfg), nil

	// case EnginePebble:
	//     return pebble.NewEngine(cfg), nil

	default:
		return nil, fmt.Errorf("%w: %q (supported: memory)", ErrUnknownEngine, cfg.Engine)
	}
}
