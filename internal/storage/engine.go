// Package storage defines the core storage abstractions for ClusterDB.
// This file declares the Engine and Iterator interfaces that every storage
// backend must satisfy.
package storage

import "context"

// Engine is the primary interface every ClusterDB storage backend must implement.
//
// Lifecycle:
//
//	Open  →  [Put | Get | Delete | Exists | Scan | Stats | Health]  →  Close
//
// All methods accept a context.Context so callers can propagate deadlines and
// cancellation signals across the distributed system. Implementations must
// respect context cancellation and return ctx.Err() when appropriate.
//
// Implementations must be safe for concurrent use by multiple goroutines.
type Engine interface {
	// Open initialises the engine and makes it ready for I/O.
	// It must be called exactly once before any other method.
	// Calling Open on an already-open engine returns ErrEngineNotOpen.
	Open(ctx context.Context) error

	// Close flushes pending state and releases all resources held by the engine.
	// After Close returns, the engine must not accept further operations.
	// It is safe to call Close more than once; subsequent calls are no-ops.
	Close(ctx context.Context) error

	// Put writes a record to the engine, creating it if it does not exist or
	// overwriting it if it does.
	//
	// Returns ErrInvalidKey if rec.Key is nil or empty.
	// Returns ErrNilValue if rec.Value is nil.
	// Returns ErrEngineNotOpen / ErrEngineClosed on lifecycle violations.
	Put(ctx context.Context, rec Record) error

	// Get retrieves the record associated with key.
	//
	// Returns ErrKeyNotFound if the key does not exist.
	// Returns ErrInvalidKey if key is nil or empty.
	// Returns ErrEngineNotOpen / ErrEngineClosed on lifecycle violations.
	Get(ctx context.Context, key Key) (Record, error)

	// Delete removes the record associated with key.
	// Delete is idempotent: deleting a non-existent key returns nil.
	//
	// Returns ErrInvalidKey if key is nil or empty.
	// Returns ErrEngineNotOpen / ErrEngineClosed on lifecycle violations.
	Delete(ctx context.Context, key Key) error

	// Exists reports whether key is present in the engine without fetching
	// the associated value. This is cheaper than Get when only presence matters.
	//
	// Returns ErrInvalidKey if key is nil or empty.
	// Returns ErrEngineNotOpen / ErrEngineClosed on lifecycle violations.
	Exists(ctx context.Context, key Key) (bool, error)

	// Scan returns an Iterator over records matching opts.
	// The caller is responsible for closing the returned iterator via
	// Iterator.Close, even when iteration terminates early.
	//
	// Returns ErrEngineNotOpen / ErrEngineClosed on lifecycle violations.
	Scan(ctx context.Context, opts ScanOptions) (Iterator, error)

	// Stats returns a point-in-time snapshot of engine metrics.
	// Implementations should minimise lock contention; Stats need not be
	// perfectly consistent.
	Stats(ctx context.Context) (Stats, error)

	// Health returns the current liveness state of the engine.
	// This is intended for consumption by health-check handlers and cluster
	// monitors. It must never block for more than a few milliseconds.
	Health(ctx context.Context) (Health, error)
}

// Iterator provides sequential access to a set of records produced by Engine.Scan.
//
// Typical usage:
//
//	iter, err := engine.Scan(ctx, opts)
//	if err != nil { ... }
//	defer iter.Close()
//
//	for iter.Next() {
//	    rec := iter.Record()
//	    // process rec
//	}
//	if err := iter.Error(); err != nil { ... }
//
// Implementations must be safe for sequential use within a single goroutine.
// Iterators are not required to be concurrency-safe.
type Iterator interface {
	// Next advances the iterator to the next record.
	// It returns true if a record is available, false when the scan is
	// exhausted or an error occurred.
	Next() bool

	// Record returns the current record.
	// Callers must call Next at least once before calling Record.
	// The returned Record is valid only until the next call to Next or Close.
	Record() Record

	// Error returns the first error encountered during iteration, if any.
	// A nil return after Next() == false means the scan completed successfully.
	Error() error

	// Close releases all resources held by the iterator.
	// Close must be called exactly once; further calls are no-ops.
	Close() error
}
