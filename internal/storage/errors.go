// Package storage defines the core storage abstractions for ClusterDB.
// This file declares sentinel errors for the storage layer.
package storage

import "errors"

// Storage-layer sentinel errors. Use errors.Is for comparison.
var (
	// ErrKeyNotFound is returned when a requested key does not exist in the engine.
	ErrKeyNotFound = errors.New("storage: key not found")

	// ErrEngineClosed is returned when an operation is attempted on a closed engine.
	ErrEngineClosed = errors.New("storage: engine is closed")

	// ErrEngineNotOpen is returned when an operation is attempted before Open() is called.
	ErrEngineNotOpen = errors.New("storage: engine is not open")

	// ErrInvalidKey is returned when a nil or zero-length key is provided.
	ErrInvalidKey = errors.New("storage: invalid key")

	// ErrNilValue is returned when a nil value is provided for a write operation.
	ErrNilValue = errors.New("storage: nil value")

	// ErrIteratorClosed is returned when a method is called on a closed iterator.
	ErrIteratorClosed = errors.New("storage: iterator is closed")

	// ErrScanFinished is returned when Next() is called on an exhausted iterator.
	ErrScanFinished = errors.New("storage: scan finished")
)
