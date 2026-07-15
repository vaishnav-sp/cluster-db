// Package manager provides manager-specific errors for the storage layer.
package manager

import "errors"

var (
	// ErrManagerNotOpen is returned when an operation is attempted before
	// the manager has been opened via Open().
	ErrManagerNotOpen = errors.New("storage manager: not open")

	// ErrManagerAlreadyOpen is returned when Open() is called on a manager
	// that has already been successfully opened.
	ErrManagerAlreadyOpen = errors.New("storage manager: already open")

	// ErrUnknownEngine is returned by the factory when the configured engine
	// name does not match any registered implementation.
	ErrUnknownEngine = errors.New("storage manager: unknown engine")
)
