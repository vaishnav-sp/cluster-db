// Package document defines the document layer for ClusterDB.
// This file declares sentinel errors for document validation.
package document

import "errors"

// Document-layer sentinel errors. Use errors.Is for comparison.
var (
	// ErrInvalidJSON is returned when the payload is not valid JSON or the root value is not an object.
	ErrInvalidJSON = errors.New("document: invalid json")

	// ErrEmptyDocument is returned when the request body or parsed object is empty.
	ErrEmptyDocument = errors.New("document: empty document")
)
