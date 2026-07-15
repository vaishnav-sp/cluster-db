// Package wal implements ClusterDB's append-only write-ahead log format.
package wal

import "errors"

// Sentinel errors returned by the WAL package. Callers should use errors.Is
// because errors returned from I/O operations include additional context.
var (
	ErrInvalidMagic     = errors.New("wal: invalid magic number")
	ErrInvalidVersion   = errors.New("wal: unsupported version")
	ErrChecksumMismatch = errors.New("wal: checksum mismatch")
	ErrUnexpectedEOF    = errors.New("wal: unexpected end of record")
	ErrCorruptedRecord  = errors.New("wal: corrupted record")
	ErrClosed           = errors.New("wal: closed")
)
