// Package wal implements the standalone append-only write-ahead log used by
// future ClusterDB storage engines. It deliberately contains no recovery or
// storage-engine integration; it only serializes durable mutation records.
package wal

// The package's public API consists of WALRecord, Writer, and Reader. This
// file intentionally reserves the package-level WAL orchestration surface for
// recovery integration in a later sprint.
