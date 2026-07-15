// Package storage defines the core storage abstractions for ClusterDB.
// This file declares shared data models consumed by all engine implementations.
package storage

import "time"

// Key is a raw byte-slice key. Using []byte avoids string allocations at the
// hot path and keeps the engine encoding-agnostic.
type Key []byte

// Value is a raw byte-slice payload. Engines treat values as opaque blobs.
type Value []byte

// Metadata holds per-record bookkeeping fields.
// All fields are optional; zero values are safe defaults.
type Metadata struct {
	// TTL is the duration after which the record should be considered expired.
	// Zero means no expiry. Enforcement is the responsibility of the engine.
	TTL time.Duration

	// Version is a monotonically increasing logical clock for the record.
	// Used for optimistic concurrency control (compare-and-swap).
	Version uint64

	// CreatedAt is the wall-clock time at which the record was first written.
	CreatedAt time.Time

	// UpdatedAt is the wall-clock time of the most recent write to this record.
	UpdatedAt time.Time

	// DeleteMarker marks the record as a tombstone for MVCC-style deletion.
	// When true, the record is logically deleted but physically retained until
	// compaction. Future MVCC implementations must respect this flag.
	DeleteMarker bool
}

// Record is the atomic unit of storage — a key bundled with its value and
// metadata. Engines always return a complete Record so callers never observe
// split state between a value and its associated metadata.
type Record struct {
	Key      Key
	Value    Value
	Metadata Metadata
}

// ScanOptions controls the behaviour of an Engine.Scan call.
// All fields are optional; zero values apply no constraints.
type ScanOptions struct {
	// Prefix restricts the scan to keys that begin with this byte sequence.
	// If set, Start/End are applied after prefix filtering.
	Prefix Key

	// Start is the inclusive lower bound for key iteration.
	// A nil Start means "from the beginning".
	Start Key

	// End is the exclusive upper bound for key iteration.
	// A nil End means "to the end".
	End Key

	// Limit caps the number of records returned. Zero means unlimited.
	Limit int

	// Reverse iterates in descending key order when true.
	Reverse bool
}

// Stats contains point-in-time metrics for a storage engine.
// Future engines may populate a subset of fields; zero values are safe.
type Stats struct {
	// Keys is the number of live (non-tombstone) records in the engine.
	Keys int64

	// Size is the approximate on-disk or in-memory size in bytes.
	Size int64

	// Engine is the human-readable name of the engine implementation.
	Engine string

	// Version is the engine's internal version string (e.g. "memory/v1").
	Version string

	// Created is the time at which the engine was opened.
	Created time.Time

	// Healthy indicates whether the engine is currently operational.
	Healthy bool
}

// Health is a lightweight liveness snapshot for a storage engine.
// It is designed to be consumed by health-check endpoints and cluster monitors.
type Health struct {
	// Healthy is true when the engine is open and able to serve requests.
	Healthy bool

	// Message is a human-readable description of the engine's current state.
	// On degradation it contains a brief explanation of the problem.
	Message string

	// Timestamp is the wall-clock time at which the health snapshot was taken.
	Timestamp time.Time
}
