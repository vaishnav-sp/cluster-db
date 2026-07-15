// Package memory provides an in-memory implementation of the storage.Engine
// interface for ClusterDB. It is intended for unit testing and local
// development only — it offers no persistence, WAL, or replication.
package memory

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/vaishnav-sp/cluster-db/internal/storage"
)

const (
	engineName    = "memory"
	engineVersion = "memory/v1"
)

// Engine is a thread-safe, in-memory storage.Engine backed by a Go map.
//
// Compile-time assertion that *Engine satisfies storage.Engine:
//
//	var _ storage.Engine = (*Engine)(nil)
type Engine struct {
	mu        sync.RWMutex
	store     map[string]storage.Record
	open      bool
	createdAt time.Time
}

// Compile-time interface assertion.
var _ storage.Engine = (*Engine)(nil)

// NewEngine returns a new, unopened in-memory Engine.
// Call Open before performing any I/O operations.
func NewEngine() *Engine {
	return &Engine{
		store: make(map[string]storage.Record),
	}
}

// Open initialises the engine. It must be called once before any I/O method.
// Calling Open on an already-open engine is a no-op.
func (e *Engine) Open(_ context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.open {
		return nil
	}

	e.open = true
	e.createdAt = time.Now()

	return nil
}

// Close marks the engine as closed. All subsequent I/O operations will return
// storage.ErrEngineClosed. Calling Close more than once is safe.
func (e *Engine) Close(_ context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.open = false

	return nil
}

// Put writes rec to the engine. It overwrites any existing record with the
// same key. Metadata.CreatedAt is preserved on overwrites.
func (e *Engine) Put(_ context.Context, rec storage.Record) error {
	if err := validateKey(rec.Key); err != nil {
		return err
	}

	if rec.Value == nil {
		return storage.ErrNilValue
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.checkOpen(); err != nil {
		return err
	}

	k := string(rec.Key)
	now := time.Now()

	// Preserve CreatedAt if the record already exists.
	if existing, ok := e.store[k]; ok {
		rec.Metadata.CreatedAt = existing.Metadata.CreatedAt
	} else {
		rec.Metadata.CreatedAt = now
	}

	rec.Metadata.UpdatedAt = now

	// Defensive copy: keep the stored value independent of caller-owned slices.
	rec.Key = copyBytes(rec.Key)
	rec.Value = copyBytes(rec.Value)

	e.store[k] = rec

	return nil
}

// Get retrieves the record associated with key.
// Returns storage.ErrKeyNotFound if the key is absent.
func (e *Engine) Get(_ context.Context, key storage.Key) (storage.Record, error) {
	if err := validateKey(key); err != nil {
		return storage.Record{}, err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if err := e.checkOpen(); err != nil {
		return storage.Record{}, err
	}

	rec, ok := e.store[string(key)]
	if !ok {
		return storage.Record{}, storage.ErrKeyNotFound
	}

	return rec, nil
}

// Delete removes the record associated with key.
// Delete is idempotent: deleting a non-existent key returns nil.
func (e *Engine) Delete(_ context.Context, key storage.Key) error {
	if err := validateKey(key); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.checkOpen(); err != nil {
		return err
	}

	delete(e.store, string(key))

	return nil
}

// Exists reports whether key is present in the engine.
// It is cheaper than Get when only presence is needed.
func (e *Engine) Exists(_ context.Context, key storage.Key) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if err := e.checkOpen(); err != nil {
		return false, err
	}

	_, ok := e.store[string(key)]

	return ok, nil
}

// Scan returns an iterator over records matching opts.
// The caller must close the iterator when done, even on early exit.
func (e *Engine) Scan(_ context.Context, opts storage.ScanOptions) (storage.Iterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if err := e.checkOpen(); err != nil {
		return nil, err
	}

	records := e.collectRecords(opts)

	return newIterator(records), nil
}

// Stats returns a point-in-time snapshot of engine metrics.
func (e *Engine) Stats(_ context.Context) (storage.Stats, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if err := e.checkOpen(); err != nil {
		return storage.Stats{}, err
	}

	var size int64
	for _, rec := range e.store {
		size += int64(len(rec.Key)) + int64(len(rec.Value))
	}

	return storage.Stats{
		Keys:    int64(len(e.store)),
		Size:    size,
		Engine:  engineName,
		Version: engineVersion,
		Created: e.createdAt,
		Healthy: true,
	}, nil
}

// Health returns the current liveness state of the engine.
func (e *Engine) Health(_ context.Context) (storage.Health, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.open {
		return storage.Health{
			Healthy:   false,
			Message:   "engine is closed",
			Timestamp: time.Now(),
		}, nil
	}

	return storage.Health{
		Healthy:   true,
		Message:   "engine is open and operational",
		Timestamp: time.Now(),
	}, nil
}

// ── internal helpers ──────────────────────────────────────────────────────────

// checkOpen returns an error if the engine is not open.
// Callers must hold at least a read lock.
func (e *Engine) checkOpen() error {
	if !e.open {
		return storage.ErrEngineClosed
	}

	return nil
}

// collectRecords gathers and filters records from the store according to opts.
// Callers must hold at least a read lock.
func (e *Engine) collectRecords(opts storage.ScanOptions) []storage.Record {
	// Collect all matching keys first so we can sort them.
	keys := make([]string, 0, len(e.store))

	for k := range e.store {
		kb := []byte(k)

		if len(opts.Prefix) > 0 && !bytes.HasPrefix(kb, opts.Prefix) {
			continue
		}

		if len(opts.Start) > 0 && bytes.Compare(kb, opts.Start) < 0 {
			continue
		}

		if len(opts.End) > 0 && bytes.Compare(kb, opts.End) >= 0 {
			continue
		}

		keys = append(keys, k)
	}

	sort.Strings(keys)

	if opts.Reverse {
		reverseStrings(keys)
	}

	if opts.Limit > 0 && len(keys) > opts.Limit {
		keys = keys[:opts.Limit]
	}

	records := make([]storage.Record, 0, len(keys))

	for _, k := range keys {
		records = append(records, e.store[k])
	}

	return records
}

// validateKey returns ErrInvalidKey if key is nil or empty.
func validateKey(key storage.Key) error {
	if len(key) == 0 {
		return fmt.Errorf("%w: key must not be nil or empty", storage.ErrInvalidKey)
	}

	return nil
}

// copyBytes returns a fresh copy of b.
func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}

	cp := make([]byte, len(b))
	copy(cp, b)

	return cp
}

// reverseStrings reverses s in place.
func reverseStrings(s []string) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// ── iterator ──────────────────────────────────────────────────────────────────

// iterator is a slice-backed, forward-only implementation of storage.Iterator.
// It is not concurrency-safe; callers must not share an iterator across
// goroutines without external synchronisation.
type iterator struct {
	records []storage.Record
	pos     int
	current storage.Record
	err     error
	closed  bool
}

// Compile-time interface assertion.
var _ storage.Iterator = (*iterator)(nil)

// newIterator returns an iterator over records.
func newIterator(records []storage.Record) *iterator {
	return &iterator{
		records: records,
		pos:     -1,
	}
}

// Next advances the iterator. Returns true if a record is available.
func (it *iterator) Next() bool {
	if it.closed {
		it.err = storage.ErrIteratorClosed
		return false
	}

	it.pos++

	if it.pos >= len(it.records) {
		return false
	}

	it.current = it.records[it.pos]

	return true
}

// Record returns the record at the current iterator position.
// Callers must call Next at least once before Record.
func (it *iterator) Record() storage.Record {
	return it.current
}

// Error returns the first error encountered during iteration.
func (it *iterator) Error() error {
	return it.err
}

// Close marks the iterator as closed and releases its internal slice.
func (it *iterator) Close() error {
	if it.closed {
		return nil
	}

	it.closed = true
	it.records = nil

	return nil
}
