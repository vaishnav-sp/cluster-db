// Package memory provides an in-memory implementation of the storage.Engine
// interface for ClusterDB. It is intended for unit testing and local
// development only — it offers no persistence, WAL, or replication.
package memory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/vaishnav-sp/cluster-db/internal/storage"
	"github.com/vaishnav-sp/cluster-db/internal/storage/checkpoint"
	"github.com/vaishnav-sp/cluster-db/internal/storage/wal"
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
	cfg       Config
	wal       *wal.Writer
	open      bool
	createdAt time.Time

	checkpointCount int64
	replayDuration  time.Duration
	lastCheckpoint  time.Time
	maintenanceStop context.CancelFunc
	maintenanceDone chan struct{}
	checkpoint      *checkpoint.Checkpoint
}

// Config configures optional persistence for Engine.
type Config struct {
	WAL                WALConfig
	CheckpointEnabled  bool
	CheckpointInterval time.Duration
	CheckpointSize     int64
	WALMaxSegmentSize  int64
	WALMaxSegments     int
}

// WALConfig controls the memory engine's write-ahead log.
type WALConfig struct {
	Enabled     bool
	Path        string
	SyncOnWrite bool
}

// Compile-time interface assertion.
var _ storage.Engine = (*Engine)(nil)

// NewEngine returns a new, unopened in-memory Engine. The optional config
// preserves the original no-argument constructor for callers that do not use
// WAL persistence. Call Open before performing any I/O operations.
func NewEngine(configs ...Config) *Engine {
	cfg := Config{}
	if len(configs) > 0 {
		cfg = configs[0]
	}
	return &Engine{
		store: make(map[string]storage.Record),
		cfg:   cfg,
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

	if e.cfg.WAL.Enabled {
		if err := e.openWALLocked(); err != nil {
			return err
		}
	}

	e.open = true
	e.createdAt = time.Now()
	e.startMaintenanceLocked()

	return nil
}

// Close marks the engine as closed. All subsequent I/O operations will return
// storage.ErrEngineClosed. Calling Close more than once is safe.
func (e *Engine) Close(_ context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.open {
		return nil
	}
	e.open = false
	stop := e.maintenanceStop
	done := e.maintenanceDone
	e.maintenanceStop = nil
	e.maintenanceDone = nil
	if stop != nil {
		stop()
	}
	e.mu.Unlock()
	if done != nil {
		<-done
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.wal != nil {
		if err := e.wal.Sync(); err != nil {
			return fmt.Errorf("memory engine: sync WAL: %w", err)
		}
		if err := e.wal.Close(); err != nil {
			return fmt.Errorf("memory engine: close WAL: %w", err)
		}
		e.wal = nil
	}
	if e.cfg.WAL.Enabled && e.cfg.CheckpointEnabled && e.walSizeLocked() > 0 {
		if err := e.createCheckpointLocked(); err != nil {
			return fmt.Errorf("memory engine: final checkpoint: %w", err)
		}
	}
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

	if err := e.appendWALLocked(wal.OperationPut, rec.Key, rec.Value, now); err != nil {
		return err
	}

	// Defensive copy: keep the stored value independent of caller-owned slices.
	rec.Key = copyBytes(rec.Key)
	rec.Value = copyBytes(rec.Value)

	e.store[k] = rec
	if err := e.maybeCheckpointLocked(false); err != nil {
		return err
	}

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

	if err := e.appendWALLocked(wal.OperationDelete, key, nil, time.Now()); err != nil {
		return err
	}

	delete(e.store, string(key))
	if err := e.maybeCheckpointLocked(false); err != nil {
		return err
	}

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
		Keys:            int64(len(e.store)),
		Size:            size,
		Engine:          engineName,
		Version:         engineVersion,
		Created:         e.createdAt,
		Healthy:         true,
		CheckpointCount: e.checkpointCount,
		WALSize:         e.walSizeLocked(),
		ReplayDuration:  e.replayDuration,
		LastCheckpoint:  e.lastCheckpoint,
		WALSegments:     int64(len(e.walSegmentPathsLocked()) + boolToInt(e.wal != nil)),
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

// openWALLocked opens the configured WAL, replays it into a fresh map, and
// installs a writer positioned at the end of the log. Callers must hold e.mu.
func (e *Engine) openWALLocked() error {
	if e.cfg.WAL.Path == "" {
		return fmt.Errorf("memory engine: WAL enabled with empty path")
	}
	if dir := filepath.Dir(e.cfg.WAL.Path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("memory engine: create WAL directory: %w", err)
		}
	}
	file, err := os.OpenFile(e.cfg.WAL.Path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("memory engine: open WAL: %w", err)
	}

	store := make(map[string]storage.Record)
	started := time.Now()
	var checkpointTime time.Time
	if cp, err := checkpoint.New(e.checkpointPath()); err != nil {
		_ = file.Close()
		return fmt.Errorf("memory engine: init checkpoint: %w", err)
	} else {
		e.checkpoint = cp
		if state, ts, loadErr := cp.LoadWithTimestamp(); loadErr == nil {
			for key, record := range state {
				store[key] = record
			}
			checkpointTime = ts
		} else if !os.IsNotExist(loadErr) {
			_ = file.Close()
			return fmt.Errorf("memory engine: load checkpoint: %w", loadErr)
		}
	}
	if err := e.replayStateLocked(file, store, checkpointTime); err != nil {
		_ = file.Close()
		return fmt.Errorf("memory engine: replay WAL: %w", err)
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return fmt.Errorf("memory engine: seek WAL end: %w", err)
	}
	writer, err := wal.NewWriter(file)
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("memory engine: create WAL writer: %w", err)
	}
	e.store = store
	e.wal = writer
	e.replayDuration = time.Since(started)
	return nil
}

// appendWALLocked records a mutation before it is applied to e.store. Callers
// must hold e.mu, which preserves WAL and in-memory mutation order.
func (e *Engine) appendWALLocked(op wal.OperationType, key, value []byte, timestamp time.Time) error {
	if e.wal == nil {
		return nil
	}
	if err := e.rotateWALIfNeededLocked(); err != nil {
		return err
	}
	if err := e.wal.Append(wal.WALRecord{Operation: op, Timestamp: timestamp, Key: key, Value: value}); err != nil {
		return fmt.Errorf("memory engine: append WAL: %w", err)
	}
	if e.cfg.WAL.SyncOnWrite {
		if err := e.wal.Sync(); err != nil {
			return fmt.Errorf("memory engine: sync WAL: %w", err)
		}
	}
	return nil
}

func (e *Engine) startMaintenanceLocked() {
	if !e.cfg.WAL.Enabled || !e.cfg.CheckpointEnabled || e.cfg.CheckpointInterval <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.maintenanceStop = cancel
	e.maintenanceDone = make(chan struct{})
	interval := e.cfg.CheckpointInterval
	go func(done chan<- struct{}) {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.mu.Lock()
				if e.open {
					_ = e.maybeCheckpointLocked(true)
				}
				e.mu.Unlock()
			}
		}
	}(e.maintenanceDone)
}

func (e *Engine) checkpointPath() string {
	if e.cfg.WAL.Path == "" {
		return ""
	}
	return e.cfg.WAL.Path + ".checkpoint"
}

func (e *Engine) replayStateLocked(file *os.File, store map[string]storage.Record, since time.Time) error {
	for _, path := range e.walSegmentPathsLocked() {
		segmentFile, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("memory engine: open WAL segment %s: %w", path, err)
		}
		if err := replay(segmentFile, store, since); err != nil {
			_ = segmentFile.Close()
			return fmt.Errorf("memory engine: replay WAL segment %s: %w", path, err)
		}
		if err := segmentFile.Close(); err != nil {
			return fmt.Errorf("memory engine: close WAL segment %s: %w", path, err)
		}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("memory engine: seek WAL start: %w", err)
	}
	if err := replay(file, store, since); err != nil {
		return err
	}
	return nil
}

func (e *Engine) maybeCheckpointLocked(force bool) error {
	if !e.cfg.WAL.Enabled || !e.cfg.CheckpointEnabled || e.cfg.WAL.Path == "" {
		return nil
	}
	if !force && e.cfg.CheckpointSize > 0 {
		if size := e.walSizeLocked(); size < e.cfg.CheckpointSize {
			return nil
		}
	}
	return e.createCheckpointLocked()
}

func (e *Engine) createCheckpointLocked() error {
	if e.checkpoint == nil {
		cp, err := checkpoint.New(e.checkpointPath())
		if err != nil {
			return err
		}
		e.checkpoint = cp
	}
	if err := e.checkpoint.Save(e.store); err != nil {
		return err
	}
	e.checkpointCount++
	e.lastCheckpoint = time.Now().UTC()
	if e.wal != nil {
		if err := e.resetWALLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) resetWALLocked() error {
	if e.wal != nil {
		if err := e.wal.Close(); err != nil {
			return fmt.Errorf("memory engine: close WAL for checkpoint: %w", err)
		}
		e.wal = nil
	}
	file, err := os.OpenFile(e.cfg.WAL.Path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("memory engine: reset WAL: %w", err)
	}
	writer, err := wal.NewWriter(file)
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("memory engine: create WAL writer: %w", err)
	}
	e.wal = writer
	return nil
}

func (e *Engine) rotateWALIfNeededLocked() error {
	if e.wal == nil || e.cfg.WAL.Path == "" || e.cfg.WALMaxSegmentSize <= 0 || e.cfg.WALMaxSegments <= 0 {
		return nil
	}
	if size := e.walSizeLocked(); size < e.cfg.WALMaxSegmentSize {
		return nil
	}
	if err := e.wal.Sync(); err != nil {
		return fmt.Errorf("memory engine: sync WAL before rotation: %w", err)
	}
	if err := e.wal.Close(); err != nil {
		return fmt.Errorf("memory engine: close WAL before rotation: %w", err)
	}
	e.wal = nil
	if err := e.rotateWALFilesLocked(); err != nil {
		return err
	}
	file, err := os.OpenFile(e.cfg.WAL.Path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("memory engine: open rotated WAL: %w", err)
	}
	writer, err := wal.NewWriter(file)
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("memory engine: create rotated WAL writer: %w", err)
	}
	e.wal = writer
	return nil
}

func (e *Engine) rotateWALFilesLocked() error {
	maxSegments := e.cfg.WALMaxSegments
	if maxSegments <= 0 {
		return nil
	}
	for index := maxSegments; index >= 2; index-- {
		src := e.segmentPath(index - 1)
		dst := e.segmentPath(index)
		if _, err := os.Stat(src); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("memory engine: stat WAL segment %s: %w", src, err)
		}
		if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("memory engine: remove WAL segment %s: %w", dst, err)
		}
		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("memory engine: rotate WAL segment %s: %w", src, err)
		}
	}
	if _, err := os.Stat(e.cfg.WAL.Path); err == nil {
		if err := os.Remove(e.segmentPath(1)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("memory engine: remove WAL segment %s: %w", e.segmentPath(1), err)
		}
		if err := os.Rename(e.cfg.WAL.Path, e.segmentPath(1)); err != nil {
			return fmt.Errorf("memory engine: rotate active WAL: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("memory engine: stat active WAL: %w", err)
	}
	return nil
}

func (e *Engine) walSizeLocked() int64 {
	var size int64
	for _, path := range e.walPathsLocked() {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0
		}
		size += info.Size()
	}
	return size
}

func (e *Engine) walPathsLocked() []string {
	paths := make([]string, 0, e.cfg.WALMaxSegments+1)
	if e.cfg.WAL.Path != "" {
		paths = append(paths, e.cfg.WAL.Path)
	}
	for index := 1; index <= e.cfg.WALMaxSegments; index++ {
		paths = append(paths, e.segmentPath(index))
	}
	return paths
}

func (e *Engine) walSegmentPathsLocked() []string {
	paths := make([]string, 0, e.cfg.WALMaxSegments)
	for index := 1; index <= e.cfg.WALMaxSegments; index++ {
		path := e.segmentPath(index)
		if _, err := os.Stat(path); err == nil {
			paths = append(paths, path)
		} else if !os.IsNotExist(err) {
			return nil
		}
	}
	return paths
}

func (e *Engine) segmentPath(index int) string {
	return fmt.Sprintf("%s.%d", e.cfg.WAL.Path, index)
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
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
