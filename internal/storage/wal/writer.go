package wal

import (
	"fmt"
	"io"
	"sync"
)

// Writer appends complete WAL records to an underlying stream. Its methods are
// safe for concurrent use. For crash durability, use a file as the destination
// and call Sync after the append whose durability is required.
type Writer struct {
	mu     sync.Mutex
	w      io.Writer
	syncer interface{ Sync() error }
	closer io.Closer
	closed bool
}

// Compile-time interface assertion.
var _ io.Closer = (*Writer)(nil)

// NewWriter creates a Writer that appends encoded records to w. If w supports
// Sync, Sync delegates to it; otherwise Sync flushes any buffered writer that
// implements Flush.
func NewWriter(w io.Writer) (*Writer, error) {
	if w == nil {
		return nil, fmt.Errorf("wal: new writer: nil destination")
	}
	writer := &Writer{w: w}
	if s, ok := w.(interface{ Sync() error }); ok {
		writer.syncer = s
	}
	if c, ok := w.(io.Closer); ok {
		writer.closer = c
	}
	return writer, nil
}

// Append encodes rec and writes it as one serialized operation.
func (w *Writer) Append(rec WALRecord) error {
	b, err := rec.Encode()
	if err != nil {
		return fmt.Errorf("wal: append: encode record: %w", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if err := writeFull(w.w, b); err != nil {
		return fmt.Errorf("wal: append: write record: %w", err)
	}
	return nil
}

// Sync flushes buffered data and, when supported by the destination, requests
// that the operating system synchronize it to stable storage.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if f, ok := w.w.(interface{ Flush() error }); ok {
		if err := f.Flush(); err != nil {
			return fmt.Errorf("wal: sync: flush: %w", err)
		}
	}
	if w.syncer != nil {
		if err := w.syncer.Sync(); err != nil {
			return fmt.Errorf("wal: sync: fsync: %w", err)
		}
	}
	return nil
}

// Close synchronizes and closes the underlying stream when it implements
// io.Closer. It is idempotent.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	if f, ok := w.w.(interface{ Flush() error }); ok {
		if err := f.Flush(); err != nil {
			return fmt.Errorf("wal: close: flush: %w", err)
		}
	}
	if w.syncer != nil {
		if err := w.syncer.Sync(); err != nil {
			return fmt.Errorf("wal: close: fsync: %w", err)
		}
	}
	if w.closer != nil {
		if err := w.closer.Close(); err != nil {
			return fmt.Errorf("wal: close: close destination: %w", err)
		}
	}
	w.closed = true
	return nil
}

func writeFull(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
