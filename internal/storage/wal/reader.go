package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Reader sequentially reads and validates WAL records. Next returns false at
// clean EOF or after an error; inspect Error to distinguish the two cases.
type Reader struct {
	r        io.Reader
	closer   io.Closer
	record   WALRecord
	err      error
	closed   bool
	finished bool
}

// Compile-time interface assertion.
var _ io.Closer = (*Reader)(nil)

// NewReader creates a Reader for r. A nil reader is rejected.
func NewReader(r io.Reader) (*Reader, error) {
	if r == nil {
		return nil, fmt.Errorf("wal: new reader: nil source")
	}
	reader := &Reader{r: r}
	if c, ok := r.(io.Closer); ok {
		reader.closer = c
	}
	return reader, nil
}

// Next advances to the next validated record. It returns false at clean EOF,
// after Close, or after the first decoding error.
func (r *Reader) Next() bool {
	if r.closed {
		if r.err == nil {
			r.err = ErrClosed
		}
		return false
	}
	if r.finished || r.err != nil {
		return false
	}

	var header [fixedHeaderSize]byte
	n, err := io.ReadFull(r.r, header[:])
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			r.finished = true
			return false
		}
		r.err = fmt.Errorf("wal: read record header: %w", ErrUnexpectedEOF)
		return false
	}
	if binary.LittleEndian.Uint32(header[0:4]) != Magic {
		r.err = ErrInvalidMagic
		return false
	}
	if header[4] != Version {
		r.err = fmt.Errorf("%w: got %d", ErrInvalidVersion, header[4])
		return false
	}
	if !Operation(header[5]).valid() {
		r.err = fmt.Errorf("%w: unknown operation %d", ErrCorruptedRecord, header[5])
		return false
	}
	length := uint64(binary.LittleEndian.Uint32(header[14:18])) + uint64(binary.LittleEndian.Uint32(header[18:22]))
	if length > maxRecordSize {
		r.err = fmt.Errorf("%w: record exceeds %d bytes", ErrCorruptedRecord, maxRecordSize)
		return false
	}

	data := make([]byte, fixedHeaderSize+int(length)+checksumSize)
	copy(data, header[:])
	if _, err := io.ReadFull(r.r, data[fixedHeaderSize:]); err != nil {
		r.err = fmt.Errorf("wal: read record body: %w", ErrUnexpectedEOF)
		return false
	}
	rec, err := Decode(data)
	if err != nil {
		r.err = fmt.Errorf("wal: decode record: %w", err)
		return false
	}
	r.record = rec
	return true
}

// Record returns the record yielded by the most recent successful Next call.
// The returned byte slices remain valid until the next successful Next call.
func (r *Reader) Record() WALRecord { return r.record }

// Error returns the first non-EOF error encountered while reading.
func (r *Reader) Error() error { return r.err }

// Close releases the underlying stream if it implements io.Closer. It is
// idempotent. Calling Next after Close causes Error to return ErrClosed.
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.closer != nil {
		if err := r.closer.Close(); err != nil {
			return fmt.Errorf("wal: close reader: %w", err)
		}
	}
	return nil
}
