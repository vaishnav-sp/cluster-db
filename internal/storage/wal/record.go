package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
)

const (
	// Magic identifies ClusterDB WAL records on disk.
	Magic uint32 = 0x4344574c
	// Version is the current WAL binary-format version.
	Version uint8 = 1

	fixedHeaderSize = 22 // magic, version, operation, timestamp, key length, value length
	checksumSize    = 4
	maxRecordSize   = 64 << 20
)

// OperationType identifies the mutation represented by a WALRecord.
type OperationType uint8

// Operation is retained as a concise alias for OperationType.
type Operation = OperationType

const (
	// OperationPut writes or replaces a key's value.
	OperationPut OperationType = iota + 1
	// OperationDelete removes a key. Delete records conventionally have no value.
	OperationDelete
)

// Short operation names are provided for call sites that prefer WAL notation.
const (
	PUT    = OperationPut
	DELETE = OperationDelete
)

// WALRecord is one durable storage mutation. Timestamp is stored as Unix
// nanoseconds in UTC. Key and Value are byte slices so the WAL does not impose
// a character encoding on storage keys or values.
type WALRecord struct {
	Operation OperationType
	Timestamp time.Time
	Key       []byte
	Value     []byte
}

// Encode serializes r using the deterministic little-endian WAL format:
// header, key, value, then a CRC32 checksum of every preceding byte.
func (r WALRecord) Encode() ([]byte, error) {
	if !r.Operation.valid() {
		return nil, fmt.Errorf("%w: unknown operation %d", ErrCorruptedRecord, r.Operation)
	}
	if len(r.Key) > maxRecordSize || len(r.Value) > maxRecordSize || len(r.Key)+len(r.Value) > maxRecordSize {
		return nil, fmt.Errorf("%w: record exceeds %d bytes", ErrCorruptedRecord, maxRecordSize)
	}

	size := fixedHeaderSize + len(r.Key) + len(r.Value) + checksumSize
	b := make([]byte, size)
	binary.LittleEndian.PutUint32(b[0:4], Magic)
	b[4] = Version
	b[5] = byte(r.Operation)
	binary.LittleEndian.PutUint64(b[6:14], uint64(r.Timestamp.UnixNano()))
	binary.LittleEndian.PutUint32(b[14:18], uint32(len(r.Key)))
	binary.LittleEndian.PutUint32(b[18:22], uint32(len(r.Value)))
	copy(b[fixedHeaderSize:], r.Key)
	copy(b[fixedHeaderSize+len(r.Key):], r.Value)
	binary.LittleEndian.PutUint32(b[size-checksumSize:], crc32.ChecksumIEEE(b[:size-checksumSize]))

	return b, nil
}

// Decode validates and decodes exactly one encoded WAL record. The returned
// key and value do not share backing storage with data.
func Decode(data []byte) (WALRecord, error) {
	if len(data) < fixedHeaderSize {
		return WALRecord{}, fmt.Errorf("%w: header", ErrUnexpectedEOF)
	}
	if binary.LittleEndian.Uint32(data[0:4]) != Magic {
		return WALRecord{}, ErrInvalidMagic
	}
	if data[4] != Version {
		return WALRecord{}, fmt.Errorf("%w: got %d", ErrInvalidVersion, data[4])
	}
	op := Operation(data[5])
	if !op.valid() {
		return WALRecord{}, fmt.Errorf("%w: unknown operation %d", ErrCorruptedRecord, op)
	}
	keyLen := uint64(binary.LittleEndian.Uint32(data[14:18]))
	valueLen := uint64(binary.LittleEndian.Uint32(data[18:22]))
	payloadLen := keyLen + valueLen
	if payloadLen > maxRecordSize {
		return WALRecord{}, fmt.Errorf("%w: record exceeds %d bytes", ErrCorruptedRecord, maxRecordSize)
	}
	expected := uint64(fixedHeaderSize+checksumSize) + payloadLen
	if uint64(len(data)) < expected {
		return WALRecord{}, fmt.Errorf("%w: payload", ErrUnexpectedEOF)
	}
	if uint64(len(data)) != expected {
		return WALRecord{}, fmt.Errorf("%w: invalid record length", ErrCorruptedRecord)
	}
	stored := binary.LittleEndian.Uint32(data[len(data)-checksumSize:])
	if crc32.ChecksumIEEE(data[:len(data)-checksumSize]) != stored {
		return WALRecord{}, ErrChecksumMismatch
	}
	keyEnd := fixedHeaderSize + int(keyLen)
	return WALRecord{
		Operation: op,
		Timestamp: time.Unix(0, int64(binary.LittleEndian.Uint64(data[6:14]))).UTC(),
		Key:       append([]byte(nil), data[fixedHeaderSize:keyEnd]...),
		Value:     append([]byte(nil), data[keyEnd:len(data)-checksumSize]...),
	}, nil
}

func (o Operation) valid() bool { return o == OperationPut || o == OperationDelete }
