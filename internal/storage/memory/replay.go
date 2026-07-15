package memory

import (
	"fmt"
	"io"

	"github.com/vaishnav-sp/cluster-db/internal/storage"
	"github.com/vaishnav-sp/cluster-db/internal/storage/wal"
)

// replay reads WAL records in order and applies them to store. It performs no
// recovery or repair: any reader error aborts engine startup.
func replay(source io.Reader, store map[string]storage.Record) error {
	reader, err := wal.NewReader(source)
	if err != nil {
		return fmt.Errorf("create reader: %w", err)
	}
	for reader.Next() {
		record := reader.Record()
		key := string(record.Key)
		switch record.Operation {
		case wal.OperationPut:
			createdAt := record.Timestamp
			if existing, ok := store[key]; ok {
				createdAt = existing.Metadata.CreatedAt
			}
			store[key] = storage.Record{
				Key:   append(storage.Key(nil), record.Key...),
				Value: append(storage.Value(nil), record.Value...),
				Metadata: storage.Metadata{
					CreatedAt: createdAt,
					UpdatedAt: record.Timestamp,
				},
			}
		case wal.OperationDelete:
			delete(store, key)
		default:
			return fmt.Errorf("unknown WAL operation %d", record.Operation)
		}
	}
	if err := reader.Error(); err != nil {
		return fmt.Errorf("read record: %w", err)
	}
	return nil
}
