package memory

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vaishnav-sp/cluster-db/internal/storage"
	"github.com/vaishnav-sp/cluster-db/internal/storage/wal"
)

func TestWALRecoveryAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.wal")
	cfg := Config{WAL: WALConfig{Enabled: true, Path: path, SyncOnWrite: true}}
	first := NewEngine(cfg)
	if err := first.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := first.Put(context.Background(), storage.Record{Key: []byte("one"), Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if err := first.Put(context.Background(), storage.Record{Key: []byte("two"), Value: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	second := NewEngine(cfg)
	if err := second.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close(context.Background()) })
	for key, want := range map[string]string{"one": "1", "two": "2"} {
		record, err := second.Get(context.Background(), []byte(key))
		if err != nil || string(record.Value) != want {
			t.Fatalf("Get(%q) = %q, %v", key, record.Value, err)
		}
	}
}

func TestWALReplayOrderAndDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.wal")
	cfg := Config{WAL: WALConfig{Enabled: true, Path: path}}
	engine := NewEngine(cfg)
	if err := engine.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(context.Background(), storage.Record{Key: []byte("key"), Value: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(context.Background(), storage.Record{Key: []byte("key"), Value: []byte("last")}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(context.Background(), storage.Record{Key: []byte("deleted"), Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Delete(context.Background(), []byte("deleted")); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	reopened := NewEngine(cfg)
	if err := reopened.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close(context.Background()) })
	record, err := reopened.Get(context.Background(), []byte("key"))
	if err != nil || string(record.Value) != "last" {
		t.Fatalf("ordered replay = %q, %v", record.Value, err)
	}
	if _, err := reopened.Get(context.Background(), []byte("deleted")); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("deleted record error = %v", err)
	}
}

func TestWALCorruptionAbortsOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.wal")
	encoded, err := (wal.WALRecord{Operation: wal.OperationPut, Key: []byte("key"), Value: []byte("value")}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] ^= 1
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(Config{WAL: WALConfig{Enabled: true, Path: path}})
	if err := engine.Open(context.Background()); !errors.Is(err, wal.ErrChecksumMismatch) {
		t.Fatalf("open error = %v", err)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func TestWALAppendFailureDoesNotMutateMemory(t *testing.T) {
	engine := NewEngine()
	if err := engine.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close(context.Background()) })
	writer, err := wal.NewWriter(failingWriter{})
	if err != nil {
		t.Fatal(err)
	}
	engine.wal = writer
	if err := engine.Put(context.Background(), storage.Record{Key: []byte("key"), Value: []byte("value")}); err == nil {
		t.Fatal("Put succeeded with failed WAL append")
	}
	if _, err := engine.Get(context.Background(), []byte("key")); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("record unexpectedly stored: %v", err)
	}
}

type syncSpy struct {
	bytes.Buffer
	calls int
}

func (s *syncSpy) Sync() error { s.calls++; return nil }

func TestWALSyncOnWrite(t *testing.T) {
	for _, test := range []struct {
		name        string
		syncOnWrite bool
		want        int
	}{
		{name: "enabled", syncOnWrite: true, want: 1},
		{name: "disabled", syncOnWrite: false, want: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine := NewEngine(Config{WAL: WALConfig{SyncOnWrite: test.syncOnWrite}})
			if err := engine.Open(context.Background()); err != nil {
				t.Fatal(err)
			}
			spy := &syncSpy{}
			writer, _ := wal.NewWriter(spy)
			engine.wal = writer
			if err := engine.Put(context.Background(), storage.Record{Key: []byte("key"), Value: []byte("value")}); err != nil {
				t.Fatal(err)
			}
			if spy.calls != test.want {
				t.Fatalf("sync calls = %d, want %d", spy.calls, test.want)
			}
			engine.wal = nil // the test-owned writer does not need engine cleanup.
			_ = engine.Close(context.Background())
		})
	}
}

func TestCheckpointCreationAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.wal")
	cfg := Config{WAL: WALConfig{Enabled: true, Path: path}, CheckpointEnabled: true, CheckpointSize: 1}
	engine := NewEngine(cfg)
	if err := engine.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(context.Background(), storage.Record{Key: []byte("checkpoint"), Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".checkpoint"); err != nil {
		t.Fatalf("checkpoint not created: %v", err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	reopened := NewEngine(cfg)
	if err := reopened.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close(context.Background()) })
	record, err := reopened.Get(context.Background(), []byte("checkpoint"))
	if err != nil || string(record.Value) != "value" {
		t.Fatalf("checkpoint recovery = %q, %v", record.Value, err)
	}
}

func TestReplayAfterCheckpointAndDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.wal")
	cfg := Config{WAL: WALConfig{Enabled: true, Path: path}, CheckpointEnabled: true, CheckpointSize: 1}
	engine := NewEngine(cfg)
	if err := engine.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(context.Background(), storage.Record{Key: []byte("kept"), Value: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	engine.cfg.CheckpointSize = 1 << 20
	if err := engine.Put(context.Background(), storage.Record{Key: []byte("later"), Value: []byte("two")}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Delete(context.Background(), []byte("kept")); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	reopened := NewEngine(cfg)
	if err := reopened.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close(context.Background()) })
	if _, err := reopened.Get(context.Background(), []byte("kept")); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("deleted checkpoint record = %v", err)
	}
	record, err := reopened.Get(context.Background(), []byte("later"))
	if err != nil || string(record.Value) != "two" {
		t.Fatalf("WAL after checkpoint = %q, %v", record.Value, err)
	}
}

func TestWALRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.wal")
	cfg := Config{WAL: WALConfig{Enabled: true, Path: path}, WALMaxSegmentSize: 1, WALMaxSegments: 4}
	engine := NewEngine(cfg)
	if err := engine.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(context.Background(), storage.Record{Key: []byte("first"), Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(context.Background(), storage.Record{Key: []byte("second"), Value: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".000001"); err != nil {
		t.Fatalf("rotated segment not created: %v", err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	reopened := NewEngine(cfg)
	if err := reopened.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close(context.Background()) })
	for key, want := range map[string]string{"first": "1", "second": "2"} {
		record, err := reopened.Get(context.Background(), []byte(key))
		if err != nil || string(record.Value) != want {
			t.Fatalf("rotation recovery %q = %q, %v", key, record.Value, err)
		}
	}
}

func TestBackgroundCheckpointExecution(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.wal")
	cfg := Config{WAL: WALConfig{Enabled: true, Path: path}, CheckpointEnabled: true, CheckpointInterval: 10 * time.Millisecond, CheckpointSize: 1}
	engine := NewEngine(cfg)
	if err := engine.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close(context.Background()) })
	if err := engine.Put(context.Background(), storage.Record{Key: []byte("key"), Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats, err := engine.Stats(context.Background())
		if err == nil && stats.CheckpointCount > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("background checkpoint did not run")
}
