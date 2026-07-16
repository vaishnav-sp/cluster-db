package checkpoint

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/vaishnav-sp/cluster-db/internal/storage"
)

func TestCheckpointSaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state", "checkpoint.bin")
	cp, err := New(path)
	if err != nil {
		t.Fatalf("new checkpoint: %v", err)
	}
	state := map[string]storage.Record{
		"a": {Key: []byte("a"), Value: []byte("1")},
	}
	if err := cp.Save(state); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("checkpoint file missing: %v", err)
	}
	loaded, err := cp.Load()
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("loaded records = %d, want 1", len(loaded))
	}
	if string(loaded["a"].Value) != "1" {
		t.Fatalf("loaded value = %q", loaded["a"].Value)
	}
}
