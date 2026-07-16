package checkpoint

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/vaishnav-sp/cluster-db/internal/storage"
)

// Checkpoint persists a snapshot of the engine state to disk.
type Checkpoint struct {
	path string
}

// New creates a checkpoint handle for path.
func New(path string) (*Checkpoint, error) {
	if path == "" {
		return nil, fmt.Errorf("checkpoint: empty path")
	}
	cp := &Checkpoint{path: path}
	if err := cp.ensureDir(); err != nil {
		return nil, err
	}
	return cp, nil
}

// Path returns the checkpoint file path.
func (c *Checkpoint) Path() string { return c.path }

// Save writes the state to disk atomically.
func (c *Checkpoint) Save(state map[string]storage.Record) error {
	if c == nil || c.path == "" {
		return fmt.Errorf("checkpoint: save: empty path")
	}
	if err := c.ensureDir(); err != nil {
		return err
	}

	snapshot := checkpointData{Records: state, Timestamp: time.Now().UTC()}
	tempFile, err := os.CreateTemp(filepath.Dir(c.path), filepath.Base(c.path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("checkpoint: create temp file: %w", err)
	}
	tempPath := tempFile.Name()

	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
	}()

	encoder := gob.NewEncoder(tempFile)
	if err := encoder.Encode(snapshot); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("checkpoint: encode snapshot: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("checkpoint: sync temp file: %w", err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("checkpoint: close temp file: %w", err)
	}
	if err := replaceFile(tempPath, c.path); err != nil {
		return fmt.Errorf("checkpoint: rename temp file: %w", err)
	}
	if err := syncDir(filepath.Dir(c.path)); err != nil {
		return fmt.Errorf("checkpoint: sync dir: %w", err)
	}
	return nil
}

// Load reads the checkpoint from disk.
func (c *Checkpoint) Load() (map[string]storage.Record, error) {
	if c == nil || c.path == "" {
		return nil, fmt.Errorf("checkpoint: load: empty path")
	}
	file, err := os.Open(c.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		}
		return nil, fmt.Errorf("checkpoint: open file: %w", err)
	}
	defer file.Close()

	var snapshot checkpointData
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("checkpoint: decode snapshot: %w", err)
	}
	return snapshot.Records, nil
}

// LoadWithTimestamp loads the checkpoint and the timestamp that was written.
func (c *Checkpoint) LoadWithTimestamp() (map[string]storage.Record, time.Time, error) {
	if c == nil || c.path == "" {
		return nil, time.Time{}, fmt.Errorf("checkpoint: load: empty path")
	}
	file, err := os.Open(c.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, time.Time{}, err
		}
		return nil, time.Time{}, fmt.Errorf("checkpoint: open file: %w", err)
	}
	defer file.Close()

	var snapshot checkpointData
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&snapshot); err != nil {
		return nil, time.Time{}, fmt.Errorf("checkpoint: decode snapshot: %w", err)
	}
	return snapshot.Records, snapshot.Timestamp, nil
}

func (c *Checkpoint) ensureDir() error {
	if dir := filepath.Dir(c.path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("checkpoint: create dir: %w", err)
		}
	}
	return nil
}

func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func replaceFile(src, dst string) error {
	if runtime.GOOS == "windows" {
		if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return os.Rename(src, dst)
}

type checkpointData struct {
	Records   map[string]storage.Record
	Timestamp time.Time
}
