package config

import (
	"path/filepath"
	"testing"
)

func TestLoadDevelopmentWALConfiguration(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	t.Setenv("APP_ENV", "development")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Storage.WAL.Enabled || cfg.Storage.WAL.Path != "./data/clusterdb.wal" || cfg.Storage.WAL.SyncOnWrite {
		t.Fatalf("development WAL config = %#v", cfg.Storage.WAL)
	}
}

func TestLoadWALConfigurationFromEnvironment(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	t.Setenv("APP_ENV", "development")
	t.Setenv("CLUSTERDB_STORAGE_WAL_ENABLED", "false")
	t.Setenv("CLUSTERDB_STORAGE_WAL_PATH", "test.wal")
	t.Setenv("CLUSTERDB_STORAGE_WAL_SYNC_ON_WRITE", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Storage.WAL.Enabled || cfg.Storage.WAL.Path != "test.wal" || !cfg.Storage.WAL.SyncOnWrite {
		t.Fatalf("environment WAL config = %#v", cfg.Storage.WAL)
	}
}
