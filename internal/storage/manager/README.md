# manager — Storage Manager

The `manager` package is the **single integration point** between the application and the storage subsystem. The application never imports concrete engine packages — it interacts exclusively with this manager.

---

## Responsibilities

| Concern | Where |
|---------|-------|
| Engine construction | `factory.go` → `Build()` |
| Lifecycle (Open / Close) | `manager.go` → `Manager.Open()` / `Manager.Close()` |
| I/O delegation | `manager.go` → `Put`, `Get`, `Delete`, `Exists`, `Scan`, `Stats`, `Health` |
| Error context | Every method wraps engine errors with `"storage manager: <op>: %w"` |
| Lifecycle logging | Zap structured logs on Open / Close |

---

## Dependency Graph

```
Application
    └── manager.Manager        ← only import the app needs
            ├── storage.Engine  (interface — no concrete type escapes)
            └── factory.Build()
                    └── memory.Engine  ← only imported here
```

---

## Factory Pattern

`factory.go` contains the **only** import of any concrete engine package in the entire codebase. Adding a new engine requires:

1. Implement `storage.Engine` in a new sub-package (e.g. `internal/storage/badger`)
2. Add a `case "badger":` branch in `Build()`
3. Set `storage.engine: badger` in config

Zero other files change.

---

## Configuration

```yaml
storage:
  engine: memory       # Supported: memory | badger (planned) | pebble (planned)
  data_directory: ./data
```

| Engine | Status | Use case |
|--------|--------|----------|
| `memory` | ✅ | Unit tests, local development |
| `badger` | 🔜 | Single-node embedded persistence |
| `pebble` | 🔜 | High-throughput production |
| `rocksdb` | 🔜 | Production via cgo |

---

## Usage

```go
mgr, err := manager.New(cfg.Storage, logger)
if err != nil { /* ErrUnknownEngine if cfg.Engine is invalid */ }

// Open during application startup
if err := mgr.Open(ctx); err != nil { ... }
defer mgr.Close(ctx)

// Write
err = mgr.Put(ctx, storage.Record{
    Key:   storage.Key("users/alice"),
    Value: storage.Value(`{"name":"alice"}`),
})

// Read
rec, err := mgr.Get(ctx, storage.Key("users/alice"))
if errors.Is(err, storage.ErrKeyNotFound) { ... }
```

---

## Error Handling

Manager errors wrap storage-layer errors, enabling fine-grained `errors.Is` checks:

```go
rec, err := mgr.Get(ctx, key)
if errors.Is(err, storage.ErrKeyNotFound) {
    // key missing
}
if errors.Is(err, manager.ErrManagerNotOpen) {
    // lifecycle violation
}
```
