# storage — ClusterDB Storage Abstraction Layer

This package defines the **pluggable storage engine contract** for ClusterDB. It contains no implementation logic — only interfaces, data models, and sentinel errors that every storage backend must satisfy.

---

## Architecture

```
internal/storage/
├── engine.go        # Engine + Iterator interfaces
├── types.go         # Key, Value, Record, Metadata, Stats, Health, ScanOptions
├── errors.go        # Sentinel errors
├── README.md
└── memory/
    ├── engine.go    # Thread-safe in-memory Engine (reference implementation)
    └── README.md
```

### Layered responsibilities

| Layer | Responsibility |
|-------|---------------|
| `storage` package | Contracts, models, errors — zero runtime logic |
| Engine implementation | Lifecycle, I/O, concurrency safety |
| Caller (server, replication) | Context deadlines, retry logic, error handling |

---

## Engine Contract

Every engine must implement `storage.Engine`:

```go
type Engine interface {
    Open(ctx context.Context) error
    Close(ctx context.Context) error
    Put(ctx context.Context, rec Record) error
    Get(ctx context.Context, key Key) (Record, error)
    Delete(ctx context.Context, key Key) error
    Exists(ctx context.Context, key Key) (bool, error)
    Scan(ctx context.Context, opts ScanOptions) (Iterator, error)
    Stats(ctx context.Context) (Stats, error)
    Health(ctx context.Context) (Health, error)
}
```

**Lifecycle invariants:**
1. `Open` must be called before any I/O method.
2. After `Close`, all I/O methods return `ErrEngineClosed`.
3. `Health` must never block for more than a few milliseconds.
4. `Scan` callers must always `defer iter.Close()`.

---

## Data Model

```
Record
├── Key      []byte          — engine-agnostic byte key
├── Value    []byte          — opaque payload blob
└── Metadata
    ├── TTL          time.Duration  — 0 = no expiry
    ├── Version      uint64         — logical clock for CAS
    ├── CreatedAt    time.Time
    ├── UpdatedAt    time.Time
    └── DeleteMarker bool           — MVCC tombstone flag
```

`Record` is the **atomic unit of storage**. Engines always return a complete `Record` so callers never observe split state between a value and its metadata.

---

## Error Handling

All sentinel errors live in `errors.go` and are compatible with `errors.Is`:

| Error | Condition |
|-------|-----------|
| `ErrKeyNotFound` | Key absent in the engine |
| `ErrEngineClosed` | Operation after `Close()` |
| `ErrEngineNotOpen` | Operation before `Open()` |
| `ErrInvalidKey` | Nil or zero-length key |
| `ErrNilValue` | Nil value passed to `Put` |
| `ErrIteratorClosed` | Iterator method after `Close()` |
| `ErrScanFinished` | `Next()` called on exhausted iterator |

---

## Future Implementations

| Engine | Status | Notes |
|--------|--------|-------|
| `memory` | ✅ Available | Reference impl; unit tests only |
| `badger` | 🔜 Planned | Embedded LSM; single-node persistence |
| `pebble` | 🔜 Planned | CockroachDB's storage layer; high throughput |
| `rocksdb` | 🔜 Planned | Via cgo; production-grade LSM |

All future engines must:
- Pass `var _ storage.Engine = (*Engine)(nil)` compile-time assertion
- Be registered via the engine factory (TBD)
- Expose metrics via `Stats()`

---

## Testing

Use `memory.Engine` as a drop-in test double:

```go
func TestMyFeature(t *testing.T) {
    eng := memory.NewEngine()
    if err := eng.Open(context.Background()); err != nil {
        t.Fatal(err)
    }
    defer eng.Close(context.Background())

    // eng satisfies storage.Engine — use it anywhere a real engine is expected
}
```
