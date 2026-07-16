# memory — In-Memory Storage Engine

An in-memory implementation of `storage.Engine` backed by a Go `map` protected by `sync.RWMutex`. This engine is the **reference implementation** for ClusterDB and is intended exclusively for unit testing and local development.

---

## Characteristics

| Property | Value |
|----------|-------|
| Persistence | ❌ None — all data is lost on process exit |
| Thread safety | ✅ `sync.RWMutex` (concurrent reads, exclusive writes) |
| WAL | Optional append-only persistence and replay |
| Snapshots | ❌ Not applicable |
| Replication | ❌ Not applicable |
| Scan order | Ascending lexicographic (or descending with `Reverse: true`) |
| External deps | ❌ None beyond the standard library |

---

## Usage

```go
import (
    "context"
    "github.com/vaishnav-sp/cluster-db/internal/storage"
    "github.com/vaishnav-sp/cluster-db/internal/storage/memory"
)

eng := memory.NewEngine()

ctx := context.Background()
if err := eng.Open(ctx); err != nil {
    log.Fatal(err)
}
defer eng.Close(ctx)

// Write
err := eng.Put(ctx, storage.Record{
    Key:   storage.Key("hello"),
    Value: storage.Value("world"),
})

// Read
rec, err := eng.Get(ctx, storage.Key("hello"))

// Scan with prefix
iter, err := eng.Scan(ctx, storage.ScanOptions{
    Prefix: storage.Key("hel"),
    Limit:  100,
})
defer iter.Close()

for iter.Next() {
    rec := iter.Record()
    fmt.Printf("%s → %s\n", rec.Key, rec.Value)
}
if err := iter.Error(); err != nil {
    log.Fatal(err)
}
```

---

## WAL persistence

The memory engine can optionally preserve mutations in an append-only WAL.
The storage manager passes `storage.wal.enabled`, `storage.wal.path`, and
`storage.wal.sync_on_write` into the engine. Each mutation is written to WAL
before the map is changed. On open, records are replayed in order; corruption
causes startup to fail rather than serving incomplete state.

```go
eng := memory.NewEngine(memory.Config{WAL: memory.WALConfig{
    Enabled: true,
    Path: "./data/clusterdb.wal",
    SyncOnWrite: true,
}})
```

## Implementation Notes

### Defensive copies
`Put` stores deep copies of both `Key` and `Value`. This prevents callers from accidentally mutating stored state through aliased byte slices — a common source of subtle bugs in high-throughput systems.

### Scan materialisation
`Scan` collects all matching records under a read-lock and hands them to a slice-backed `iterator`. There are no goroutines or channels involved. This keeps the implementation simple and makes iterator behaviour fully deterministic in tests.

### CreatedAt preservation
On an overwrite `Put`, `Metadata.CreatedAt` is preserved from the existing record. `Metadata.UpdatedAt` is always set to `time.Now()`.

---

## Limitations

- **No TTL enforcement** — `Metadata.TTL` is stored but not acted upon. The caller is responsible for expiry logic if needed at this level.
- **No atomicity across multiple keys** — there is no multi-key transaction support. Each operation is independently atomic.
- **Memory-only** — unsuitable for datasets that exceed available RAM.

---

## Testing

This engine is the recommended test double for any component that depends on `storage.Engine`:

```go
func setupEngine(t *testing.T) storage.Engine {
    t.Helper()

    eng := memory.NewEngine()
    if err := eng.Open(context.Background()); err != nil {
        t.Fatalf("open engine: %v", err)
    }

    t.Cleanup(func() {
        _ = eng.Close(context.Background())
    })

    return eng
}
```

### Checkpoint and WAL lifecycle

With checkpointing enabled, the engine periodically checks the total WAL size.
Once it reaches `storage.checkpoint_size`, it writes a complete, sorted snapshot
to `<wal path>.checkpoint`, synchronizes it, resets the active WAL, and removes
obsolete rotated segments. WAL files rotate into numbered segments when
`storage.wal_max_segment_size` is reached; `storage.wal_max_segments` bounds
the number retained before a checkpoint compacts them.

On startup, recovery first loads the checkpoint, then replays numbered WAL
segments in order, and finally replays the active WAL. A corrupt checkpoint or
segment stops startup rather than exposing incomplete state. Background
maintenance performs the same size check at `storage.checkpoint_interval` and
is stopped before the WAL is closed.
