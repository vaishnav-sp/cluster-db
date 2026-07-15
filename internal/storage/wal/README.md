# Write-Ahead Log

This package provides ClusterDB's standalone, append-only binary write-ahead
log (WAL). A future storage engine will append a mutation, synchronize it when
the requested durability policy requires it, and only then apply the mutation
to its in-memory state.

## Binary format

All integer fields use little-endian encoding. A record has no alignment or
padding and is followed immediately by the next record.

```
0                 4 5 6              14       18       22
+-----------------+-+-+-+-+-+-+-+-+----------------+--------+-------+
| magic (uint32)  | version | op    | timestamp ns   | keyLen | valLen|
+-----------------+-+-+-+-+-+-+-+-+----------------+--------+-------+
| key bytes                       | value bytes              |
+---------------------------------+--------------------------+
| CRC32 IEEE of every preceding byte in this record (uint32) |
+-------------------------------------------------------------+
```

The magic number and version make accidental or incompatible input fail fast.
The CRC32 covers the header and payload. `Reader.Next` validates it before
exposing a record; a malformed, truncated, or checksum-mismatched record
stops iteration with a typed error.

## Crash consistency and future recovery

`Writer.Append` serializes concurrent appends. `Writer.Sync` flushes buffered
destinations and calls `Sync` on files, which is the durability boundary for a
future engine. On startup, recovery will read records sequentially and replay
valid PUT and DELETE operations into the storage engine. This sprint performs
no replay, tail repair, compaction, or integration with the storage manager.
