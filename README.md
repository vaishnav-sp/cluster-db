# ClusterDB

A distributed key-value database written in Go that provides horizontal scalability, fault tolerance, and eventual consistency through configurable replication, consistent hashing, gossip-based membership, hinted handoff, and read repair.

ClusterDB is designed as a lightweight distributed storage system that automatically routes requests to the correct node, replicates data across the cluster, and tolerates node failures while maintaining high availability.

---

# Features

- Distributed Key-Value Store
- RESTful HTTP API
- Consistent Hash Ring
- Configurable Replication Factor
- Automatic Request Routing
- Replica-to-Replica RPC Communication
- Read Repair
- Hinted Handoff
- Gossip-based Membership
- Write-Ahead Logging (WAL)
- Checkpointing
- In-Memory Storage Engine
- Failure Detection
- Eventual Consistency
- Comprehensive Unit Tests

---

# Technology Stack

| Component | Technology |
|------------|------------|
| Language | Go 1.21+ |
| HTTP API | net/http |
| Inter-node Communication | Custom RPC |
| Storage | In-Memory KV Store |
| Durability | Write-Ahead Log (WAL) |
| Membership | Gossip Protocol |
| Data Distribution | Consistent Hashing |
| Testing | Go Testing Framework |

---

# Architecture

> Architecture diagram will be added here.

ClusterDB consists of multiple nodes connected through a gossip-based membership protocol. Incoming client requests are routed using a consistent hash ring. Writes are replicated to replica nodes, while failed replications are stored using hinted handoff. Divergent replicas are automatically synchronized using read repair.

---

# Implemented Components

## HTTP Gateway

- REST API
- Request validation
- Routing
- Error handling

---

## Cluster Manager

Responsible for

- Membership management
- Node discovery
- Local node identification
- Replica selection
- Failure detection

---

## Consistent Hash Ring

Provides

- Automatic data partitioning
- Minimal key movement
- Primary owner selection
- Replica owner selection

---

## Storage Engine

Each node contains

- Storage Manager
- Memory Engine
- Write-Ahead Log
- Checkpoint Manager

---

## Replication

Supports configurable replication factor.

Example

- RF = 1
- RF = 2
- RF = 3

Replica writes are performed asynchronously using Cluster RPC.

---

## Read Repair

Whenever replicas return different values,

ClusterDB

- Detects stale replicas
- Determines the majority value
- Repairs outdated replicas asynchronously

---

## Hinted Handoff

If a replica is unavailable,

ClusterDB stores the write locally as a hint.

When the failed node comes back online,

the stored hints are replayed automatically.

---

## Gossip Membership

Cluster nodes exchange membership information periodically to

- detect failures
- propagate node status
- maintain an updated cluster view

---

# Repository Structure

```
cluster-db/
├── cmd/
│   └── gateway/
│
├── configs/
│
├── docs/
│   ├── architecture/
│   ├── api/
│   └── images/
│
├── internal/
│   ├── app/
│   ├── cluster/
│   │   ├── gossip/
│   │   ├── handoff/
│   │   ├── hashring/
│   │   ├── rpc/
│   │   └── consistency/
│   │
│   ├── server/
│   │   ├── handlers/
│   │   └── middleware/
│   │
│   └── storage/
│       ├── manager/
│       ├── memory/
│       ├── wal/
│       └── checkpoint/
│
├── go.mod
├── go.sum
└── README.md
```

---

# REST API

| Method | Endpoint | Description |
|---------|----------|-------------|
| PUT | `/v1/kv/{key}` | Store a value |
| GET | `/v1/kv/{key}` | Retrieve a value |
| DELETE | `/v1/kv/{key}` | Delete a value |
| HEAD | `/v1/kv/{key}` | Check whether a key exists |

---

# Running Locally

Clone the repository

```bash
git clone https://github.com/vaishnav-sp/cluster-db.git
```

Move into the project

```bash
cd cluster-db
```

Download dependencies

```bash
go mod tidy
```

Run the gateway

```bash
go run ./cmd/gateway
```

---

# Running Tests

Execute all unit tests

```bash
go test ./...
```

Run handler tests

```bash
go test ./internal/server/handlers -v
```

---

# Fault Tolerance

ClusterDB tolerates failures using

- Configurable Replication
- Read Repair
- Hinted Handoff
- Gossip Membership
- Automatic Request Routing

---

# Storage Pipeline

For every write,

```
Client
    ↓
HTTP Gateway
    ↓
KV Handler
    ↓
Storage Manager
    ↓
Write-Ahead Log
    ↓
Memory Engine
    ↓
Checkpoint
    ↓
Replication
```

---

# Future Improvements

Planned enhancements include

- Persistent disk storage engine
- Compression
- Bloom filters
- Merkle-tree based anti-entropy
- Dynamic cluster rebalancing
- Snapshot streaming
- Metrics and monitoring
- Docker deployment
- Kubernetes support
- Benchmark suite

---

# License

This project is licensed under the MIT License.

---

## Project Status

Current Version

**v1.0**

Implemented

- Distributed KV Store
- Replication
- Consistent Hashing
- Gossip Membership
- Read Repair
- Hinted Handoff
- WAL
- Checkpointing
- Request Routing
- Automated Test Suite