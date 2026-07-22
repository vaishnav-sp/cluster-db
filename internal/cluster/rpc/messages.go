package rpc

import "time"

// HeartbeatRequest represents a heartbeat message between cluster nodes.
type HeartbeatRequest struct {
	NodeID    string    `json:"node_id"`
	Address   string    `json:"address"`
	Timestamp time.Time `json:"timestamp"`
}

// HeartbeatResponse represents the response to a heartbeat message.
type HeartbeatResponse struct {
	Accepted bool   `json:"accepted"`
	Message  string `json:"message,omitempty"`
}

// JoinRequest represents a request for a node to join the cluster.
type JoinRequest struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

// MemberInfo describes a cluster member in a membership snapshot.
type MemberInfo struct {
	ID       string `json:"id"`
	Address  string `json:"address"`
	Status   string `json:"status"`
	IsLeader bool   `json:"is_leader"`
}

// JoinResponse represents the response to a join request.
type JoinResponse struct {
	Accepted bool         `json:"accepted"`
	Message  string       `json:"message,omitempty"`
	Members  []MemberInfo `json:"members,omitempty"`
}

// LeaveRequest represents a request for a node to leave the cluster.
type LeaveRequest struct {
	NodeID string `json:"node_id"`
}

// LeaveResponse represents the response to a leave request.
type LeaveResponse struct {
	Accepted bool   `json:"accepted"`
	Message  string `json:"message,omitempty"`
}

// AppendEntriesRequest represents replication log entries from a leader.
type AppendEntriesRequest struct {
	LeaderID string   `json:"leader_id"`
	Term     int64    `json:"term"`
	Entries  []string `json:"entries,omitempty"`
}

// AppendEntriesResponse represents the response to an append entries request.
type AppendEntriesResponse struct {
	Accepted bool   `json:"accepted"`
	Message  string `json:"message,omitempty"`
}

// KVGetRequest represents a request to get a key.
type KVGetRequest struct {
	Key string `json:"key"`
}

// KVGetResponse represents the response to a key get request.
type KVGetResponse struct {
	Value []byte `json:"value,omitempty"`
	Found bool   `json:"found"`
	Error string `json:"error,omitempty"`
}

// KVPutRequest represents a request to write a key.
type KVPutRequest struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// KVPutResponse represents the response to a key write request.
type KVPutResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// KVDeleteRequest represents a request to delete a key.
type KVDeleteRequest struct {
	Key string `json:"key"`
}

// KVDeleteResponse represents the response to a key delete request.
type KVDeleteResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// ReplicaPutRequest represents a request to write a replicated key on a replica.
type ReplicaPutRequest struct {
	Key     string `json:"key"`
	Value   []byte `json:"value"`
	Version uint64 `json:"version,omitempty"`
}

// ReplicaPutResponse represents the response to a replicated key write request.
type ReplicaPutResponse struct {
	Success bool   `json:"success"`
	Version uint64 `json:"version,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ReplicaDeleteRequest represents a request to delete a replicated key on a replica.
type ReplicaDeleteRequest struct {
	Key     string `json:"key"`
	Version uint64 `json:"version,omitempty"`
}

// ReplicaDeleteResponse represents the response to a replicated key delete request.
type ReplicaDeleteResponse struct {
	Success bool   `json:"success"`
	Version uint64 `json:"version,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ReplicaGetRequest represents a request to read a key directly from local storage on a replica.
// No routing or forwarding is performed; the handler reads its own store only.
type ReplicaGetRequest struct {
	Key string `json:"key"`
}

// ReplicaGetResponse represents the response to a replica read request.
type ReplicaGetResponse struct {
	Found        bool   `json:"found"`
	Value        []byte `json:"value,omitempty"`
	Version      uint64 `json:"version,omitempty"`
	DeleteMarker bool   `json:"delete_marker,omitempty"`
	Error        string `json:"error,omitempty"`
}

// GossipNodeInfo describes a node's membership state in a gossip payload.
type GossipNodeInfo struct {
	ID            string    `json:"id"`
	Address       string    `json:"address"`
	Status        string    `json:"status"`
	Version       string    `json:"version"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

// GossipRequest represents a gossip state exchange request.
type GossipRequest struct {
	SenderID   string           `json:"sender_id"`
	SenderAddr string           `json:"sender_addr"`
	Nodes      []GossipNodeInfo `json:"nodes"`
}

// GossipResponse represents the response to a gossip state exchange request.
type GossipResponse struct {
	Accepted bool             `json:"accepted"`
	Message  string           `json:"message,omitempty"`
	Nodes    []GossipNodeInfo `json:"nodes,omitempty"`
}
