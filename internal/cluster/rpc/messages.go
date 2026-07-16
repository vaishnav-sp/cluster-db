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
