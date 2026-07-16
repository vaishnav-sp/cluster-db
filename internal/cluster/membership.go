package cluster

import (
	"fmt"
	"sync"
	"time"
)

// Status represents the health state of a cluster node.
type Status int

const (
	// Alive indicates a healthy node.
	Alive Status = iota + 1
	// Suspect indicates a node that may be unhealthy.
	Suspect
	// Dead indicates a node that is no longer considered healthy.
	Dead
)

// Node describes a single cluster member.
type Node struct {
	ID            string
	Address       string
	Status        Status
	LastHeartbeat time.Time
	IsLeader      bool
	Version       string
}

// Membership manages the in-memory cluster membership state.
type Membership struct {
	mu    sync.RWMutex
	nodes map[string]Node
}

// NewMembership creates an empty membership manager.
func NewMembership() *Membership {
	return &Membership{nodes: make(map[string]Node)}
}

// AddNode inserts or replaces a node in the membership list.
func (m *Membership) AddNode(node Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.ID] = node
}

// RemoveNode deletes a node by id.
func (m *Membership) RemoveNode(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, id)
}

// GetNode returns a node by id if it exists.
func (m *Membership) GetNode(id string) (Node, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	node, ok := m.nodes[id]
	return node, ok
}

// ListNodes returns a snapshot of all nodes.
func (m *Membership) ListNodes() []Node {
	m.mu.RLock()
	defer m.mu.RUnlock()
	nodes := make([]Node, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// UpdateHeartbeat refreshes the heartbeat timestamp for a node.
func (m *Membership) UpdateHeartbeat(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	node, ok := m.nodes[id]
	if !ok {
		return fmt.Errorf("cluster: node %q not found", id)
	}
	node.LastHeartbeat = time.Now().UTC()
	node.Status = Alive
	m.nodes[id] = node
	return nil
}

// MarkSuspect marks a node as suspect.
func (m *Membership) MarkSuspect(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	node, ok := m.nodes[id]
	if !ok {
		return fmt.Errorf("cluster: node %q not found", id)
	}
	node.Status = Suspect
	m.nodes[id] = node
	return nil
}

// MarkDead marks a node as dead.
func (m *Membership) MarkDead(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	node, ok := m.nodes[id]
	if !ok {
		return fmt.Errorf("cluster: node %q not found", id)
	}
	node.Status = Dead
	m.nodes[id] = node
	return nil
}

// Leader returns the current leader node, if one exists.
func (m *Membership) Leader() (Node, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, node := range m.nodes {
		if node.IsLeader {
			return node, true
		}
	}
	return Node{}, false
}

// SetLeader sets the leader flag on a node and clears it on others.
func (m *Membership) SetLeader(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	node, ok := m.nodes[id]
	if !ok {
		return fmt.Errorf("cluster: node %q not found", id)
	}
	for key := range m.nodes {
		current := m.nodes[key]
		current.IsLeader = false
		m.nodes[key] = current
	}
	node.IsLeader = true
	m.nodes[id] = node
	return nil
}

// Count returns the number of nodes in the membership list.
func (m *Membership) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.nodes)
}
