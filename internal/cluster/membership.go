package cluster

import (
	"context"
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

	localNodeID string

	heartbeatInterval time.Duration
	failureTimeout    time.Duration

	failureDetectorStopCh  chan struct{}
	failureDetectorDoneCh  chan struct{}
	failureDetectorStarted bool

	changeHook func()
}

func (s Status) String() string {
	switch s {
	case Alive:
		return "alive"
	case Suspect:
		return "suspect"
	case Dead:
		return "dead"
	default:
		return "unknown"
	}
}

// NewMembership creates an empty membership manager.
func NewMembership() *Membership {
	return &Membership{
		nodes:             make(map[string]Node),
		heartbeatInterval: time.Second,
		failureTimeout:    3 * time.Second,
	}
}

// AddNode inserts or replaces a node in the membership list.
func (m *Membership) AddNode(node Node) {
	m.mu.Lock()
	m.nodes[node.ID] = node
	m.electLeaderLocked()
	m.mu.Unlock()
	m.notifyChange()
}

// SetLocalNodeID configures the node identifier that should be excluded from failure detection.
func (m *Membership) SetLocalNodeID(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.localNodeID = id
}

// SetFailureDetectorConfig configures the detector interval and failure timeout.
func (m *Membership) SetFailureDetectorConfig(heartbeatInterval, failureTimeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if heartbeatInterval > 0 {
		m.heartbeatInterval = heartbeatInterval
	}
	if failureTimeout > 0 {
		m.failureTimeout = failureTimeout
	}
}

// StartFailureDetector begins a background ticker that evaluates node health.
func (m *Membership) StartFailureDetector(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	m.mu.Lock()
	if m.failureDetectorStarted {
		m.mu.Unlock()
		return
	}
	if m.heartbeatInterval <= 0 {
		m.heartbeatInterval = time.Second
	}
	if m.failureTimeout <= 0 {
		m.failureTimeout = 3 * time.Second
	}
	interval := m.heartbeatInterval
	timeout := m.failureTimeout
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	m.failureDetectorStopCh = stopCh
	m.failureDetectorDoneCh = doneCh
	m.failureDetectorStarted = true
	m.mu.Unlock()

	go func() {
		defer close(doneCh)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-ticker.C:
				m.detectFailures(time.Now().UTC(), timeout)
			}
		}
	}()
}

// StopFailureDetector stops the background failure detector.
func (m *Membership) StopFailureDetector() {
	m.mu.Lock()
	if !m.failureDetectorStarted {
		m.mu.Unlock()
		return
	}
	stopCh := m.failureDetectorStopCh
	doneCh := m.failureDetectorDoneCh
	m.failureDetectorStopCh = nil
	m.failureDetectorDoneCh = nil
	m.failureDetectorStarted = false
	m.mu.Unlock()

	close(stopCh)
	if doneCh != nil {
		<-doneCh
	}
}

// RemoveNode deletes a node by id.
func (m *Membership) RemoveNode(id string) {
	m.mu.Lock()
	delete(m.nodes, id)
	m.electLeaderLocked()
	m.mu.Unlock()
	m.notifyChange()
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
	node, ok := m.nodes[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("cluster: node %q not found", id)
	}
	node.LastHeartbeat = time.Now().UTC()
	node.Status = Alive
	m.nodes[id] = node
	m.electLeaderLocked()
	m.mu.Unlock()
	m.notifyChange()
	return nil
}

// MarkSuspect marks a node as suspect.
func (m *Membership) MarkSuspect(id string) error {
	m.mu.Lock()
	node, ok := m.nodes[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("cluster: node %q not found", id)
	}
	node.Status = Suspect
	m.nodes[id] = node
	m.electLeaderLocked()
	m.mu.Unlock()
	m.notifyChange()
	return nil
}

// MarkDead marks a node as dead.
func (m *Membership) MarkDead(id string) error {
	m.mu.Lock()
	node, ok := m.nodes[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("cluster: node %q not found", id)
	}
	node.Status = Dead
	m.nodes[id] = node
	m.electLeaderLocked()
	m.mu.Unlock()
	m.notifyChange()
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

// ElectLeader selects the current leader deterministically from the alive nodes.
func (m *Membership) ElectLeader() (Node, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.electLeaderLocked()
}

// CurrentLeader returns the current leader node, if one exists.
func (m *Membership) CurrentLeader() (Node, bool) {
	return m.Leader()
}

// IsLeader reports whether the given id is the current leader.
func (m *Membership) IsLeader(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	leader, ok := m.Leader()
	return ok && leader.ID == id
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

func (m *Membership) setChangeHook(h func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.changeHook = h
}

func (m *Membership) notifyChange() {
	m.mu.RLock()
	hook := m.changeHook
	m.mu.RUnlock()
	if hook != nil {
		hook()
	}
}

func (m *Membership) detectFailures(now time.Time, failureTimeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, node := range m.nodes {
		if id == m.localNodeID || node.ID == m.localNodeID {
			continue
		}
		if node.Status == Dead {
			continue
		}

		elapsed := now.Sub(node.LastHeartbeat)
		if node.Status == Alive && elapsed > failureTimeout {
			node.Status = Suspect
			m.nodes[id] = node
			continue
		}
		if node.Status == Suspect && elapsed > 2*failureTimeout {
			node.Status = Dead
			m.nodes[id] = node
			m.electLeaderLocked()
		}
	}
}

func (m *Membership) electLeaderLocked() (Node, bool) {
	var leader Node
	var found bool
	var leaderID string
	var currentLeader Node
	var currentLeaderFound bool

	for _, node := range m.nodes {
		if node.IsLeader {
			currentLeader = node
			currentLeaderFound = true
		}
		if node.Status != Alive {
			continue
		}
		if node.ID == m.localNodeID {
			continue
		}
		if !found || node.ID < leaderID {
			leader = node
			leaderID = node.ID
			found = true
		}
	}

	for key := range m.nodes {
		current := m.nodes[key]
		current.IsLeader = false
		m.nodes[key] = current
	}

	if found {
		leader.IsLeader = true
		m.nodes[leader.ID] = leader
		return leader, true
	}
	if currentLeaderFound {
		return currentLeader, false
	}
	return Node{}, false
}
