package handoff

import (
	"context"
	"sync"
	"time"
)

const (
	OperationPut    = "put"
	OperationDelete = "delete"
)

// Hint stores a single deferred write destined for an unreachable replica.
type Hint struct {
	TargetNode string
	Operation  string
	Key        string
	Value      []byte
	Timestamp  time.Time
}

// Manager holds pending hints and replays them when a node recovers.
type Manager struct {
	mu    sync.Mutex
	hints map[string][]Hint // keyed by target node ID
}

// NewManager creates a new hinted handoff Manager.
func NewManager() *Manager {
	return &Manager{
		hints: make(map[string][]Hint),
	}
}

// StoreHint persists a hint for a replica that could not be reached.
func (m *Manager) StoreHint(targetNode, operation, key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hints[targetNode] = append(m.hints[targetNode], Hint{
		TargetNode: targetNode,
		Operation:  operation,
		Key:        key,
		Value:      value,
		Timestamp:  time.Now().UTC(),
	})
}

// PendingHints returns a copy of all pending hints for the given node.
func (m *Manager) PendingHints(nodeID string) []Hint {
	m.mu.Lock()
	defer m.mu.Unlock()
	src := m.hints[nodeID]
	if len(src) == 0 {
		return nil
	}
	out := make([]Hint, len(src))
	copy(out, src)
	return out
}

// RemoveHint removes the hint at the given index for the target node.
func (m *Manager) RemoveHint(nodeID string, index int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	hints := m.hints[nodeID]
	if index < 0 || index >= len(hints) {
		return
	}
	m.hints[nodeID] = append(hints[:index], hints[index+1:]...)
	if len(m.hints[nodeID]) == 0 {
		delete(m.hints, nodeID)
	}
}

// ReplayHints sends every pending hint for nodeID to the provided address.
// On success the hint is removed; on failure it is left intact for the next replay attempt.
func (m *Manager) ReplayHints(ctx context.Context, nodeID, address string, put func(ctx context.Context, address, key string, value []byte) error, del func(ctx context.Context, address, key string) error) {
	hints := m.PendingHints(nodeID)
	// Traverse in reverse so RemoveHint index shifts do not affect upcoming entries.
	for i := len(hints) - 1; i >= 0; i-- {
		h := hints[i]
		var err error
		switch h.Operation {
		case OperationPut:
			err = put(ctx, address, h.Key, h.Value)
		case OperationDelete:
			err = del(ctx, address, h.Key)
		}
		if err == nil {
			m.RemoveHint(nodeID, i)
		}
	}
}
