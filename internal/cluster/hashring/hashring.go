package hashring

import (
	"hash/crc32"
	"sort"
	"sync"
)

// HashRing stores nodes on a consistent hash ring with virtual nodes.
type HashRing struct {
	mu       sync.RWMutex
	ring     []entry
	replicas int
	nodes    map[string]struct{}
}

type entry struct {
	key  uint32
	node string
}

// New creates a new hash ring with the provided number of replicas.
// If replicas <= 0, a default of 100 replicas is used.
func New(replicas int) *HashRing {
	if replicas <= 0 {
		replicas = 100
	}
	return &HashRing{replicas: replicas, nodes: make(map[string]struct{})}
}

// AddNode adds a node to the ring using virtual nodes.
// Duplicate additions are ignored.
func (r *HashRing) AddNode(nodeID string) {
	if nodeID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.nodes[nodeID]; exists {
		return
	}
	r.nodes[nodeID] = struct{}{}
	for i := 0; i < r.replicas; i++ {
		virtualKey := r.virtualKey(nodeID, i)
		r.ring = append(r.ring, entry{key: virtualKey, node: nodeID})
	}
	sort.Slice(r.ring, func(i, j int) bool {
		return r.ring[i].key < r.ring[j].key
	})
}

// RemoveNode removes a node and its virtual nodes from the ring.
// Removing a missing node is a no-op.
func (r *HashRing) RemoveNode(nodeID string) {
	if nodeID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.nodes[nodeID]; !exists {
		return
	}
	delete(r.nodes, nodeID)
	filtered := make([]entry, 0, len(r.ring))
	for _, item := range r.ring {
		if item.node == nodeID {
			continue
		}
		filtered = append(filtered, item)
	}
	r.ring = filtered
}

// GetNode returns the node responsible for the given key and whether a node exists.
func (r *HashRing) GetNode(key string) (string, bool) {
	if key == "" {
		return "", false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.ring) == 0 {
		return "", false
	}

	hashed := crc32.ChecksumIEEE([]byte(key))
	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i].key >= hashed
	})
	if idx == len(r.ring) {
		idx = 0
	}
	return r.ring[idx].node, true
}

// Nodes returns a stable slice of the currently known nodes.
func (r *HashRing) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	nodes := make([]string, 0, len(r.nodes))
	for node := range r.nodes {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	return nodes
}

// Count returns the number of distinct nodes on the ring.
func (r *HashRing) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

func (r *HashRing) virtualKey(nodeID string, replica int) uint32 {
	return crc32.ChecksumIEEE([]byte(nodeID + "#" + string(rune('0'+replica))))
}
