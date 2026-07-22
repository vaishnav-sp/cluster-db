package gossip

import (
	"math/rand"
	"time"

	"github.com/vaishnav-sp/cluster-db/internal/cluster"
)

// PeerSelector handles random peer selection for gossip dissemination.
type PeerSelector struct {
	rnd *rand.Rand
}

// NewPeerSelector creates a new PeerSelector instance.
func NewPeerSelector() *PeerSelector {
	return &PeerSelector{
		rnd: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// SelectRandomPeers picks up to `fanout` random peer nodes from `nodes`, excluding `localNodeID` and dead nodes.
func (ps *PeerSelector) SelectRandomPeers(nodes []cluster.Node, fanout int, localNodeID string) []cluster.Node {
	if fanout <= 0 || len(nodes) == 0 {
		return nil
	}

	var candidates []cluster.Node
	for _, n := range nodes {
		if n.ID == localNodeID || n.Address == "" {
			continue
		}
		candidates = append(candidates, n)
	}

	if len(candidates) <= fanout {
		return candidates
	}

	selected := make([]cluster.Node, fanout)
	perm := ps.rnd.Perm(len(candidates))
	for i := 0; i < fanout; i++ {
		selected[i] = candidates[perm[i]]
	}
	return selected
}

// SelectRandomPeers is a convenience helper function using a default selector.
func SelectRandomPeers(nodes []cluster.Node, fanout int, localNodeID string) []cluster.Node {
	return NewPeerSelector().SelectRandomPeers(nodes, fanout, localNodeID)
}
