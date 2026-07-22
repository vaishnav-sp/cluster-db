package gossip

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/vaishnav-sp/cluster-db/internal/cluster"
	clusterRPC "github.com/vaishnav-sp/cluster-db/internal/cluster/rpc"
)

// GossipNodeState contains membership information for a single node.
type GossipNodeState struct {
	NodeID        string         `json:"node_id"`
	Address       string         `json:"address"`
	Status        cluster.Status `json:"status"`
	Version       string         `json:"version"`
	LastHeartbeat time.Time      `json:"last_heartbeat"`
}

// GossipMessage represents a full membership exchange between gossip peers.
type GossipMessage struct {
	SenderID   string            `json:"sender_id"`
	SenderAddr string            `json:"sender_addr"`
	Nodes      []GossipNodeState `json:"nodes"`
}

// FromNode converts a cluster.Node into a GossipNodeState.
func FromNode(node cluster.Node) GossipNodeState {
	return GossipNodeState{
		NodeID:        node.ID,
		Address:       node.Address,
		Status:        node.Status,
		Version:       node.Version,
		LastHeartbeat: node.LastHeartbeat,
	}
}

// ToNode converts a GossipNodeState into a cluster.Node.
func (s GossipNodeState) ToNode() cluster.Node {
	return cluster.Node{
		ID:            s.NodeID,
		Address:       s.Address,
		Status:        s.Status,
		LastHeartbeat: s.LastHeartbeat,
		Version:       s.Version,
	}
}

// Serialize encodes a GossipMessage into JSON bytes.
func (m GossipMessage) Serialize() ([]byte, error) {
	return json.Marshal(m)
}

// DeserializeGossipMessage decodes JSON bytes into a GossipMessage.
func DeserializeGossipMessage(data []byte) (GossipMessage, error) {
	var msg GossipMessage
	err := json.Unmarshal(data, &msg)
	return msg, err
}

// ToGossipNodeStates converts a slice of RPC GossipNodeInfo into a slice of GossipNodeState.
func ToGossipNodeStates(nodes []clusterRPC.GossipNodeInfo) []GossipNodeState {
	var states []GossipNodeState
	for _, info := range nodes {
		statusVal := cluster.Alive
		switch strings.ToLower(info.Status) {
		case "suspect":
			statusVal = cluster.Suspect
		case "dead":
			statusVal = cluster.Dead
		}
		states = append(states, GossipNodeState{
			NodeID:        info.ID,
			Address:       info.Address,
			Status:        statusVal,
			Version:       info.Version,
			LastHeartbeat: info.LastHeartbeat,
		})
	}
	return states
}
