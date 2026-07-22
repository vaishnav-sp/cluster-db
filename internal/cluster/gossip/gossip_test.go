package gossip

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/vaishnav-sp/cluster-db/internal/cluster"
	clusterRPC "github.com/vaishnav-sp/cluster-db/internal/cluster/rpc"
)

// 1. TestMembershipMerge tests merging disjoint and overlapping node membership states.
func TestMembershipMerge(t *testing.T) {
	mem := cluster.NewMembership()
	mem.AddNode(cluster.Node{ID: "node-1", Address: "127.0.0.1:9001", Status: cluster.Alive, Version: "v1"})

	engine := NewEngine(Config{
		LocalNodeID: "node-1",
		Membership:  mem,
		Logger:      zap.NewNop(),
	})

	now := time.Now().UTC()
	remoteNodes := []GossipNodeState{
		{NodeID: "node-2", Address: "127.0.0.1:9002", Status: cluster.Alive, Version: "v1", LastHeartbeat: now},
		{NodeID: "node-1", Address: "127.0.0.1:9001", Status: cluster.Alive, Version: "v1", LastHeartbeat: now},
	}

	engine.MergeMembership(remoteNodes)

	if mem.Count() != 2 {
		t.Fatalf("expected 2 nodes after merge, got %d", mem.Count())
	}
	n2, ok := mem.GetNode("node-2")
	if !ok {
		t.Fatal("node-2 not found in membership")
	}
	if n2.Status != cluster.Alive {
		t.Errorf("expected node-2 to be Alive, got %v", n2.Status)
	}
}

// 2. TestVersionConflict tests that a node state with a higher version overrides a lower version state.
func TestVersionConflict(t *testing.T) {
	mem := cluster.NewMembership()
	mem.AddNode(cluster.Node{ID: "node-2", Address: "127.0.0.1:9002", Status: cluster.Suspect, Version: "v1"})

	engine := NewEngine(Config{
		LocalNodeID: "node-1",
		Membership:  mem,
		Logger:      zap.NewNop(),
	})

	now := time.Now().UTC()

	// Lower version remote should NOT override higher version existing
	engine.MergeMembership([]GossipNodeState{
		{NodeID: "node-2", Address: "127.0.0.1:9002", Status: cluster.Alive, Version: "v0", LastHeartbeat: now},
	})
	n2, _ := mem.GetNode("node-2")
	if n2.Version != "v1" {
		t.Errorf("expected version v1 to remain, got %s", n2.Version)
	}

	// Higher version remote SHOULD override lower version existing
	engine.MergeMembership([]GossipNodeState{
		{NodeID: "node-2", Address: "127.0.0.1:9002", Status: cluster.Alive, Version: "v2", LastHeartbeat: now},
	})
	n2, _ = mem.GetNode("node-2")
	if n2.Version != "v2" {
		t.Errorf("expected version to be updated to v2, got %s", n2.Version)
	}
	if n2.Status != cluster.Alive {
		t.Errorf("expected status to be updated to Alive, got %v", n2.Status)
	}
}

// 3. TestHeartbeatUpdate verifies timestamp updates when merging newer heartbeats.
func TestHeartbeatUpdate(t *testing.T) {
	oldTime := time.Now().UTC().Add(-10 * time.Second)
	newTime := time.Now().UTC()

	mem := cluster.NewMembership()
	mem.AddNode(cluster.Node{ID: "node-2", Address: "127.0.0.1:9002", Status: cluster.Suspect, Version: "v1", LastHeartbeat: oldTime})

	engine := NewEngine(Config{
		LocalNodeID: "node-1",
		Membership:  mem,
		Logger:      zap.NewNop(),
	})

	engine.MergeMembership([]GossipNodeState{
		{NodeID: "node-2", Address: "127.0.0.1:9002", Status: cluster.Alive, Version: "v1", LastHeartbeat: newTime},
	})

	n2, _ := mem.GetNode("node-2")
	if n2.LastHeartbeat.Before(newTime) {
		t.Errorf("expected heartbeat timestamp to be updated to %v, got %v", newTime, n2.LastHeartbeat)
	}
	if n2.Status != cluster.Alive {
		t.Errorf("expected status to upgrade to Alive, got %v", n2.Status)
	}
}

// 4. TestSuspectTransition verifies that a node with stale heartbeats transitions to Suspect.
func TestSuspectTransition(t *testing.T) {
	mem := cluster.NewMembership()
	staleTime := time.Now().UTC().Add(-200 * time.Millisecond)
	mem.AddNode(cluster.Node{ID: "node-2", Address: "127.0.0.1:9002", Status: cluster.Alive, Version: "v1", LastHeartbeat: staleTime})

	engine := NewEngine(Config{
		LocalNodeID:    "node-1",
		Membership:     mem,
		Logger:         zap.NewNop(),
		FailureTimeout: 100 * time.Millisecond,
	})

	engine.evaluateFailureDetector()

	n2, _ := mem.GetNode("node-2")
	if n2.Status != cluster.Suspect {
		t.Errorf("expected node-2 status to be Suspect, got %v", n2.Status)
	}
}

// 5. TestDeadTransition verifies that a Suspect node with extended stale heartbeat transitions to Dead.
func TestDeadTransition(t *testing.T) {
	mem := cluster.NewMembership()
	veryStaleTime := time.Now().UTC().Add(-300 * time.Millisecond)
	mem.AddNode(cluster.Node{ID: "node-2", Address: "127.0.0.1:9002", Status: cluster.Suspect, Version: "v1", LastHeartbeat: veryStaleTime})

	engine := NewEngine(Config{
		LocalNodeID:    "node-1",
		Membership:     mem,
		Logger:         zap.NewNop(),
		FailureTimeout: 100 * time.Millisecond,
	})

	engine.evaluateFailureDetector()

	n2, _ := mem.GetNode("node-2")
	if n2.Status != cluster.Dead {
		t.Errorf("expected node-2 status to be Dead, got %v", n2.Status)
	}
}

// 6. TestGossipMessageSerialization tests JSON encoding and decoding of GossipMessage.
func TestGossipMessageSerialization(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	orig := GossipMessage{
		SenderID:   "node-1",
		SenderAddr: "127.0.0.1:9001",
		Nodes: []GossipNodeState{
			{NodeID: "node-1", Address: "127.0.0.1:9001", Status: cluster.Alive, Version: "v1", LastHeartbeat: now},
			{NodeID: "node-2", Address: "127.0.0.1:9002", Status: cluster.Suspect, Version: "v1", LastHeartbeat: now},
		},
	}

	data, err := orig.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize gossip message: %v", err)
	}

	deserialized, err := DeserializeGossipMessage(data)
	if err != nil {
		t.Fatalf("failed to deserialize gossip message: %v", err)
	}

	if deserialized.SenderID != orig.SenderID {
		t.Errorf("expected sender %s, got %s", orig.SenderID, deserialized.SenderID)
	}
	if len(deserialized.Nodes) != len(orig.Nodes) {
		t.Fatalf("expected %d nodes, got %d", len(orig.Nodes), len(deserialized.Nodes))
	}
	if deserialized.Nodes[0].NodeID != orig.Nodes[0].NodeID {
		t.Errorf("expected node[0] ID %s, got %s", orig.Nodes[0].NodeID, deserialized.Nodes[0].NodeID)
	}
}

// 7. TestRandomPeerSelection tests peer selection excluding local node and dead nodes.
func TestRandomPeerSelection(t *testing.T) {
	nodes := []cluster.Node{
		{ID: "node-1", Address: "127.0.0.1:9001", Status: cluster.Alive},
		{ID: "node-2", Address: "127.0.0.1:9002", Status: cluster.Alive},
		{ID: "node-3", Address: "127.0.0.1:9003", Status: cluster.Alive},
		{ID: "node-4", Address: "127.0.0.1:9004", Status: cluster.Alive},
	}

	selector := NewPeerSelector()
	selected := selector.SelectRandomPeers(nodes, 2, "node-1")

	if len(selected) != 2 {
		t.Fatalf("expected 2 peers selected, got %d", len(selected))
	}

	for _, p := range selected {
		if p.ID == "node-1" {
			t.Errorf("local node-1 should not be selected as a peer")
		}
	}
}

// 8. TestGossipExchangeIntegration verifies RPC-driven state exchange between two gossip engines.
func TestGossipExchangeIntegration(t *testing.T) {
	mem1 := cluster.NewMembership()
	mem1.AddNode(cluster.Node{ID: "node-1", Address: "127.0.0.1:9001", Status: cluster.Alive, Version: "v1", LastHeartbeat: time.Now().UTC()})
	mem1.AddNode(cluster.Node{ID: "node-2", Address: "127.0.0.1:9002", Status: cluster.Alive, Version: "v1", LastHeartbeat: time.Now().UTC()})

	mem2 := cluster.NewMembership()
	mem2.AddNode(cluster.Node{ID: "node-2", Address: "127.0.0.1:9002", Status: cluster.Alive, Version: "v1", LastHeartbeat: time.Now().UTC()})
	mem2.AddNode(cluster.Node{ID: "node-3", Address: "127.0.0.1:9003", Status: cluster.Alive, Version: "v1", LastHeartbeat: time.Now().UTC()})

	eng1 := NewEngine(Config{LocalNodeID: "node-1", LocalAddress: "127.0.0.1:9001", Membership: mem1, Logger: zap.NewNop()})
	eng2 := NewEngine(Config{LocalNodeID: "node-2", LocalAddress: "127.0.0.1:9002", Membership: mem2, Logger: zap.NewNop()})

	rpcSrv2 := clusterRPC.NewServer()
	rpcSrv2.GossipHandler = func(req clusterRPC.GossipRequest) (clusterRPC.GossipResponse, error) {
		return eng2.HandleInboundGossip(req), nil
	}
	ts2 := httptest.NewServer(rpcSrv2.Handler())
	defer ts2.Close()

	client1 := clusterRPC.NewClient(time.Second)
	req := clusterRPC.GossipRequest{
		SenderID:   "node-1",
		SenderAddr: "127.0.0.1:9001",
		Nodes:      eng1.GetLocalStateSnapshot(),
	}

	resp, err := client1.Gossip(context.Background(), ts2.Listener.Addr().String(), req)
	if err != nil {
		t.Fatalf("gossip RPC failed: %v", err)
	}
	if !resp.Accepted {
		t.Fatalf("gossip RPC not accepted")
	}

	// Eng2 should now have node-1 in its membership
	if _, ok := mem2.GetNode("node-1"); !ok {
		t.Error("engine-2 should have learned node-1 via gossip exchange")
	}

	// Eng1 merges response nodes and learns node-3
	eng1.MergeMembership(toNodeStates(resp.Nodes))
	if _, ok := mem1.GetNode("node-3"); !ok {
		t.Error("engine-1 should have learned node-3 from gossip response")
	}
}

func toNodeStates(nodes []clusterRPC.GossipNodeInfo) []GossipNodeState {
	var res []GossipNodeState
	for _, n := range nodes {
		statusVal := cluster.Alive
		if n.Status == "suspect" {
			statusVal = cluster.Suspect
		} else if n.Status == "dead" {
			statusVal = cluster.Dead
		}
		res = append(res, GossipNodeState{
			NodeID:        n.ID,
			Address:       n.Address,
			Status:        statusVal,
			Version:       n.Version,
			LastHeartbeat: n.LastHeartbeat,
		})
	}
	return res
}
