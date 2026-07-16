package cluster

import (
	"context"
	"testing"
	"time"
)

func TestMembershipLifecycle(t *testing.T) {
	m := NewMembership()

	m.AddNode(Node{ID: "node-1", Address: "127.0.0.1:9000", Status: Alive, Version: "v1"})
	m.AddNode(Node{ID: "node-2", Address: "127.0.0.1:9001", Status: Alive, Version: "v1"})

	if got := m.Count(); got != 2 {
		t.Fatalf("count = %d, want 2", got)
	}

	if _, ok := m.GetNode("node-1"); !ok {
		t.Fatalf("node-1 not found")
	}

	if err := m.SetLeader("node-2"); err != nil {
		t.Fatalf("set leader: %v", err)
	}
	leader, ok := m.Leader()
	if !ok || leader.ID != "node-2" {
		t.Fatalf("leader = %+v, want node-2", leader)
	}

	if err := m.UpdateHeartbeat("node-1"); err != nil {
		t.Fatalf("update heartbeat: %v", err)
	}
	if err := m.MarkSuspect("node-1"); err != nil {
		t.Fatalf("mark suspect: %v", err)
	}
	if err := m.MarkDead("node-1"); err != nil {
		t.Fatalf("mark dead: %v", err)
	}

	node, ok := m.GetNode("node-1")
	if !ok {
		t.Fatalf("node-1 missing after updates")
	}
	if node.Status != Dead {
		t.Fatalf("status = %v, want %v", node.Status, Dead)
	}
	if node.LastHeartbeat.IsZero() {
		t.Fatalf("heartbeat should be updated")
	}

	m.RemoveNode("node-1")
	if got := m.Count(); got != 1 {
		t.Fatalf("count after remove = %d, want 1", got)
	}
}

func TestMembershipSetLeaderClearsPreviousLeader(t *testing.T) {
	m := NewMembership()
	m.AddNode(Node{ID: "node-1", Status: Alive, IsLeader: true})
	m.AddNode(Node{ID: "node-2", Status: Alive})

	if err := m.SetLeader("node-2"); err != nil {
		t.Fatalf("set leader: %v", err)
	}

	first, _ := m.GetNode("node-1")
	second, _ := m.GetNode("node-2")
	if first.IsLeader {
		t.Fatalf("node-1 should no longer be leader")
	}
	if !second.IsLeader {
		t.Fatalf("node-2 should be leader")
	}
}

func TestMembershipUpdateHeartbeatRequiresExistingNode(t *testing.T) {
	m := NewMembership()
	if err := m.UpdateHeartbeat("missing"); err == nil {
		t.Fatal("expected error for missing node")
	}
}

func TestMembershipListNodesReturnsSnapshot(t *testing.T) {
	m := NewMembership()
	m.AddNode(Node{ID: "node-1", Status: Alive})
	m.AddNode(Node{ID: "node-2", Status: Alive})

	nodes := m.ListNodes()
	if len(nodes) != 2 {
		t.Fatalf("len(nodes) = %d, want 2", len(nodes))
	}

	// Mutate the returned slice and ensure the internal state is unchanged.
	nodes[0].Status = Dead
	updated, _ := m.GetNode("node-1")
	if updated.Status != Alive {
		t.Fatalf("internal node status changed unexpectedly")
	}
}

func TestElectLeaderInitialElection(t *testing.T) {
	m := NewMembership()
	m.AddNode(Node{ID: "node-2", Status: Alive})
	m.AddNode(Node{ID: "node-1", Status: Alive})

	leader, ok := m.ElectLeader()
	if !ok {
		t.Fatal("expected leader")
	}
	if leader.ID != "node-1" {
		t.Fatalf("leader ID = %s, want node-1", leader.ID)
	}
	if !m.IsLeader("node-1") {
		t.Fatal("node-1 should be leader")
	}
}

func TestElectLeaderAfterLeaderRemoval(t *testing.T) {
	m := NewMembership()
	m.AddNode(Node{ID: "node-2", Status: Alive})
	m.AddNode(Node{ID: "node-1", Status: Alive})

	m.RemoveNode("node-1")
	leader, ok := m.CurrentLeader()
	if !ok || leader.ID != "node-2" {
		t.Fatalf("leader after removal = %+v, want node-2", leader)
	}
}

func TestElectLeaderAfterLeaderFailure(t *testing.T) {
	m := NewMembership()
	m.AddNode(Node{ID: "node-2", Status: Alive})
	m.AddNode(Node{ID: "node-1", Status: Alive})

	if err := m.MarkDead("node-1"); err != nil {
		t.Fatalf("mark dead: %v", err)
	}
	leader, ok := m.CurrentLeader()
	if !ok || leader.ID != "node-2" {
		t.Fatalf("leader = %+v, want node-2", leader)
	}
}

func TestElectLeaderWhenBetterNodeJoins(t *testing.T) {
	m := NewMembership()
	m.AddNode(Node{ID: "node-2", Status: Alive})
	m.AddNode(Node{ID: "node-10", Status: Alive})

	leader, ok := m.CurrentLeader()
	if !ok || leader.ID != "node-10" {
		t.Fatalf("leader = %+v, want node-10", leader)
	}

	m.AddNode(Node{ID: "node-1", Status: Alive})
	leader, ok = m.CurrentLeader()
	if !ok || leader.ID != "node-1" {
		t.Fatalf("leader after better node join = %+v, want node-1", leader)
	}
}

func TestElectLeaderNoAliveNodes(t *testing.T) {
	m := NewMembership()
	m.AddNode(Node{ID: "node-1", Status: Suspect})
	m.AddNode(Node{ID: "node-2", Status: Dead})

	if _, ok := m.CurrentLeader(); ok {
		t.Fatal("expected no leader when no nodes are alive")
	}
}

func TestElectLeaderDeterministicAcrossCalls(t *testing.T) {
	m := NewMembership()
	m.AddNode(Node{ID: "node-3", Status: Alive})
	m.AddNode(Node{ID: "node-1", Status: Alive})
	m.AddNode(Node{ID: "node-2", Status: Alive})

	first, _ := m.ElectLeader()
	second, _ := m.ElectLeader()
	if first.ID != second.ID || first.ID != "node-1" {
		t.Fatalf("leader should remain deterministic, got %s and %s", first.ID, second.ID)
	}
}

func TestFailureDetectorMarksAliveNodeSuspect(t *testing.T) {
	m := NewMembership()
	m.SetLocalNodeID("local")
	m.SetFailureDetectorConfig(10*time.Millisecond, 15*time.Millisecond)
	m.AddNode(Node{ID: "local", Address: "127.0.0.1:9000", Status: Alive, LastHeartbeat: time.Now().UTC()})
	m.AddNode(Node{ID: "node-1", Address: "127.0.0.1:9001", Status: Alive, LastHeartbeat: time.Now().Add(-50 * time.Millisecond).UTC()})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.StartFailureDetector(ctx)
	defer m.StopFailureDetector()

	if err := waitForNodeStatus(t, m, "node-1", Suspect, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestFailureDetectorMarksSuspectNodeDead(t *testing.T) {
	m := NewMembership()
	m.SetLocalNodeID("local")
	m.SetFailureDetectorConfig(10*time.Millisecond, 15*time.Millisecond)
	m.AddNode(Node{ID: "local", Address: "127.0.0.1:9000", Status: Alive, LastHeartbeat: time.Now().UTC()})
	m.AddNode(Node{ID: "node-1", Address: "127.0.0.1:9001", Status: Suspect, LastHeartbeat: time.Now().Add(-50 * time.Millisecond).UTC()})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.StartFailureDetector(ctx)
	defer m.StopFailureDetector()

	if err := waitForNodeStatus(t, m, "node-1", Dead, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestFailureDetectorRestoresSuspectNodeOnHeartbeat(t *testing.T) {
	m := NewMembership()
	m.SetLocalNodeID("local")
	m.SetFailureDetectorConfig(10*time.Millisecond, 40*time.Millisecond)
	m.AddNode(Node{ID: "local", Address: "127.0.0.1:9000", Status: Alive, LastHeartbeat: time.Now().UTC()})
	m.AddNode(Node{ID: "node-1", Address: "127.0.0.1:9001", Status: Suspect, LastHeartbeat: time.Now().Add(-100 * time.Millisecond).UTC()})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.StartFailureDetector(ctx)
	defer m.StopFailureDetector()

	if err := m.UpdateHeartbeat("node-1"); err != nil {
		t.Fatalf("update heartbeat: %v", err)
	}

	if err := waitForNodeStatus(t, m, "node-1", Alive, time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestFailureDetectorReelectsLeaderAfterLeaderDies(t *testing.T) {
	m := NewMembership()
	m.SetLocalNodeID("local")
	m.SetFailureDetectorConfig(10*time.Millisecond, 15*time.Millisecond)
	m.AddNode(Node{ID: "local", Address: "127.0.0.1:9000", Status: Alive, LastHeartbeat: time.Now().UTC()})
	m.AddNode(Node{ID: "node-1", Address: "127.0.0.1:9001", Status: Alive, LastHeartbeat: time.Now().Add(-50 * time.Millisecond).UTC()})
	m.AddNode(Node{ID: "node-2", Address: "127.0.0.1:9002", Status: Alive, LastHeartbeat: time.Now().UTC()})
	if err := m.SetLeader("node-1"); err != nil {
		t.Fatalf("set leader: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.StartFailureDetector(ctx)
	defer m.StopFailureDetector()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		leader, ok := m.Leader()
		if ok && leader.ID == "node-2" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("leader did not re-elect to node-2")
}

func TestFailureDetectorShutdown(t *testing.T) {
	m := NewMembership()
	m.SetLocalNodeID("local")
	m.SetFailureDetectorConfig(10*time.Millisecond, 20*time.Millisecond)
	m.AddNode(Node{ID: "local", Address: "127.0.0.1:9000", Status: Alive, LastHeartbeat: time.Now().UTC()})
	m.AddNode(Node{ID: "node-1", Address: "127.0.0.1:9001", Status: Alive, LastHeartbeat: time.Now().UTC()})

	ctx, cancel := context.WithCancel(context.Background())
	m.StartFailureDetector(ctx)
	cancel()
	m.StopFailureDetector()
	m.StopFailureDetector()
}

func waitForNodeStatus(t *testing.T, m *Membership, id string, want Status, timeout time.Duration) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		node, ok := m.GetNode(id)
		if ok && node.Status == want {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}

	node, ok := m.GetNode(id)
	if !ok {
		return &statusError{nodeID: id, want: want, got: "missing"}
	}
	return &statusError{nodeID: id, want: want, got: node.Status}
}

type statusError struct {
	nodeID string
	want   Status
	got    interface{}
}

func (e *statusError) Error() string {
	got := "unknown"
	if status, ok := e.got.(Status); ok {
		got = status.String()
	} else if value, ok := e.got.(string); ok {
		got = value
	}
	return "node " + e.nodeID + " status = " + got + ", want " + e.want.String()
}
