package cluster

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	clusterRPC "github.com/vaishnav-sp/cluster-db/internal/cluster/rpc"
)

func TestManagerJoinSyncsMembership(t *testing.T) {
	rpcServer := clusterRPC.NewServer()
	rpcServer.JoinHandler = func(req clusterRPC.JoinRequest) (clusterRPC.JoinResponse, error) {
		return clusterRPC.JoinResponse{Accepted: true, Members: []clusterRPC.MemberInfo{{ID: "node-1", Address: "127.0.0.1:9001", Status: "alive"}}}, nil
	}
	server := httptest.NewServer(rpcServer.Handler())
	defer server.Close()

	manager := NewManager(NewMembership(), nil, time.Second, time.Second, "node-2", server.Listener.Addr().String())
	members, err := manager.JoinCluster(server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("join failed: %v", err)
	}
	if len(members) == 0 {
		t.Fatal("expected membership data")
	}
}

func TestManagerDuplicateJoinIsIgnored(t *testing.T) {
	manager := NewManager(NewMembership(), nil, time.Second, time.Second, "node-1", "127.0.0.1:9001")
	manager.membership.AddNode(Node{ID: "node-1", Address: "127.0.0.1:9001", Status: Alive})
	manager.membership.AddNode(Node{ID: "node-1", Address: "127.0.0.1:9001", Status: Alive})
	if got := manager.membership.Count(); got != 1 {
		t.Fatalf("duplicate join should not add another node, got %d", got)
	}
}

func TestManagerLeaveRemovesNode(t *testing.T) {
	manager := NewManager(NewMembership(), nil, time.Second, time.Second, "node-1", "127.0.0.1:9001")
	manager.membership.AddNode(Node{ID: "node-1", Address: "127.0.0.1:9001", Status: Alive})
	manager.membership.AddNode(Node{ID: "node-2", Address: "127.0.0.1:9002", Status: Alive})
	manager.membership.RemoveNode("node-2")
	if _, ok := manager.membership.GetNode("node-2"); ok {
		t.Fatal("node-2 should be removed")
	}
}

func TestManagerHeartbeatExchange(t *testing.T) {
	rpcServer := clusterRPC.NewServer()
	rpcServer.HeartbeatHandler = func(req clusterRPC.HeartbeatRequest) (clusterRPC.HeartbeatResponse, error) {
		return clusterRPC.HeartbeatResponse{Accepted: true}, nil
	}
	server := httptest.NewServer(rpcServer.Handler())
	defer server.Close()

	manager := NewManager(NewMembership(), nil, time.Second, time.Second, "node-1", server.Listener.Addr().String())
	manager.membership.AddNode(Node{ID: "node-2", Address: server.Listener.Addr().String(), Status: Alive})
	manager.sendHeartbeats()

	node, ok := manager.membership.GetNode("node-2")
	if !ok {
		t.Fatal("node-2 missing")
	}
	if node.Status != Alive {
		t.Fatalf("node-2 status = %v, want alive", node.Status)
	}
}

func TestManagerUnreachableNodeIsIgnored(t *testing.T) {
	manager := NewManager(NewMembership(), nil, time.Second, time.Second, "node-1", "127.0.0.1:9001")
	manager.membership.AddNode(Node{ID: "node-2", Address: "127.0.0.1:65535", Status: Alive})
	manager.sendHeartbeats()
	if _, ok := manager.membership.GetNode("node-2"); !ok {
		t.Fatal("node-2 should still exist")
	}
}

func TestManagerHandleJoinAndHeartbeat(t *testing.T) {
	manager := NewManager(NewMembership(), nil, time.Second, time.Second, "node-1", "127.0.0.1:9001")
	joinResp, err := manager.HandleJoin(clusterRPC.JoinRequest{NodeID: "node-2", Address: "127.0.0.1:9002"})
	if err != nil {
		t.Fatalf("handle join: %v", err)
	}
	if !joinResp.Accepted {
		t.Fatal("expected join response to be accepted")
	}

	heartbeatResp, err := manager.HandleHeartbeat(clusterRPC.HeartbeatRequest{NodeID: "node-2", Address: "127.0.0.1:9002"})
	if err != nil {
		t.Fatalf("handle heartbeat: %v", err)
	}
	if !heartbeatResp.Accepted {
		t.Fatal("expected heartbeat response to be accepted")
	}
}

func TestManagerStartStop(t *testing.T) {
	manager := NewManager(NewMembership(), nil, 10*time.Millisecond, 20*time.Millisecond, "node-1", "127.0.0.1:9001")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	manager.Stop()
}

func TestManagerOwnerLookupUsesHashRing(t *testing.T) {
	membership := NewMembership()
	membership.AddNode(Node{ID: "node-1", Address: "127.0.0.1:9001", Status: Alive, Version: "cluster/v1"})
	membership.AddNode(Node{ID: "node-2", Address: "127.0.0.1:9002", Status: Alive, Version: "cluster/v1"})

	manager := NewManager(membership, nil, time.Second, 3*time.Second, "local", "127.0.0.1:9000")

	node, ok := manager.Owner("alpha")
	if !ok {
		t.Fatal("expected owner for key")
	}
	if node.ID == "" {
		t.Fatal("expected populated owner node")
	}
}

func TestManagerOwnerUpdatesWhenNodeRemoved(t *testing.T) {
	membership := NewMembership()
	membership.AddNode(Node{ID: "node-1", Address: "127.0.0.1:9001", Status: Alive, Version: "cluster/v1"})
	membership.AddNode(Node{ID: "node-2", Address: "127.0.0.1:9002", Status: Alive, Version: "cluster/v1"})

	manager := NewManager(membership, nil, time.Second, 3*time.Second, "local", "127.0.0.1:9000")
	membership.RemoveNode("node-1")

	node, ok := manager.Owner("alpha")
	if !ok {
		t.Fatal("expected owner after removal")
	}
	if node.ID != "node-2" {
		t.Fatalf("owner ID = %s, want node-2", node.ID)
	}
}

func TestManagerOwnerUpdatesWhenNodeAdded(t *testing.T) {
	membership := NewMembership()
	membership.AddNode(Node{ID: "node-1", Address: "127.0.0.1:9001", Status: Alive, Version: "cluster/v1"})

	manager := NewManager(membership, nil, time.Second, 3*time.Second, "local", "127.0.0.1:9000")
	membership.AddNode(Node{ID: "node-2", Address: "127.0.0.1:9002", Status: Alive, Version: "cluster/v1"})

	if got := manager.hashRing.Count(); got != 2 {
		t.Fatalf("hash ring count = %d, want 2", got)
	}
}

func TestManagerLeaderRemainsUnchanged(t *testing.T) {
	membership := NewMembership()
	membership.AddNode(Node{ID: "node-1", Address: "127.0.0.1:9001", Status: Alive, Version: "cluster/v1"})
	membership.AddNode(Node{ID: "node-2", Address: "127.0.0.1:9002", Status: Alive, Version: "cluster/v1"})
	if err := membership.SetLeader("node-1"); err != nil {
		t.Fatalf("set leader: %v", err)
	}

	manager := NewManager(membership, nil, time.Second, 3*time.Second, "local", "127.0.0.1:9000")
	membership.AddNode(Node{ID: "node-3", Address: "127.0.0.1:9003", Status: Alive, Version: "cluster/v1"})

	leader, ok := manager.Leader()
	if !ok {
		t.Fatal("expected leader")
	}
	if leader.ID != "node-1" {
		t.Fatalf("leader ID = %s, want node-1", leader.ID)
	}
}

func TestManagerOwnerOnEmptyCluster(t *testing.T) {
	membership := NewMembership()
	manager := NewManager(membership, nil, time.Second, 3*time.Second, "local", "127.0.0.1:9000")

	if _, ok := manager.Owner("alpha"); ok {
		t.Fatal("expected no owner for empty cluster")
	}
	if _, ok := manager.Leader(); ok {
		t.Fatal("expected no leader for empty cluster")
	}
	if got := manager.LocalNode(); got.ID != "" {
		t.Fatalf("local node = %+v, want empty", got)
	}
}
