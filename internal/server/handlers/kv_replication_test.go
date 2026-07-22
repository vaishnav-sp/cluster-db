package handlers

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/vaishnav-sp/cluster-db/internal/cluster"
	clusterRPC "github.com/vaishnav-sp/cluster-db/internal/cluster/rpc"
	"github.com/vaishnav-sp/cluster-db/internal/config"
	"github.com/vaishnav-sp/cluster-db/internal/storage"
	"github.com/vaishnav-sp/cluster-db/internal/storage/manager"
)

// replicationTestNode is a self-contained test node: its own store plus an RPC server.
type replicationTestNode struct {
	nodeID  string
	store   *manager.Manager
	rpcSrv  *httptest.Server
}

// newReplicationNode creates a storage manager backed by an in-memory engine and
// starts an RPC server that handles replica PUT / DELETE / KV GET / PUT / DELETE.
func newReplicationNode(t *testing.T, nodeID string) *replicationTestNode {
	t.Helper()

	store, err := manager.New(config.StorageConfig{Engine: "memory"}, zap.NewNop())
	if err != nil {
		t.Fatalf("node %s: create store: %v", nodeID, err)
	}
	if err := store.Open(context.Background()); err != nil {
		t.Fatalf("node %s: open store: %v", nodeID, err)
	}

	srv := clusterRPC.NewServer()

	srv.ReplicaPutHandler = func(req clusterRPC.ReplicaPutRequest) (clusterRPC.ReplicaPutResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		rec := storage.Record{Key: storage.Key(req.Key), Value: storage.Value(req.Value)}
		if err := store.Put(ctx, rec); err != nil {
			return clusterRPC.ReplicaPutResponse{Error: err.Error()}, nil
		}
		return clusterRPC.ReplicaPutResponse{Success: true}, nil
	}
	srv.ReplicaDeleteHandler = func(req clusterRPC.ReplicaDeleteRequest) (clusterRPC.ReplicaDeleteResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := store.Delete(ctx, storage.Key(req.Key)); err != nil {
			return clusterRPC.ReplicaDeleteResponse{Error: err.Error()}, nil
		}
		return clusterRPC.ReplicaDeleteResponse{Success: true}, nil
	}
	srv.KVGetHandler = func(req clusterRPC.KVGetRequest) (clusterRPC.KVGetResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		rec, err := store.Get(ctx, storage.Key(req.Key))
		if err != nil {
			return clusterRPC.KVGetResponse{Error: err.Error()}, nil
		}
		return clusterRPC.KVGetResponse{Value: rec.Value, Found: true}, nil
	}
	srv.KVPutHandler = func(req clusterRPC.KVPutRequest) (clusterRPC.KVPutResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		rec := storage.Record{Key: storage.Key(req.Key), Value: storage.Value(req.Value)}
		if err := store.Put(ctx, rec); err != nil {
			return clusterRPC.KVPutResponse{Error: err.Error()}, nil
		}
		return clusterRPC.KVPutResponse{Success: true}, nil
	}
	srv.KVDeleteHandler = func(req clusterRPC.KVDeleteRequest) (clusterRPC.KVDeleteResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := store.Delete(ctx, storage.Key(req.Key)); err != nil {
			return clusterRPC.KVDeleteResponse{Error: err.Error()}, nil
		}
		return clusterRPC.KVDeleteResponse{Success: true}, nil
	}

	rpcSrv := httptest.NewServer(srv.Handler())

	t.Cleanup(func() {
		rpcSrv.Close()
		store.Close(context.Background())
	})

	return &replicationTestNode{nodeID: nodeID, store: store, rpcSrv: rpcSrv}
}

// address returns the host:port that the node's RPC server is listening on.
func (n *replicationTestNode) address() string {
	return n.rpcSrv.Listener.Addr().String()
}

// hasKey checks whether the node's store contains a given key.
func (n *replicationTestNode) hasKey(t *testing.T, key string) bool {
	t.Helper()
	_, err := n.store.Get(context.Background(), storage.Key(key))
	return err == nil
}

// setupReplicationCluster builds a cluster of `count` nodes registered in a
// shared membership, starts the local manager (first node) and returns the
// KVHandler wired for local-node-0.
func setupReplicationCluster(t *testing.T, replicationFactor int, nodes ...*replicationTestNode) (*KVHandler, *cluster.Manager) {
	t.Helper()

	membership := cluster.NewMembership()
	for _, n := range nodes {
		membership.AddNode(cluster.Node{
			ID:      n.nodeID,
			Address: n.address(),
			Status:  cluster.Alive,
		})
	}

	// Node-0 is always the "local" node from the handler's perspective.
	localManager := cluster.NewManager(
		membership,
		zap.NewNop(),
		50*time.Millisecond,
		200*time.Millisecond,
		nodes[0].nodeID,
		nodes[0].address(),
	)
	localManager.ReplicationFactor = replicationFactor

	if err := localManager.Start(context.Background()); err != nil {
		t.Fatalf("start manager: %v", err)
	}

	handler := NewKVHandler(nodes[0].store, localManager)

	t.Cleanup(func() { localManager.Stop() })

	return handler, localManager
}

// findKeyForPrimary scans keys until it finds one whose primary owner in the
// ring is the requested node.
func findKeyForPrimary(t *testing.T, m *cluster.Manager, primaryID string, rf int) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("repkey-%d", i)
		owners, ok := m.Owners(k, rf)
		if !ok || len(owners) == 0 {
			continue
		}
		if owners[0].ID == primaryID {
			return k
		}
	}
	t.Fatalf("could not find a key whose primary is %q", primaryID)
	return ""
}

// -------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------

// TestReplication_Factor1 ensures that with RF=1 a write stays only on the primary.
func TestReplication_Factor1(t *testing.T) {
	n0 := newReplicationNode(t, "node-0")
	n1 := newReplicationNode(t, "node-1")

	handler, mgr := setupReplicationCluster(t, 1, n0, n1)

	key := findKeyForPrimary(t, mgr, "node-0", 1)

	req := httptest.NewRequest(http.MethodPut, "/v1/kv/"+key, bytes.NewReader([]byte("rf1-val")))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d body=%s", w.Code, w.Body.String())
	}

	if !n0.hasKey(t, key) {
		t.Fatal("primary node-0 should have the key")
	}
	if n1.hasKey(t, key) {
		t.Fatal("node-1 should NOT have the key with RF=1")
	}
}

// TestReplication_Factor2 checks that with RF=2 the second node on the ring is also written.
func TestReplication_Factor2(t *testing.T) {
	n0 := newReplicationNode(t, "node-0")
	n1 := newReplicationNode(t, "node-1")
	n2 := newReplicationNode(t, "node-2")

	handler, mgr := setupReplicationCluster(t, 2, n0, n1, n2)

	key := findKeyForPrimary(t, mgr, "node-0", 2)

	req := httptest.NewRequest(http.MethodPut, "/v1/kv/"+key, bytes.NewReader([]byte("rf2-val")))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d body=%s", w.Code, w.Body.String())
	}

	owners, ok := mgr.Owners(key, 2)
	if !ok || len(owners) < 2 {
		t.Fatal("expected 2 owners")
	}

	nodeMap := map[string]*replicationTestNode{"node-0": n0, "node-1": n1, "node-2": n2}
	for i, owner := range owners {
		nd := nodeMap[owner.ID]
		if !nd.hasKey(t, key) {
			t.Errorf("owner[%d] %s should have the key", i, owner.ID)
		}
	}

	// Verify the node NOT in the owner list has no copy.
	for id, nd := range nodeMap {
		inOwners := false
		for _, o := range owners {
			if o.ID == id {
				inOwners = true
				break
			}
		}
		if !inOwners && nd.hasKey(t, key) {
			t.Errorf("non-owner %s should NOT have the key", id)
		}
	}
}

// TestReplication_Factor3 checks full 3-node replication.
func TestReplication_Factor3(t *testing.T) {
	n0 := newReplicationNode(t, "node-0")
	n1 := newReplicationNode(t, "node-1")
	n2 := newReplicationNode(t, "node-2")

	handler, mgr := setupReplicationCluster(t, 3, n0, n1, n2)

	key := findKeyForPrimary(t, mgr, "node-0", 3)

	req := httptest.NewRequest(http.MethodPut, "/v1/kv/"+key, bytes.NewReader([]byte("rf3-val")))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d body=%s", w.Code, w.Body.String())
	}

	for _, nd := range []*replicationTestNode{n0, n1, n2} {
		if !nd.hasKey(t, key) {
			t.Errorf("node %s should have the key with RF=3", nd.nodeID)
		}
	}
}

// TestReplication_ReplicaOrdering verifies that ReplicaOwners respects the
// clockwise ordering defined by the consistent hash ring.
func TestReplication_ReplicaOrdering(t *testing.T) {
	n0 := newReplicationNode(t, "node-0")
	n1 := newReplicationNode(t, "node-1")
	n2 := newReplicationNode(t, "node-2")

	_, mgr := setupReplicationCluster(t, 3, n0, n1, n2)

	key := findKeyForPrimary(t, mgr, "node-0", 3)
	owners3, ok := mgr.Owners(key, 3)
	if !ok || len(owners3) != 3 {
		t.Fatalf("expected 3 owners, got %d", len(owners3))
	}

	// primary with RF=3 must match primary with RF=1 for the same key
	owners1, ok := mgr.Owners(key, 1)
	if !ok || len(owners1) != 1 {
		t.Fatal("expected 1 owner with RF=1")
	}
	if owners3[0].ID != owners1[0].ID {
		t.Errorf("primary mismatch: RF=3 primary=%s RF=1 primary=%s", owners3[0].ID, owners1[0].ID)
	}

	// RF=2 should be a prefix of RF=3
	owners2, ok := mgr.Owners(key, 2)
	if !ok || len(owners2) != 2 {
		t.Fatal("expected 2 owners with RF=2")
	}
	for i := 0; i < 2; i++ {
		if owners2[i].ID != owners3[i].ID {
			t.Errorf("RF=2 owner[%d]=%s but RF=3 owner[%d]=%s", i, owners2[i].ID, i, owners3[i].ID)
		}
	}
}

// TestReplication_SuccessfulReplicatedPUT verifies value is present on all replicas after PUT.
func TestReplication_SuccessfulReplicatedPUT(t *testing.T) {
	n0 := newReplicationNode(t, "node-0")
	n1 := newReplicationNode(t, "node-1")

	handler, mgr := setupReplicationCluster(t, 2, n0, n1)

	key := findKeyForPrimary(t, mgr, "node-0", 2)
	val := []byte("replicated-value")

	req := httptest.NewRequest(http.MethodPut, "/v1/kv/"+key, bytes.NewReader(val))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("PUT failed: %d %s", w.Code, w.Body.String())
	}

	// Primary
	rec, err := n0.store.Get(context.Background(), storage.Key(key))
	if err != nil {
		t.Fatalf("primary missing key: %v", err)
	}
	if string(rec.Value) != string(val) {
		t.Errorf("primary value = %q, want %q", rec.Value, val)
	}

	// Replica
	rec, err = n1.store.Get(context.Background(), storage.Key(key))
	if err != nil {
		t.Fatalf("replica missing key: %v", err)
	}
	if string(rec.Value) != string(val) {
		t.Errorf("replica value = %q, want %q", rec.Value, val)
	}
}

// TestReplication_SuccessfulReplicatedDELETE verifies that DELETE is replicated.
func TestReplication_SuccessfulReplicatedDELETE(t *testing.T) {
	n0 := newReplicationNode(t, "node-0")
	n1 := newReplicationNode(t, "node-1")

	handler, mgr := setupReplicationCluster(t, 2, n0, n1)

	key := findKeyForPrimary(t, mgr, "node-0", 2)
	val := []byte("to-delete")

	// Pre-populate both nodes directly so the key exists on both.
	rec := storage.Record{Key: storage.Key(key), Value: storage.Value(val)}
	if err := n0.store.Put(context.Background(), rec); err != nil {
		t.Fatalf("seed primary: %v", err)
	}
	if err := n1.store.Put(context.Background(), rec); err != nil {
		t.Fatalf("seed replica: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/v1/kv/"+key, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("DELETE failed: %d %s", w.Code, w.Body.String())
	}

	if n0.hasKey(t, key) {
		t.Error("primary should no longer have the key after DELETE")
	}
	if n1.hasKey(t, key) {
		t.Error("replica should no longer have the key after DELETE")
	}
}

// TestReplication_ReplicaFailure verifies that a replica failure causes the
// handler to return an error and NOT acknowledge success.
func TestReplication_ReplicaFailure(t *testing.T) {
	n0 := newReplicationNode(t, "node-0")
	n1 := newReplicationNode(t, "node-1")

	handler, mgr := setupReplicationCluster(t, 2, n0, n1)

	key := findKeyForPrimary(t, mgr, "node-0", 2)

	// Shut down the replica's RPC server to simulate a network failure.
	n1.rpcSrv.Close()

	req := httptest.NewRequest(http.MethodPut, "/v1/kv/"+key, bytes.NewReader([]byte("bad-replica")))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Must NOT return 201; must return a gateway error.
	if w.Code == http.StatusCreated {
		t.Fatal("expected error when replica is down, got 201 Created")
	}
	if w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 Bad Gateway, got %d body=%s", w.Code, w.Body.String())
	}
}

// TestReplication_PrimaryFailure verifies that when the primary is unreachable
// and the request is forwarded to it, a meaningful error is returned.
func TestReplication_PrimaryFailure(t *testing.T) {
	n0 := newReplicationNode(t, "node-0")
	n1 := newReplicationNode(t, "node-1")

	// Build a cluster where n1 is the "local" node but n0 is the primary for the key.
	_, mgr0 := setupReplicationCluster(t, 1, n0, n1)
	key := findKeyForPrimary(t, mgr0, "node-0", 1)

	// Now build a handler from n1's perspective so it must forward to n0.
	membership := cluster.NewMembership()
	membership.AddNode(cluster.Node{
		ID:      "node-0",
		Address: n0.address(),
		Status:  cluster.Alive,
	})
	membership.AddNode(cluster.Node{
		ID:      "node-1",
		Address: n1.address(),
		Status:  cluster.Alive,
	})

	n1Manager := cluster.NewManager(
		membership,
		zap.NewNop(),
		50*time.Millisecond,
		200*time.Millisecond,
		"node-1",
		n1.address(),
	)
	n1Manager.ReplicationFactor = 1

	if err := n1Manager.Start(context.Background()); err != nil {
		t.Fatalf("start n1 manager: %v", err)
	}
	t.Cleanup(func() { n1Manager.Stop() })

	handler := NewKVHandler(n1.store, n1Manager)

	// Close n0 RPC server — primary is now unreachable.
	n0.rpcSrv.Close()

	req := httptest.NewRequest(http.MethodPut, "/v1/kv/"+key, bytes.NewReader([]byte("fail-primary")))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code == http.StatusCreated {
		t.Fatal("expected error when primary is down, got 201 Created")
	}
	// Expect a gateway-level error (502) since the forwarded RPC call fails.
	if w.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 Bad Gateway, got %d body=%s", w.Code, w.Body.String())
	}
}

// TestReplication_RoutingStillWorks checks that after enabling replication the
// basic routing (GET/PUT/DELETE to the correct node) still functions correctly.
func TestReplication_RoutingStillWorks(t *testing.T) {
	n0 := newReplicationNode(t, "node-0")
	n1 := newReplicationNode(t, "node-1")

	handler, mgr := setupReplicationCluster(t, 1, n0, n1)

	localKey := findKeyForPrimary(t, mgr, "node-0", 1)
	remoteKey := findKeyForPrimary(t, mgr, "node-1", 1)

	// --- PUT to locally-owned key ---
	putReq := httptest.NewRequest(http.MethodPut, "/v1/kv/"+localKey, bytes.NewReader([]byte("local-val")))
	putW := httptest.NewRecorder()
	handler.ServeHTTP(putW, putReq)
	if putW.Code != http.StatusCreated {
		t.Fatalf("local PUT: expected 201, got %d", putW.Code)
	}
	if !n0.hasKey(t, localKey) {
		t.Error("local PUT: key should exist on n0")
	}
	if n1.hasKey(t, localKey) {
		t.Error("local PUT with RF=1: key should NOT exist on n1")
	}

	// --- GET locally-owned key ---
	getReq := httptest.NewRequest(http.MethodGet, "/v1/kv/"+localKey, nil)
	getW := httptest.NewRecorder()
	handler.ServeHTTP(getW, getReq)
	if getW.Code != http.StatusOK {
		t.Fatalf("local GET: expected 200, got %d", getW.Code)
	}
	if getW.Body.String() != "local-val" {
		t.Errorf("local GET: expected 'local-val', got %q", getW.Body.String())
	}

	// --- PUT to remotely-owned key (should be forwarded) ---
	putRemoteReq := httptest.NewRequest(http.MethodPut, "/v1/kv/"+remoteKey, bytes.NewReader([]byte("remote-val")))
	putRemoteW := httptest.NewRecorder()
	handler.ServeHTTP(putRemoteW, putRemoteReq)
	if putRemoteReq.Body != nil {
		putRemoteReq.Body.Close()
	}
	if putRemoteW.Code != http.StatusCreated {
		t.Fatalf("remote PUT: expected 201, got %d body=%s", putRemoteW.Code, putRemoteW.Body.String())
	}

	// --- DELETE locally-owned key ---
	delReq := httptest.NewRequest(http.MethodDelete, "/v1/kv/"+localKey, nil)
	delW := httptest.NewRecorder()
	handler.ServeHTTP(delW, delReq)
	if delW.Code != http.StatusNoContent {
		t.Fatalf("local DELETE: expected 204, got %d", delW.Code)
	}
	if n0.hasKey(t, localKey) {
		t.Error("local DELETE: key should be gone from n0")
	}
}
