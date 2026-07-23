package handlers

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/vaishnav-sp/cluster-db/internal/cluster"
	clusterRPC "github.com/vaishnav-sp/cluster-db/internal/cluster/rpc"
	"github.com/vaishnav-sp/cluster-db/internal/config"
	"github.com/vaishnav-sp/cluster-db/internal/storage"
	"github.com/vaishnav-sp/cluster-db/internal/storage/manager"
)

func setupRoutingTest(t *testing.T) (*KVHandler, *manager.Manager, *cluster.Manager, *manager.Manager, *httptest.Server) {
	// 1. Create storage for local node
	localStore, err := manager.New(config.StorageConfig{Engine: "memory"}, zap.NewNop())
	if err != nil {
		t.Fatalf("failed to create local store: %v", err)
	}
	if err := localStore.Open(context.Background()); err != nil {
		t.Fatalf("failed to open local store: %v", err)
	}

	// 2. Create storage for remote node
	remoteStore, err := manager.New(config.StorageConfig{Engine: "memory"}, zap.NewNop())
	if err != nil {
		t.Fatalf("failed to create remote store: %v", err)
	}
	if err := remoteStore.Open(context.Background()); err != nil {
		t.Fatalf("failed to open remote store: %v", err)
	}

	// 3. Create remote RPC server
	remoteRPC := clusterRPC.NewServer()
	remoteRPC.KVGetHandler = func(req clusterRPC.KVGetRequest) (clusterRPC.KVGetResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		rec, err := remoteStore.Get(ctx, storage.Key(req.Key))
		if err != nil {
			return clusterRPC.KVGetResponse{Error: err.Error()}, nil
		}
		return clusterRPC.KVGetResponse{Value: rec.Value, Found: true}, nil
	}
	remoteRPC.KVPutHandler = func(req clusterRPC.KVPutRequest) (clusterRPC.KVPutResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		rec := storage.Record{Key: storage.Key(req.Key), Value: storage.Value(req.Value)}
		if err := remoteStore.Put(ctx, rec); err != nil {
			return clusterRPC.KVPutResponse{Error: err.Error()}, nil
		}
		return clusterRPC.KVPutResponse{Success: true}, nil
	}
	remoteRPC.KVDeleteHandler = func(req clusterRPC.KVDeleteRequest) (clusterRPC.KVDeleteResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := remoteStore.Delete(ctx, storage.Key(req.Key)); err != nil {
			return clusterRPC.KVDeleteResponse{Error: err.Error()}, nil
		}
		return clusterRPC.KVDeleteResponse{Success: true}, nil
	}
	remoteRPC.ReplicaGetHandler = func(req clusterRPC.ReplicaGetRequest) (clusterRPC.ReplicaGetResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		rec, err := remoteStore.Get(ctx, storage.Key(req.Key))
		if err != nil {
			return clusterRPC.ReplicaGetResponse{Found: false}, nil
		}
		return clusterRPC.ReplicaGetResponse{Found: true, Value: rec.Value}, nil
	}
	remoteRPC.ReplicaPutHandler = func(req clusterRPC.ReplicaPutRequest) (clusterRPC.ReplicaPutResponse, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		rec := storage.Record{Key: storage.Key(req.Key), Value: storage.Value(req.Value)}
		if err := remoteStore.Put(ctx, rec); err != nil {
			return clusterRPC.ReplicaPutResponse{Error: err.Error()}, nil
		}
		return clusterRPC.ReplicaPutResponse{Success: true}, nil
	}

	remoteRPCServer := httptest.NewServer(remoteRPC.Handler())

	// 4. Create local cluster manager
	membership := cluster.NewMembership()
	localManager := cluster.NewManager(
		membership,
		zap.NewNop(),
		100*time.Millisecond,
		500*time.Millisecond,
		"node-local",
		"127.0.0.1:9001",
	)

	// Register local node and remote node on the membership
	membership.AddNode(cluster.Node{
		ID:      "node-local",
		Address: "127.0.0.1:9001",
		Status:  cluster.Alive,
	})
	membership.AddNode(cluster.Node{
		ID:      "node-remote",
		Address: remoteRPCServer.Listener.Addr().String(),
		Status:  cluster.Alive,
	})

	// Start local manager (this will initialize its internal hashRing)
	if err := localManager.Start(context.Background()); err != nil {
		t.Fatalf("failed to start local cluster manager: %v", err)
	}

	// 5. Create local KVHandler
	localHandler := NewKVHandler(localStore, localManager, nil)
	t.Cleanup(func() {
		localManager.Stop()
		localStore.Close(context.Background())
		remoteStore.Close(context.Background())
		remoteRPCServer.Close()
	})

	return localHandler, localStore, localManager, remoteStore, remoteRPCServer
}

func findKeys(t *testing.T, m *cluster.Manager) (string, string) {
	var localKey, remoteKey string
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("key-%d", i)
		owner, ok := m.Owner(k)
		if !ok {
			continue
		}
		if owner.ID == "node-local" && localKey == "" {
			localKey = k
		}
		if owner.ID == "node-remote" && remoteKey == "" {
			remoteKey = k
		}
		if localKey != "" && remoteKey != "" {
			break
		}
	}
	if localKey == "" || remoteKey == "" {
		t.Fatalf("could not find keys for local/remote routing")
	}
	return localKey, remoteKey
}

func TestKVHandlers_LocalOwner(t *testing.T) {
	handler, localStore, localManager, remoteStore, _ := setupRoutingTest(t)
	localKey, _ := findKeys(t, localManager)

	// 1. PUT locally
	reqPut := httptest.NewRequest(http.MethodPut, "/v1/kv/"+localKey, bytes.NewReader([]byte("local-val")))
	wPut := httptest.NewRecorder()
	handler.ServeHTTP(wPut, reqPut)

	if wPut.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", wPut.Code)
	}

	// Verify key is in localStore and NOT in remoteStore
	rec, err := localStore.Get(context.Background(), storage.Key(localKey))
	if err != nil {
		t.Fatalf("expected key in local store: %v", err)
	}
	if string(rec.Value) != "local-val" {
		t.Fatalf("expected 'local-val', got %q", string(rec.Value))
	}
	if _, err := remoteStore.Get(context.Background(), storage.Key(localKey)); err == nil {
		t.Fatal("expected key not to exist in remote store")
	}

	// 2. GET locally
	reqGet := httptest.NewRequest(http.MethodGet, "/v1/kv/"+localKey, nil)
	wGet := httptest.NewRecorder()
	handler.ServeHTTP(wGet, reqGet)

	if wGet.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", wGet.Code)
	}
	if wGet.Body.String() != "local-val" {
		t.Fatalf("expected 'local-val', got %q", wGet.Body.String())
	}

	// 3. DELETE locally
	reqDelete := httptest.NewRequest(http.MethodDelete, "/v1/kv/"+localKey, nil)
	wDelete := httptest.NewRecorder()
	handler.ServeHTTP(wDelete, reqDelete)

	if wDelete.Code != http.StatusNoContent {
		t.Fatalf("expected 204 NoContent, got %d", wDelete.Code)
	}
	if _, err := localStore.Get(context.Background(), storage.Key(localKey)); err == nil {
		t.Fatal("expected key to be deleted locally")
	}
}

func TestKVHandlers_RoutingDecisionAndRemoteForwarding(t *testing.T) {
	handler, localStore, localManager, remoteStore, _ := setupRoutingTest(t)
	localKey, remoteKey := findKeys(t, localManager)

	// Verify routing decisions
	ownerLocal, ok1 := localManager.Owner(localKey)
	ownerRemote, ok2 := localManager.Owner(remoteKey)
	if !ok1 || ownerLocal.ID != "node-local" {
		t.Fatalf("routing decision failed for local key: %+v", ownerLocal)
	}
	if !ok2 || ownerRemote.ID != "node-remote" {
		t.Fatalf("routing decision failed for remote key: %+v", ownerRemote)
	}

	// 1. Forwarded PUT
	reqPut := httptest.NewRequest(http.MethodPut, "/v1/kv/"+remoteKey, bytes.NewReader([]byte("remote-val")))
	wPut := httptest.NewRecorder()
	handler.ServeHTTP(wPut, reqPut)

	if wPut.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", wPut.Code)
	}

	// Verify key is in remoteStore and NOT in localStore
	rec, err := remoteStore.Get(context.Background(), storage.Key(remoteKey))
	if err != nil {
		t.Fatalf("expected key in remote store: %v", err)
	}
	if string(rec.Value) != "remote-val" {
		t.Fatalf("expected 'remote-val', got %q\n", string(rec.Value))
	}
	if _, err := localStore.Get(context.Background(), storage.Key(remoteKey)); err == nil {
		t.Fatal("expected key not to exist in local store")
	}

	// 2. Forwarded GET — uses ReplicaGet via read routing
	reqGet := httptest.NewRequest(http.MethodGet, "/v1/kv/"+remoteKey, nil)
	wGet := httptest.NewRecorder()
	handler.ServeHTTP(wGet, reqGet)

	if wGet.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", wGet.Code)
	}
	if wGet.Body.String() != "remote-val" {
		t.Fatalf("expected 'remote-val', got %q", wGet.Body.String())
	}

	// 3. Forwarded DELETE
	reqDelete := httptest.NewRequest(http.MethodDelete, "/v1/kv/"+remoteKey, nil)
	wDelete := httptest.NewRecorder()
	handler.ServeHTTP(wDelete, reqDelete)

	if wDelete.Code != http.StatusNoContent {
		t.Fatalf("expected 204 NoContent, got %d", wDelete.Code)
	}
	if _, err := remoteStore.Get(context.Background(), storage.Key(remoteKey)); err == nil {
		t.Fatal("expected key to be deleted remotely")
	}
}

func TestKVHandlers_OwnerUnavailable(t *testing.T) {
	// Scenario A: Empty cluster / no owner found
	t.Run("NoOwner", func(t *testing.T) {
		localStore, _ := manager.New(config.StorageConfig{Engine: "memory"}, zap.NewNop())
		_ = localStore.Open(context.Background())
		defer localStore.Close(context.Background())

		// Create a cluster manager with no nodes
		membership := cluster.NewMembership()
		localManager := cluster.NewManager(
			membership,
			zap.NewNop(),
			100*time.Millisecond,
			500*time.Millisecond,
			"node-local",
			"127.0.0.1:9001",
		)
		if err := localManager.Start(context.Background()); err != nil {
			t.Fatalf("failed to start: %v", err)
		}
		localManager.Membership().RemoveNode("node-local") // remove the node so ring is empty!

		handler := NewKVHandler(localStore, localManager, nil)
		reqGet := httptest.NewRequest(http.MethodGet, "/v1/kv/anykey", nil)
		wGet := httptest.NewRecorder()
		handler.ServeHTTP(wGet, reqGet)

		if wGet.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 Service Unavailable, got %d", wGet.Code)
		}
	})

	// Scenario B: Node is Alive but connection fails
	t.Run("NetworkFailure", func(t *testing.T) {
		handler, _, localManager, _, remoteRPCServer := setupRoutingTest(t)
		_, remoteKey := findKeys(t, localManager)

		// Close remote RPC server to simulate network crash / unreachable
		remoteRPCServer.Close()

		reqGet := httptest.NewRequest(http.MethodGet, "/v1/kv/"+remoteKey, nil)
		wGet := httptest.NewRecorder()
		handler.ServeHTTP(wGet, reqGet)

		// After failover exhausts all replicas, we get 503
		if wGet.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 Service Unavailable, got %d", wGet.Code)
		}
	})
}

// ─── Read Routing Tests ───────────────────────────────────────────────────────

// TestReadRouting_LocalOwnerReadsLocally verifies that when the local node owns a
// key, the GET handler reads from local storage without making any RPC calls.
func TestReadRouting_LocalOwnerReadsLocally(t *testing.T) {
	handler, localStore, localManager, _, _ := setupRoutingTest(t)
	localKey, _ := findKeys(t, localManager)

	// Pre-populate local store directly.
	if err := localStore.Put(context.Background(), storage.Record{
		Key: storage.Key(localKey), Value: storage.Value("direct"),
	}); err != nil {
		t.Fatalf("pre-populate: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/kv/"+localKey, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if w.Body.String() != "direct" {
		t.Fatalf("expected 'direct', got %q", w.Body.String())
	}
}

// ─── Read Failover Tests ──────────────────────────────────────────────────────

// TestReadFailover_FirstReplicaTimesOutFallsToNext verifies that when the first
// owner's connection fails, the handler transparently tries the next replica.
func TestReadFailover_FirstReplicaTimesOutFallsToNext(t *testing.T) {
	// Build a mock cluster where the first "owner" always errors, and the second
	// succeeds.  We test this by constructing replicaReadWithFailover directly.
	h := &KVHandler{
		client: clusterRPC.NewClient(100 * time.Millisecond),
	}

	// First owner: closed server → connection refused.
	closedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedServer.Close() // close immediately

	// Second owner: returns a valid response.
	goodValue := []byte("replica-value")
	goodServer := httptest.NewServer(clusterRPC.NewServer().Handler())
	defer goodServer.Close()
	goodSrv := clusterRPC.NewServer()
	goodSrv.ReplicaGetHandler = func(req clusterRPC.ReplicaGetRequest) (clusterRPC.ReplicaGetResponse, error) {
		return clusterRPC.ReplicaGetResponse{Found: true, Value: goodValue}, nil
	}
	goodTS := httptest.NewServer(goodSrv.Handler())
	defer goodTS.Close()

	owners := []cluster.Node{
		{ID: "n1", Address: closedServer.Listener.Addr().String(), Status: cluster.Alive},
		{ID: "n2", Address: goodTS.Listener.Addr().String(), Status: cluster.Alive},
	}

	val, err := h.replicaReadWithFailover(context.Background(), owners, "somekey")
	if err != nil {
		t.Fatalf("expected success after failover, got error: %v", err)
	}
	if !bytes.Equal(val, goodValue) {
		t.Fatalf("expected %q, got %q", goodValue, val)
	}
}

// TestReadFailover_AllReplicasFail verifies that 503 is returned when every
// replica is unreachable.
func TestReadFailover_AllReplicasFail(t *testing.T) {
	h := &KVHandler{
		client: clusterRPC.NewClient(50 * time.Millisecond),
	}

	// Both servers are immediately closed.
	s1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	s1.Close()
	s2.Close()

	owners := []cluster.Node{
		{ID: "n1", Address: s1.Listener.Addr().String(), Status: cluster.Alive},
		{ID: "n2", Address: s2.Listener.Addr().String(), Status: cluster.Alive},
	}

	_, err := h.replicaReadWithFailover(context.Background(), owners, "k")
	if err == nil {
		t.Fatal("expected error when all replicas fail")
	}
}

// TestReadFailover_SkipsDeadNodes verifies that nodes with non-Alive status are
// skipped without making a network call.
func TestReadFailover_SkipsDeadNodes(t *testing.T) {
	var calls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&calls, 1)
		w.WriteHeader(http.StatusNotImplemented)
	}))
	defer srv.Close()

	h := &KVHandler{client: clusterRPC.NewClient(time.Second)}
	owners := []cluster.Node{
		{ID: "dead", Address: srv.Listener.Addr().String(), Status: cluster.Dead},
		{ID: "suspect", Address: srv.Listener.Addr().String(), Status: cluster.Suspect},
	}

	_, err := h.replicaReadWithFailover(context.Background(), owners, "k")
	if err == nil {
		t.Fatal("expected error because all nodes are skipped")
	}
	if n := atomic.LoadInt64(&calls); n != 0 {
		t.Fatalf("expected 0 RPC calls for dead/suspect nodes, got %d", n)
	}
}

// ─── Read Repair / Majority Tests ────────────────────────────────────────────

// TestMajorityValue_ClearMajority verifies that the majority value wins when
// replicas disagree.
func TestMajorityValue_ClearMajority(t *testing.T) {
	results := []replicaReadResult{
		{value: []byte("a")},
		{value: []byte("b")},
		{value: []byte("a")},
	}
	got := majorityValue(results)
	if string(got) != "a" {
		t.Fatalf("majority = %q, want %q", got, "a")
	}
}

// TestMajorityValue_AllSame verifies consistent output when all replicas agree.
func TestMajorityValue_AllSame(t *testing.T) {
	results := []replicaReadResult{
		{value: []byte("x")},
		{value: []byte("x")},
		{value: []byte("x")},
	}
	got := majorityValue(results)
	if string(got) != "x" {
		t.Fatalf("majority = %q, want %q", got, "x")
	}
}

// TestMajorityValue_Tie verifies that a tie returns one of the values (first seen wins).
func TestMajorityValue_Tie(t *testing.T) {
	results := []replicaReadResult{
		{value: []byte("p")},
		{value: []byte("q")},
	}
	got := majorityValue(results)
	// Both have count=1; first inserted key wins.
	if string(got) != "p" && string(got) != "q" {
		t.Fatalf("unexpected majority value: %q", got)
	}
}

// TestMajorityValue_Empty verifies that an empty result set is handled gracefully.
func TestMajorityValue_Empty(t *testing.T) {
	got := majorityValue(nil)
	if string(got) != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

// TestReadRepair_AsyncUpdateStalReplica verifies that if a replica holds a stale
// value, the repair goroutine sends a ReplicaPut to update it.
func TestReadRepair_AsyncUpdateStalReplica(t *testing.T) {
	// Two mock replica servers: one fresh, one stale.
	freshValue := []byte("fresh")
	staleValue := []byte("stale")

	var repairReceived int64

	staleSrv := clusterRPC.NewServer()
	staleSrv.ReplicaGetHandler = func(req clusterRPC.ReplicaGetRequest) (clusterRPC.ReplicaGetResponse, error) {
		return clusterRPC.ReplicaGetResponse{Found: true, Value: staleValue}, nil
	}
	staleSrv.ReplicaPutHandler = func(req clusterRPC.ReplicaPutRequest) (clusterRPC.ReplicaPutResponse, error) {
		if bytes.Equal(req.Value, freshValue) {
			atomic.StoreInt64(&repairReceived, 1)
		}
		return clusterRPC.ReplicaPutResponse{Success: true}, nil
	}
	staleTS := httptest.NewServer(staleSrv.Handler())
	defer staleTS.Close()

	freshSrv := clusterRPC.NewServer()
	freshSrv.ReplicaGetHandler = func(req clusterRPC.ReplicaGetRequest) (clusterRPC.ReplicaGetResponse, error) {
		return clusterRPC.ReplicaGetResponse{Found: true, Value: freshValue}, nil
	}
	freshTS := httptest.NewServer(freshSrv.Handler())
	defer freshTS.Close()

	h := &KVHandler{client: clusterRPC.NewClient(time.Second)}

	owners := []cluster.Node{
		{ID: "fresh", Address: freshTS.Listener.Addr().String(), Status: cluster.Alive},
		{ID: "stale", Address: staleTS.Listener.Addr().String(), Status: cluster.Alive},
	}

	// Trigger async repair; the repair runs in the background.
	h.asyncReadRepair("repairkey", owners, freshValue)

	// Wait briefly for the goroutine to complete (no sleep needed: repair is
	// triggered synchronously inside asyncReadRepair, which itself is the goroutine).
	// Since we call asyncReadRepair directly (not via go), we can check immediately.
	if atomic.LoadInt64(&repairReceived) != 1 {
		t.Fatal("expected stale replica to receive a repair ReplicaPut")
	}
}

// ─── Manager Helper Tests ─────────────────────────────────────────────────────

// TestManagerIsLocalNode verifies the IsLocalNode helper.
func TestManagerIsLocalNode(t *testing.T) {
	membership := cluster.NewMembership()
	m := cluster.NewManager(membership, zap.NewNop(), time.Second, 3*time.Second, "node-local", "127.0.0.1:9001")

	if !m.IsLocalNode("node-local") {
		t.Fatal("expected IsLocalNode(node-local) = true")
	}
	if m.IsLocalNode("node-remote") {
		t.Fatal("expected IsLocalNode(node-remote) = false")
	}
	if m.IsLocalNode("") {
		t.Fatal("expected IsLocalNode('') = false")
	}
}
