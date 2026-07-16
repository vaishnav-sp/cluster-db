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
	localHandler := NewKVHandler(localStore, localManager)

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
		t.Fatalf("expected 'remote-val', got %q", string(rec.Value))
	}
	if _, err := localStore.Get(context.Background(), storage.Key(remoteKey)); err == nil {
		t.Fatal("expected key not to exist in local store")
	}

	// 2. Forwarded GET
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

		handler := NewKVHandler(localStore, localManager)
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

		if wGet.Code != http.StatusBadGateway {
			t.Fatalf("expected 502 Bad Gateway, got %d", wGet.Code)
		}
	})
}
