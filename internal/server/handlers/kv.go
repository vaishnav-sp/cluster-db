package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/vaishnav-sp/cluster-db/internal/cluster"
	clusterRPC "github.com/vaishnav-sp/cluster-db/internal/cluster/rpc"
	"github.com/vaishnav-sp/cluster-db/internal/storage"
	storageManager "github.com/vaishnav-sp/cluster-db/internal/storage/manager"
)

// KVHandler handles key-value REST operations using the storage manager.
type KVHandler struct {
	manager        *storageManager.Manager
	clusterManager *cluster.Manager
	client         *clusterRPC.Client
}

// NewKVHandler creates a new KV handler with the storage manager dependency.
func NewKVHandler(manager *storageManager.Manager, clusterManager *cluster.Manager) *KVHandler {
	return &KVHandler{
		manager:        manager,
		clusterManager: clusterManager,
		client:         clusterRPC.NewClient(5 * time.Second),
	}
}

// ServeHTTP routes KV requests.
func (h *KVHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.manager == nil {
		WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "storage unavailable"})
		return
	}

	key, ok := extractKey(r)
	if !ok {
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid key"})
		return
	}

	switch r.Method {
	case http.MethodPut:
		h.handlePut(w, r, key)
	case http.MethodGet:
		h.handleGet(w, r, key)
	case http.MethodDelete:
		h.handleDelete(w, r, key)
	case http.MethodHead:
		h.handleHead(w, r, key)
	default:
		WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (h *KVHandler) getRoute(key string) (cluster.Node, bool, error) {
	if h.clusterManager == nil {
		return cluster.Node{}, true, nil
	}

	owner, ok := h.clusterManager.Owner(key)
	if !ok {
		return cluster.Node{}, false, fmt.Errorf("no owner node found on consistent hash ring")
	}

	localNode := h.clusterManager.LocalNode()
	if owner.ID == localNode.ID {
		return owner, true, nil
	}

	return owner, false, nil
}

func (h *KVHandler) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	var owners []cluster.Node
	var ok bool
	if h.clusterManager != nil {
		owners, ok = h.clusterManager.Owners(key, h.replicationFactor())
		if !ok || len(owners) == 0 {
			WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no owners found for key"})
			return
		}
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	var primary cluster.Node
	var isLocal bool = true
	var replicas []cluster.Node

	if h.clusterManager != nil {
		primary = owners[0]
		localNode := h.clusterManager.LocalNode()
		isLocal = (localNode.ID != "" && primary.ID == localNode.ID)
		replicas = owners[1:]
	}

	if !isLocal {
		if primary.Status != cluster.Alive {
			WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": fmt.Sprintf("primary node %s is unavailable (status: %v)", primary.ID, primary.Status)})
			return
		}
		h.handlePutRemote(w, r.Context(), primary, key, body)
		return
	}

	rec := storage.Record{Key: storage.Key(key), Value: storage.Value(body)}
	if err := h.manager.Put(r.Context(), rec); err != nil {
		WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "storage write failed"})
		return
	}

	if _, err := h.manager.Get(r.Context(), storage.Key(key)); err != nil {
		WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "storage write failed"})
		return
	}

	for _, replica := range replicas {
		if replica.Status != cluster.Alive {
			WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": fmt.Sprintf("replica node %s is unavailable (status: %v)", replica.ID, replica.Status)})
			return
		}

		resp, err := h.client.ReplicaPut(r.Context(), replica.Address, clusterRPC.ReplicaPutRequest{Key: key, Value: body})
		if err != nil {
			h.writeGatewayError(w, err)
			return
		}
		if resp.Error != "" {
			h.writeStorageError(w, mapRemoteError(resp.Error))
			return
		}
	}

	w.WriteHeader(http.StatusCreated)
}

func (h *KVHandler) handlePutRemote(w http.ResponseWriter, ctx context.Context, owner cluster.Node, key string, body []byte) {
	resp, err := h.client.KVPut(ctx, owner.Address, clusterRPC.KVPutRequest{Key: key, Value: body})
	if err != nil {
		h.writeGatewayError(w, err)
		return
	}
	if resp.Error != "" {
		h.writeStorageError(w, mapRemoteError(resp.Error))
		return
	}
	w.WriteHeader(http.StatusCreated)
}

// handleGet implements topology-aware read routing with failover and async read repair.
//
// Algorithm:
//  1. Determine replica owners from the consistent hash ring.
//  2. If the current node owns the key, read locally.
//  3. Otherwise, attempt ReplicaGet on each owner in order (failover).
//  4. If multiple replicas responded and values differ, asynchronously repair
//     stale replicas using ReplicaPut (read repair, majority wins).
//  5. Return 503 only when every replica fails.
func (h *KVHandler) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	if h.clusterManager == nil {
		// No cluster: read locally.
		h.readLocal(w, r.Context(), key)
		return
	}

	owners, ok := h.clusterManager.Owners(key, h.replicationFactor())
	if !ok || len(owners) == 0 {
		WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no owner node found on consistent hash ring"})
		return
	}

	localNode := h.clusterManager.LocalNode()

	// Check whether the local node is one of the owners.
	for _, o := range owners {
		if h.clusterManager.IsLocalNode(o.ID) || (localNode.ID != "" && o.ID == localNode.ID) {
			h.readLocal(w, r.Context(), key)
			return
		}
	}

	// Remote read with failover across owners.
	value, err := h.replicaReadWithFailover(r.Context(), owners, key)
	if err != nil {
		WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "all replicas failed: " + err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(value)
}

// readLocal reads a key from the local storage manager.
func (h *KVHandler) readLocal(w http.ResponseWriter, ctx context.Context, key string) {
	rec, err := h.manager.Get(ctx, storage.Key(key))
	if err != nil {
		h.writeStorageError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(rec.Value)
}

// replicaReadResult holds the outcome of a single ReplicaGet RPC call.
type replicaReadResult struct {
	node  cluster.Node
	value []byte
	found bool
}

// replicaReadWithFailover attempts ReplicaGet on each owner in order and returns
// the first successful response.  All successful results are collected so that
// read repair can reconcile divergent replicas asynchronously.
func (h *KVHandler) replicaReadWithFailover(ctx context.Context, owners []cluster.Node, key string) ([]byte, error) {
	var lastErr error
	var successes []replicaReadResult

	for _, owner := range owners {
		if owner.Status != cluster.Alive {
			lastErr = fmt.Errorf("node %s is not alive (status: %v)", owner.ID, owner.Status)
			continue
		}

		resp, err := h.client.ReplicaGet(ctx, owner.Address, clusterRPC.ReplicaGetRequest{Key: key})
		if err != nil {
			lastErr = fmt.Errorf("node %s: %w", owner.ID, err)
			continue
		}
		if resp.Error != "" {
			lastErr = fmt.Errorf("node %s: %s", owner.ID, resp.Error)
			continue
		}
		if !resp.Found {
			// Treat not-found as a definitive answer from this replica.
			// We still try other owners in case they have it.
			lastErr = fmt.Errorf("node %s: key not found", owner.ID)
			continue
		}

		successes = append(successes, replicaReadResult{node: owner, value: resp.Value, found: true})
		break // First success: return to client immediately.
	}

	if len(successes) == 0 {
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, fmt.Errorf("key not found on any replica")
	}

	// Collect additional results for read repair (best-effort, no blocking).
	// Attempt remaining owners in background to detect divergence.
	go h.asyncReadRepair(key, owners, successes[0].value)

	return successes[0].value, nil
}

// asyncReadRepair polls the remaining replica owners and repairs any that hold
// a stale value.  The majority value among all successful responses wins.
// This function is called asynchronously and never blocks the caller.
func (h *KVHandler) asyncReadRepair(key string, owners []cluster.Node, firstValue []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Collect all responses (including the first one we already have).
	results := []replicaReadResult{{value: firstValue, found: true}}
	for _, owner := range owners {
		if owner.Status != cluster.Alive {
			continue
		}
		resp, err := h.client.ReplicaGet(ctx, owner.Address, clusterRPC.ReplicaGetRequest{Key: key})
		if err != nil || resp.Error != "" || !resp.Found {
			continue
		}
		results = append(results, replicaReadResult{node: owner, value: resp.Value, found: true})
	}

	if len(results) < 2 {
		return // Nothing to repair.
	}

	canonical := majorityValue(results)
	if canonical == nil {
		return
	}

	// Update stale replicas asynchronously.
	for _, r := range results {
		if r.node.Address == "" {
			continue // skip local pseudo-entry
		}
		if !bytes.Equal(r.value, canonical) {
			// Fire-and-forget; ignore errors.
			repairCtx, repairCancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, _ = h.client.ReplicaPut(repairCtx, r.node.Address, clusterRPC.ReplicaPutRequest{Key: key, Value: canonical})
			repairCancel()
		}
	}
}

// majorityValue returns the value held by the majority of replicas.
// If there is no strict majority, returns the first value encountered.
func majorityValue(results []replicaReadResult) []byte {
	counts := make(map[string]int)
	var keys []string
	for _, r := range results {
		s := string(r.value)
		if _, seen := counts[s]; !seen {
			keys = append(keys, s)
		}
		counts[s]++
	}
	var majority string
	var best int
	for _, k := range keys {
		if counts[k] > best {
			best = counts[k]
			majority = k
		}
	}
	return []byte(majority)
}

func (h *KVHandler) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	var owners []cluster.Node
	var ok bool
	if h.clusterManager != nil {
		owners, ok = h.clusterManager.Owners(key, h.replicationFactor())
		if !ok || len(owners) == 0 {
			WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no owners found for key"})
			return
		}
	}

	var primary cluster.Node
	var isLocal bool = true
	var replicas []cluster.Node

	if h.clusterManager != nil {
		primary = owners[0]
		localNode := h.clusterManager.LocalNode()
		isLocal = (localNode.ID != "" && primary.ID == localNode.ID)
		replicas = owners[1:]
	}

	if !isLocal {
		if primary.Status != cluster.Alive {
			WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": fmt.Sprintf("primary node %s is unavailable (status: %v)", primary.ID, primary.Status)})
			return
		}
		h.handleDeleteRemote(w, r.Context(), primary, key)
		return
	}

	if err := h.manager.Delete(r.Context(), storage.Key(key)); err != nil {
		h.writeStorageError(w, err)
		return
	}

	for _, replica := range replicas {
		if replica.Status != cluster.Alive {
			WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": fmt.Sprintf("replica node %s is unavailable (status: %v)", replica.ID, replica.Status)})
			return
		}

		resp, err := h.client.ReplicaDelete(r.Context(), replica.Address, clusterRPC.ReplicaDeleteRequest{Key: key})
		if err != nil {
			h.writeGatewayError(w, err)
			return
		}
		if resp.Error != "" {
			h.writeStorageError(w, mapRemoteError(resp.Error))
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *KVHandler) handleDeleteRemote(w http.ResponseWriter, ctx context.Context, owner cluster.Node, key string) {
	resp, err := h.client.KVDelete(ctx, owner.Address, clusterRPC.KVDeleteRequest{Key: key})
	if err != nil {
		h.writeGatewayError(w, err)
		return
	}
	if resp.Error != "" {
		h.writeStorageError(w, mapRemoteError(resp.Error))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *KVHandler) handleHead(w http.ResponseWriter, r *http.Request, key string) {
	owner, local, err := h.getRoute(key)
	if err != nil {
		WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}

	if !local {
		if owner.Status != cluster.Alive {
			WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": fmt.Sprintf("owner node %s is unavailable (status: %v)", owner.ID, owner.Status)})
			return
		}
		h.handleHeadRemote(w, r.Context(), owner, key)
		return
	}

	_, err = h.manager.Get(r.Context(), storage.Key(key))
	if err != nil {
		h.writeStorageError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *KVHandler) handleHeadRemote(w http.ResponseWriter, ctx context.Context, owner cluster.Node, key string) {
	resp, err := h.client.KVGet(ctx, owner.Address, clusterRPC.KVGetRequest{Key: key})
	if err != nil {
		h.writeGatewayError(w, err)
		return
	}
	if resp.Error != "" {
		h.writeStorageError(w, mapRemoteError(resp.Error))
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *KVHandler) writeGatewayError(w http.ResponseWriter, err error) {
	if errors.Is(err, context.Canceled) {
		WriteJSON(w, http.StatusRequestTimeout, map[string]string{"error": "request canceled"})
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		WriteJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "request timed out"})
		return
	}
	WriteJSON(w, http.StatusBadGateway, map[string]string{"error": fmt.Sprintf("routing request to owner failed: %v", err)})
}

func mapRemoteError(errStr string) error {
	switch errStr {
	case "storage: key not found":
		return storage.ErrKeyNotFound
	case "storage: invalid key":
		return storage.ErrInvalidKey
	case "storage: nil value":
		return storage.ErrNilValue
	case "storage: engine is closed":
		return storage.ErrEngineClosed
	case "storage: engine is not open":
		return storage.ErrEngineNotOpen
	default:
		return errors.New(errStr)
	}
}

func (h *KVHandler) writeStorageError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, storage.ErrKeyNotFound):
		WriteJSON(w, http.StatusNotFound, map[string]string{"error": "key not found"})
	case errors.Is(err, storage.ErrInvalidKey):
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid key"})
	case errors.Is(err, storage.ErrNilValue):
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid value"})
	case errors.Is(err, context.Canceled):
		WriteJSON(w, http.StatusRequestTimeout, map[string]string{"error": "request canceled"})
	case errors.Is(err, context.DeadlineExceeded):
		WriteJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "request timed out"})
	default:
		WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "storage error"})
	}
}

func extractKey(r *http.Request) (string, bool) {
	if r == nil || r.URL == nil {
		return "", false
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "v1" || parts[1] != "kv" {
		return "", false
	}

	key := strings.Join(parts[2:], "/")
	if key == "" {
		return "", false
	}

	return key, true
}

func (h *KVHandler) replicationFactor() int {
	if h.clusterManager == nil || h.clusterManager.ReplicationFactor <= 0 {
		return 1
	}
	return h.clusterManager.ReplicationFactor
}
