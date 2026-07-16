package handlers

import (
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

func (h *KVHandler) handleGet(w http.ResponseWriter, r *http.Request, key string) {
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
		h.handleGetRemote(w, r.Context(), owner, key)
		return
	}

	rec, err := h.manager.Get(r.Context(), storage.Key(key))
	if err != nil {
		h.writeStorageError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(rec.Value)
}

func (h *KVHandler) handleGetRemote(w http.ResponseWriter, ctx context.Context, owner cluster.Node, key string) {
	resp, err := h.client.KVGet(ctx, owner.Address, clusterRPC.KVGetRequest{Key: key})
	if err != nil {
		h.writeGatewayError(w, err)
		return
	}
	if resp.Error != "" {
		h.writeStorageError(w, mapRemoteError(resp.Error))
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(resp.Value)
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
