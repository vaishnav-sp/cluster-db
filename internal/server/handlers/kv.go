package handlers

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/vaishnav-sp/cluster-db/internal/storage"
	storageManager "github.com/vaishnav-sp/cluster-db/internal/storage/manager"
)

// KVHandler handles key-value REST operations using the storage manager.
type KVHandler struct {
	manager *storageManager.Manager
}

// NewKVHandler creates a new KV handler with the storage manager dependency.
func NewKVHandler(manager *storageManager.Manager) *KVHandler {
	return &KVHandler{manager: manager}
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

func (h *KVHandler) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
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

	w.WriteHeader(http.StatusCreated)
}

func (h *KVHandler) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	rec, err := h.manager.Get(r.Context(), storage.Key(key))
	if err != nil {
		h.writeStorageError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(rec.Value)
}

func (h *KVHandler) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	if err := h.manager.Delete(r.Context(), storage.Key(key)); err != nil {
		h.writeStorageError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *KVHandler) handleHead(w http.ResponseWriter, r *http.Request, key string) {
	_, err := h.manager.Get(r.Context(), storage.Key(key))
	if err != nil {
		h.writeStorageError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
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
