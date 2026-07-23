package handlers

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/vaishnav-sp/cluster-db/internal/document"
	docservice "github.com/vaishnav-sp/cluster-db/internal/document/service"
	"github.com/vaishnav-sp/cluster-db/internal/storage"
)

// DocumentHandler handles JSON document REST operations via the document service.
type DocumentHandler struct {
	service *docservice.Service
}

// NewDocumentHandler creates a document handler backed by the document service.
func NewDocumentHandler(service *docservice.Service) *DocumentHandler {
	return &DocumentHandler{service: service}
}

// ServeHTTP routes document requests under /v1/documents.
func (h *DocumentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.service == nil {
		WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "document service unavailable"})
		return
	}

	id, collection, ok := parseDocumentPath(r)
	if !ok {
		WriteJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}

	if collection {
		if r.Method != http.MethodPost {
			WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		h.handleCreate(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r, id)
	case http.MethodDelete:
		h.handleDelete(w, r, id)
	default:
		WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (h *DocumentHandler) handleCreate(w http.ResponseWriter, r *http.Request) {
	if !isJSONContentType(r.Header.Get("Content-Type")) {
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "content type must be application/json"})
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	doc, err := document.Validate(body)
	if err != nil {
		h.writeDocumentValidationError(w, err)
		return
	}

	id, err := h.service.Create(r.Context(), doc)
	if err != nil {
		h.writeDocumentStorageError(w, err)
		return
	}

	WriteJSON(w, http.StatusCreated, map[string]string{"id": id})
}

func (h *DocumentHandler) handleGet(w http.ResponseWriter, r *http.Request, id string) {
	doc, err := h.service.Get(r.Context(), id)
	if err != nil {
		h.writeDocumentStorageError(w, err)
		return
	}

	WriteJSON(w, http.StatusOK, doc)
}

func (h *DocumentHandler) handleDelete(w http.ResponseWriter, r *http.Request, id string) {
	exists, err := h.service.Exists(r.Context(), id)
	if err != nil {
		h.writeDocumentStorageError(w, err)
		return
	}
	if !exists {
		WriteJSON(w, http.StatusNotFound, map[string]string{"error": "document not found"})
		return
	}

	if err := h.service.Delete(r.Context(), id); err != nil {
		h.writeDocumentStorageError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func parseDocumentPath(r *http.Request) (id string, collection bool, ok bool) {
	if r == nil || r.URL == nil {
		return "", false, false
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "v1" || parts[1] != "documents" {
		return "", false, false
	}

	if len(parts) == 2 {
		return "", true, true
	}
	if len(parts) != 3 {
		return "", false, false
	}

	id = parts[2]
	if id == "" {
		return "", false, false
	}

	return id, false, true
}

func isJSONContentType(contentType string) bool {
	mediaType := strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
	return mediaType == "" || mediaType == "application/json"
}

func (h *DocumentHandler) writeDocumentValidationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, document.ErrInvalidJSON):
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
	case errors.Is(err, document.ErrEmptyDocument):
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid document"})
	default:
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid document"})
	}
}

func (h *DocumentHandler) writeDocumentStorageError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, storage.ErrKeyNotFound):
		WriteJSON(w, http.StatusNotFound, map[string]string{"error": "document not found"})
	case errors.Is(err, context.Canceled):
		WriteJSON(w, http.StatusRequestTimeout, map[string]string{"error": "request canceled"})
	case errors.Is(err, context.DeadlineExceeded):
		WriteJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "request timed out"})
	default:
		WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "storage error"})
	}
}
