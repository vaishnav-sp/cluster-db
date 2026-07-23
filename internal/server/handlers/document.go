package handlers

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
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
		if r.Method == http.MethodGet {
			h.handleFind(w, r)
			return
		}
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

func (h *DocumentHandler) handleFind(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	_, hasField := query["field"]
	_, hasValue := query["value"]

	switch {
	case hasField && !hasValue:
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "missing value parameter"})
		return
	case hasValue && !hasField:
		WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "missing field parameter"})
		return
	case !hasField && !hasValue:
		WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	if _, hasAggregate := query["aggregate"]; hasAggregate {
		aggregate := query.Get("aggregate")
		if !isSupportedAggregate(aggregate) {
			WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid aggregate parameter"})
			return
		}

		docs, err := h.service.FindByField(r.Context(), query.Get("field"), query.Get("value"), -1, 0, query.Get("sort"))
		if err != nil {
			h.writeDocumentStorageError(w, err)
			return
		}
		result, err := h.service.AggregateDocuments(docs, aggregate)
		if err != nil {
			h.writeDocumentStorageError(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, result)
		return
	}

	limit := -1
	var err error
	if rawLimit, ok := query["limit"]; ok {
		limit, err = strconv.Atoi(rawLimit[0])
		if err != nil || limit < 0 {
			WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid limit parameter"})
			return
		}
	}

	offset := 0
	if rawOffset, ok := query["offset"]; ok {
		offset, err = strconv.Atoi(rawOffset[0])
		if err != nil || offset < 0 {
			WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid offset parameter"})
			return
		}
	}

	docs, err := h.service.FindByField(r.Context(), query.Get("field"), query.Get("value"), limit, offset, query.Get("sort"))
	if err != nil {
		h.writeDocumentStorageError(w, err)
		return
	}

	WriteJSON(w, http.StatusOK, docs)
}

func isSupportedAggregate(aggregate string) bool {
	if aggregate == "count" {
		return true
	}

	parts := strings.SplitN(aggregate, ":", 2)
	if len(parts) != 2 || parts[1] == "" {
		return false
	}

	switch parts[0] {
	case "sum", "avg", "min", "max":
		return true
	default:
		return false
	}
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
