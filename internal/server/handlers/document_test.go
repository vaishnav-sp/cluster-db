package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"

	"github.com/vaishnav-sp/cluster-db/internal/config"
	docservice "github.com/vaishnav-sp/cluster-db/internal/document/service"
	"github.com/vaishnav-sp/cluster-db/internal/storage"
	"github.com/vaishnav-sp/cluster-db/internal/storage/manager"
)

func setupDocumentHandlerTest(t *testing.T) (*DocumentHandler, *manager.Manager) {
	t.Helper()

	store, err := manager.New(config.StorageConfig{Engine: "memory"}, zap.NewNop())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	if err := store.Open(context.Background()); err != nil {
		t.Fatalf("open store: %v", err)
	}

	t.Cleanup(func() {
		_ = store.Close(context.Background())
	})

	svc := docservice.New(store)
	return NewDocumentHandler(svc), store
}

func newDocumentRequest(method, path string, body []byte, contentType string) *http.Request {
	var reader *bytes.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return req
}

func createDocumentViaHandler(t *testing.T, handler *DocumentHandler, body string) string {
	t.Helper()

	req := newDocumentRequest(http.MethodPost, "/v1/documents", []byte(body), "application/json")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body = %q", rr.Code, rr.Body.String())
	}

	var payload map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	id := payload["id"]
	if id == "" {
		t.Fatalf("expected non-empty id in response %q", rr.Body.String())
	}
	return id
}

func TestDocumentHandlerPostValidDocument(t *testing.T) {
	handler, store := setupDocumentHandlerTest(t)

	body := `{"name":"Alice","age":24,"city":"Chennai"}`
	req := newDocumentRequest(http.MethodPost, "/v1/documents", []byte(body), "application/json")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body = %q", rr.Code, http.StatusCreated, rr.Body.String())
	}
	if !strings.Contains(rr.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("expected JSON content type, got %q", rr.Header().Get("Content-Type"))
	}

	var created map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	id := created["id"]
	if id == "" {
		t.Fatalf("expected id in response, got %q", rr.Body.String())
	}

	ctx := context.Background()
	rec, err := store.Get(ctx, storage.Key(id))
	if err != nil {
		t.Fatalf("store get: %v", err)
	}

	var stored map[string]any
	if err := json.Unmarshal(rec.Value, &stored); err != nil {
		t.Fatalf("decode stored document: %v", err)
	}
	if stored["_id"] != id {
		t.Fatalf("stored _id = %v, want %q", stored["_id"], id)
	}
	if stored["name"] != "Alice" {
		t.Fatalf("stored name = %v", stored["name"])
	}
}

func TestDocumentHandlerPostValidationErrors(t *testing.T) {
	handler, _ := setupDocumentHandlerTest(t)

	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantError  string
	}{
		{name: "invalid json", body: `{`, wantStatus: http.StatusBadRequest, wantError: "invalid json"},
		{name: "json array", body: `[1,2,3]`, wantStatus: http.StatusBadRequest, wantError: "invalid json"},
		{name: "json string primitive", body: `"hello"`, wantStatus: http.StatusBadRequest, wantError: "invalid json"},
		{name: "json number primitive", body: `42`, wantStatus: http.StatusBadRequest, wantError: "invalid json"},
		{name: "empty object", body: `{}`, wantStatus: http.StatusBadRequest, wantError: "invalid document"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := newDocumentRequest(http.MethodPost, "/v1/documents", []byte(tc.body), "application/json")
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			if rr.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d, body = %q", rr.Code, tc.wantStatus, rr.Body.String())
			}
			assertErrorJSON(t, rr.Body.String(), tc.wantError)
		})
	}
}

func TestDocumentHandlerPostRejectsNonJSONContentType(t *testing.T) {
	handler, _ := setupDocumentHandlerTest(t)

	req := newDocumentRequest(http.MethodPost, "/v1/documents", []byte(`{"name":"Alice"}`), "text/plain")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
	assertErrorJSON(t, rr.Body.String(), "content type must be application/json")
}

func TestDocumentHandlerGetExistingDocument(t *testing.T) {
	handler, _ := setupDocumentHandlerTest(t)

	id := createDocumentViaHandler(t, handler, `{"name":"Bob","score":99}`)

	req := httptest.NewRequest(http.MethodGet, "/v1/documents/"+id, nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %q", rr.Code, rr.Body.String())
	}

	var got map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode get response: %v", err)
	}

	want := map[string]any{"_id": id, "name": "Bob", "score": float64(99)}
	if !mapsEqual(got, want) {
		t.Fatalf("GET body = %#v, want %#v", got, want)
	}
}

func TestDocumentHandlerGetMissingDocument(t *testing.T) {
	handler, _ := setupDocumentHandlerTest(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/documents/01ARZ3NDEKTSV4RRFFQ69G5FAV", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d, body = %q", rr.Code, http.StatusNotFound, rr.Body.String())
	}
	assertErrorJSON(t, rr.Body.String(), "document not found")
}

func TestDocumentHandlerDeleteExistingDocument(t *testing.T) {
	handler, store := setupDocumentHandlerTest(t)

	id := createDocumentViaHandler(t, handler, `{"name":"ToDelete"}`)

	req := httptest.NewRequest(http.MethodDelete, "/v1/documents/"+id, nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d, body = %q", rr.Code, http.StatusNoContent, rr.Body.String())
	}
	if rr.Body.Len() != 0 {
		t.Fatalf("expected empty body for 204, got %q", rr.Body.String())
	}

	_, err := store.Get(context.Background(), storage.Key(id))
	if err == nil {
		t.Fatal("expected document to be removed from storage")
	}
	if !strings.Contains(err.Error(), storage.ErrKeyNotFound.Error()) {
		t.Fatalf("expected key not found, got %v", err)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/v1/documents/"+id, nil)
	getRR := httptest.NewRecorder()
	handler.ServeHTTP(getRR, getReq)
	if getRR.Code != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want %d", getRR.Code, http.StatusNotFound)
	}
}

func TestDocumentHandlerDeleteMissingDocument(t *testing.T) {
	handler, _ := setupDocumentHandlerTest(t)

	req := httptest.NewRequest(http.MethodDelete, "/v1/documents/01ARZ3NDEKTSV4RRFFQ69G5FAV", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d, body = %q", rr.Code, http.StatusNotFound, rr.Body.String())
	}
	assertErrorJSON(t, rr.Body.String(), "document not found")
}

func TestDocumentHandlerUnsupportedMethodsAndPaths(t *testing.T) {
	handler, _ := setupDocumentHandlerTest(t)

	id := createDocumentViaHandler(t, handler, `{"name":"PathTest"}`)

	methodTests := []struct {
		method string
		path   string
		want   int
	}{
		{method: http.MethodGet, path: "/v1/documents", want: http.StatusMethodNotAllowed},
		{method: http.MethodPut, path: "/v1/documents", want: http.StatusMethodNotAllowed},
		{method: http.MethodPatch, path: "/v1/documents/" + id, want: http.StatusMethodNotAllowed},
		{method: http.MethodPost, path: "/v1/documents/" + id, want: http.StatusMethodNotAllowed},
	}

	for _, tc := range methodTests {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			if rr.Code != tc.want {
				t.Fatalf("status = %d, want %d, body = %q", rr.Code, tc.want, rr.Body.String())
			}
			assertErrorJSON(t, rr.Body.String(), "method not allowed")
		})
	}

	notFoundPaths := []string{
		"/v1/kv/documents",
		"/v1/documents/extra/nested",
	}
	for _, path := range notFoundPaths {
		t.Run("not found "+path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			if rr.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want %d, body = %q", rr.Code, http.StatusNotFound, rr.Body.String())
			}
			assertErrorJSON(t, rr.Body.String(), "not found")
		})
	}

	// Trailing slash normalizes to the collection path; GET is not allowed there.
	t.Run("collection trailing slash GET", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/documents/", nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d, want %d, body = %q", rr.Code, http.StatusMethodNotAllowed, rr.Body.String())
		}
		assertErrorJSON(t, rr.Body.String(), "method not allowed")
	})
}

func assertErrorJSON(t *testing.T, body, wantError string) {
	t.Helper()

	var payload map[string]string
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("decode error body: %v, raw = %q", err, body)
	}
	if payload["error"] != wantError {
		t.Fatalf("error = %q, want %q, body = %q", payload["error"], wantError, body)
	}
}

func mapsEqual(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if !valuesEqual(v, b[k]) {
			return false
		}
	}
	return true
}

func valuesEqual(a, b any) bool {
	switch av := a.(type) {
	case float64:
		bv, ok := b.(float64)
		return ok && av == bv
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	default:
		return false
	}
}
