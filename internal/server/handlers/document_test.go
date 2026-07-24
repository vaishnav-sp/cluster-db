package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/vaishnav-sp/cluster-db/internal/cluster"
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
	return NewDocumentHandler(svc, nil), store
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

func TestDocumentHandlerLocalShardRouting(t *testing.T) {
	store, err := manager.New(config.StorageConfig{Engine: "memory"}, zap.NewNop())
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	if err := store.Open(context.Background()); err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close(context.Background())

	membership := cluster.NewMembership()
	membership.AddNode(cluster.Node{ID: "local", Address: "local", Status: cluster.Alive})
	clusterManager := cluster.NewManager(membership, zap.NewNop(), time.Second, time.Second, "local", "local")
	service := docservice.New(store)
	handler := NewDocumentHandler(service, clusterManager)

	id := createDocumentViaHandler(t, handler, `{"city":"Chennai"}`)
	if _, err := service.Get(context.Background(), id); err != nil {
		t.Fatalf("local routed document was not stored locally: %v", err)
	}
}

func TestDocumentHandlerRemoteShardRouting(t *testing.T) {
	remoteStore, err := manager.New(config.StorageConfig{Engine: "memory"}, zap.NewNop())
	if err != nil {
		t.Fatalf("create remote store: %v", err)
	}
	if err := remoteStore.Open(context.Background()); err != nil {
		t.Fatalf("open remote store: %v", err)
	}
	defer remoteStore.Close(context.Background())

	remoteMembership := cluster.NewMembership()
	remoteMembership.AddNode(cluster.Node{ID: "remote", Status: cluster.Alive})
	remoteManager := cluster.NewManager(remoteMembership, zap.NewNop(), time.Second, time.Second, "remote", "")
	remoteService := docservice.New(remoteStore)
	remoteHandler := NewDocumentHandler(remoteService, remoteManager)
	remoteServer := httptest.NewServer(remoteHandler)
	defer remoteServer.Close()

	remoteAddress := strings.TrimPrefix(remoteServer.URL, "http://")
	remoteMembership.AddNode(cluster.Node{ID: "remote", Address: remoteAddress, Status: cluster.Alive})

	localStore, err := manager.New(config.StorageConfig{Engine: "memory"}, zap.NewNop())
	if err != nil {
		t.Fatalf("create local store: %v", err)
	}
	if err := localStore.Open(context.Background()); err != nil {
		t.Fatalf("open local store: %v", err)
	}
	defer localStore.Close(context.Background())

	originMembership := cluster.NewMembership()
	originMembership.AddNode(cluster.Node{ID: "remote", Address: remoteAddress, Status: cluster.Alive})
	originManager := cluster.NewManager(originMembership, zap.NewNop(), time.Second, time.Second, "origin", "")
	originHandler := NewDocumentHandler(docservice.New(localStore), originManager)

	id := createDocumentViaHandler(t, originHandler, `{"city":"Chennai"}`)
	if _, err := remoteService.Get(context.Background(), id); err != nil {
		t.Fatalf("remote routed document was not stored remotely: %v", err)
	}
	if _, err := localStore.Get(context.Background(), storage.Key(id)); err == nil {
		t.Fatal("remote routed document was unexpectedly stored locally")
	}

	getRequest := httptest.NewRequest(http.MethodGet, "/v1/documents/"+id, nil)
	getRecorder := httptest.NewRecorder()
	originHandler.ServeHTTP(getRecorder, getRequest)
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("remote GET status = %d, body = %q", getRecorder.Code, getRecorder.Body.String())
	}

	deleteRequest := httptest.NewRequest(http.MethodDelete, "/v1/documents/"+id, nil)
	deleteRecorder := httptest.NewRecorder()
	originHandler.ServeHTTP(deleteRecorder, deleteRequest)
	if deleteRecorder.Code != http.StatusNoContent {
		t.Fatalf("remote DELETE status = %d, body = %q", deleteRecorder.Code, deleteRecorder.Body.String())
	}
	if _, err := remoteService.Get(context.Background(), id); err == nil {
		t.Fatal("remote routed document still exists after DELETE")
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

func TestDocumentHandlerQuerySingleDocument(t *testing.T) {
	handler, _ := setupDocumentHandlerTest(t)
	id := createDocumentViaHandler(t, handler, `{"name":"Alice","city":"Chennai"}`)

	req := httptest.NewRequest(http.MethodGet, "/v1/documents?field=city&value=Chennai", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %q", rr.Code, http.StatusOK, rr.Body.String())
	}
	var docs []map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &docs); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if len(docs) != 1 || docs[0]["_id"] != id || docs[0]["name"] != "Alice" {
		t.Fatalf("query response = %#v, want document %q", docs, id)
	}
}

func TestDocumentHandlerQueryMultipleDocuments(t *testing.T) {
	handler, _ := setupDocumentHandlerTest(t)
	firstID := createDocumentViaHandler(t, handler, `{"name":"Alice","city":"Chennai"}`)
	secondID := createDocumentViaHandler(t, handler, `{"name":"Bob","city":"Chennai"}`)
	createDocumentViaHandler(t, handler, `{"name":"Carol","city":"Bengaluru"}`)

	req := httptest.NewRequest(http.MethodGet, "/v1/documents?field=city&value=Chennai", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %q", rr.Code, http.StatusOK, rr.Body.String())
	}
	var docs []map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &docs); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("query returned %d documents, want 2", len(docs))
	}
	gotIDs := map[string]bool{docs[0]["_id"].(string): true, docs[1]["_id"].(string): true}
	if !gotIDs[firstID] || !gotIDs[secondID] {
		t.Fatalf("query IDs = %v, want %q and %q", gotIDs, firstID, secondID)
	}
}

func TestDocumentHandlerQueryNoResultsReturnsEmptyArray(t *testing.T) {
	handler, _ := setupDocumentHandlerTest(t)
	createDocumentViaHandler(t, handler, `{"city":"Chennai"}`)

	req := httptest.NewRequest(http.MethodGet, "/v1/documents?field=city&value=Delhi", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %q", rr.Code, http.StatusOK, rr.Body.String())
	}
	if strings.TrimSpace(rr.Body.String()) != "[]" {
		t.Fatalf("body = %q, want []", rr.Body.String())
	}
}

func TestDocumentHandlerQueryPagination(t *testing.T) {
	handler, _ := setupDocumentHandlerTest(t)
	firstID := createDocumentViaHandler(t, handler, `{"city":"Chennai","name":"first"}`)
	secondID := createDocumentViaHandler(t, handler, `{"city":"Chennai","name":"second"}`)
	thirdID := createDocumentViaHandler(t, handler, `{"city":"Chennai","name":"third"}`)

	tests := []struct {
		name    string
		query   string
		wantIDs []string
	}{
		{name: "limit", query: "limit=2", wantIDs: []string{firstID, secondID}},
		{name: "offset", query: "offset=1", wantIDs: []string{secondID, thirdID}},
		{name: "limit and offset", query: "limit=1&offset=1", wantIDs: []string{secondID}},
		{name: "limit larger than dataset", query: "limit=10", wantIDs: []string{firstID, secondID, thirdID}},
		{name: "offset beyond dataset", query: "limit=2&offset=10", wantIDs: []string{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/documents?field=city&value=Chennai&"+tc.query, nil)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d, body = %q", rr.Code, http.StatusOK, rr.Body.String())
			}
			var docs []map[string]any
			if err := json.Unmarshal(rr.Body.Bytes(), &docs); err != nil {
				t.Fatalf("decode query response: %v", err)
			}
			if len(docs) != len(tc.wantIDs) {
				t.Fatalf("query returned %d documents, want %d", len(docs), len(tc.wantIDs))
			}
			for i, wantID := range tc.wantIDs {
				if docs[i]["_id"] != wantID {
					t.Fatalf("document %d ID = %v, want %q", i, docs[i]["_id"], wantID)
				}
			}
		})
	}
}

func TestDocumentHandlerQuerySortingBeforePagination(t *testing.T) {
	handler, _ := setupDocumentHandlerTest(t)
	createDocumentViaHandler(t, handler, `{"city":"Chennai","age":30}`)
	createDocumentViaHandler(t, handler, `{"city":"Chennai","age":10}`)
	thirdID := createDocumentViaHandler(t, handler, `{"city":"Chennai","age":20}`)

	tests := []struct {
		name   string
		query  string
		wantID string
	}{
		{name: "ascending", query: "sort=age&limit=1&offset=1", wantID: thirdID},
		{name: "descending", query: "sort=-age&limit=1&offset=1", wantID: thirdID},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/documents?field=city&value=Chennai&"+tc.query, nil)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d, body = %q", rr.Code, http.StatusOK, rr.Body.String())
			}
			var docs []map[string]any
			if err := json.Unmarshal(rr.Body.Bytes(), &docs); err != nil {
				t.Fatalf("decode query response: %v", err)
			}
			if len(docs) != 1 || docs[0]["_id"] != tc.wantID {
				t.Fatalf("query response = %#v, want document %q", docs, tc.wantID)
			}
		})
	}

}

func TestDocumentHandlerQueryAggregation(t *testing.T) {
	handler, _ := setupDocumentHandlerTest(t)
	createDocumentViaHandler(t, handler, `{"city":"Chennai","age":10}`)
	createDocumentViaHandler(t, handler, `{"city":"Chennai","age":20}`)
	createDocumentViaHandler(t, handler, `{"city":"Chennai","age":"not numeric"}`)
	createDocumentViaHandler(t, handler, `{"city":"Bengaluru","age":100}`)

	normalRequest := httptest.NewRequest(http.MethodGet, "/v1/documents?field=city&value=Chennai", nil)
	normalRecorder := httptest.NewRecorder()
	handler.ServeHTTP(normalRecorder, normalRequest)
	if normalRecorder.Code != http.StatusOK {
		t.Fatalf("normal query status = %d, want %d, body = %q", normalRecorder.Code, http.StatusOK, normalRecorder.Body.String())
	}
	var normalDocs []map[string]any
	if err := json.Unmarshal(normalRecorder.Body.Bytes(), &normalDocs); err != nil {
		t.Fatalf("decode normal query response: %v", err)
	}
	if len(normalDocs) != 3 {
		t.Fatalf("normal query returned %d documents, want 3", len(normalDocs))
	}

	tests := []struct {
		name      string
		aggregate string
		field     string
		want      float64
	}{
		{name: "count after filtering", aggregate: "count", field: "count", want: 3},
		{name: "sum", aggregate: "sum:age", field: "sum", want: 30},
		{name: "average", aggregate: "avg:age", field: "average", want: 15},
		{name: "minimum", aggregate: "min:age", field: "minimum", want: 10},
		{name: "maximum", aggregate: "max:age", field: "maximum", want: 20},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/documents?field=city&value=Chennai&aggregate="+tc.aggregate, nil)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d, body = %q", rr.Code, http.StatusOK, rr.Body.String())
			}
			var result map[string]any
			if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
				t.Fatalf("decode aggregate response: %v", err)
			}
			if result[tc.field] != tc.want {
				t.Fatalf("aggregate response = %v, want %s=%v", result, tc.field, tc.want)
			}
		})
	}
}

func TestDocumentHandlerAggregationIgnoresPaginationAndHandlesEmptyValues(t *testing.T) {
	handler, _ := setupDocumentHandlerTest(t)
	createDocumentViaHandler(t, handler, `{"city":"Chennai","age":10}`)
	createDocumentViaHandler(t, handler, `{"city":"Chennai","age":20}`)

	req := httptest.NewRequest(http.MethodGet, "/v1/documents?field=city&value=Chennai&aggregate=count&limit=1&offset=1", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %q", rr.Code, http.StatusOK, rr.Body.String())
	}
	var countResult map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &countResult); err != nil {
		t.Fatalf("decode count response: %v", err)
	}
	if countResult["count"] != float64(2) {
		t.Fatalf("count response = %v, want 2 despite pagination", countResult)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/documents?field=city&value=Unknown&aggregate=sum:age", nil)
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("empty status = %d, want %d, body = %q", rr.Code, http.StatusOK, rr.Body.String())
	}
	var emptyResult map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &emptyResult); err != nil {
		t.Fatalf("decode empty aggregate response: %v", err)
	}
	if emptyResult["field"] != "age" || emptyResult["value"] != nil {
		t.Fatalf("empty aggregate response = %v, want age=null", emptyResult)
	}
}

func TestDocumentHandlerQueryRejectsInvalidPagination(t *testing.T) {
	handler, _ := setupDocumentHandlerTest(t)

	tests := []struct {
		name      string
		query     string
		wantError string
	}{
		{name: "negative limit", query: "limit=-1", wantError: "invalid limit parameter"},
		{name: "non-integer limit", query: "limit=abc", wantError: "invalid limit parameter"},
		{name: "negative offset", query: "offset=-1", wantError: "invalid offset parameter"},
		{name: "non-integer offset", query: "offset=abc", wantError: "invalid offset parameter"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/documents?field=city&value=Chennai&"+tc.query, nil)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d, body = %q", rr.Code, http.StatusBadRequest, rr.Body.String())
			}
			assertErrorJSON(t, rr.Body.String(), tc.wantError)
		})
	}
}

func TestDocumentHandlerQueryValidatesParameters(t *testing.T) {
	handler, _ := setupDocumentHandlerTest(t)

	tests := []struct {
		name      string
		path      string
		wantError string
	}{
		{name: "missing value", path: "/v1/documents?field=city", wantError: "missing value parameter"},
		{name: "missing field", path: "/v1/documents?value=Chennai", wantError: "missing field parameter"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d, body = %q", rr.Code, http.StatusBadRequest, rr.Body.String())
			}
			assertErrorJSON(t, rr.Body.String(), tc.wantError)
		})
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
