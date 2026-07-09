package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHealthHandlerReturnsJSONPayload(t *testing.T) {
	handler := NewHealthHandler("clusterdb", "dev", time.Unix(0, 0).UTC())

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d", rr.Code)
	}
	if contentType := rr.Header().Get("Content-Type"); !strings.Contains(contentType, "application/json") {
		t.Fatalf("expected JSON content type, got %q", contentType)
	}
	if body := rr.Body.String(); !strings.Contains(body, `"status":"ok"`) {
		t.Fatalf("expected health payload, got %q", body)
	}
}

func TestLiveAndReadyHandlersReturnJSON(t *testing.T) {
	handler := NewHealthHandler("clusterdb", "dev", time.Now())

	for _, tc := range []struct {
		path string
		body string
	}{
		{path: "/live", body: `"status":"alive"`},
		{path: "/ready", body: `"status":"ready"`},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("unexpected status code for %s: %d", tc.path, rr.Code)
		}
		if body := rr.Body.String(); !strings.Contains(body, tc.body) {
			t.Fatalf("expected %s payload, got %q", tc.path, body)
		}
	}
}

func TestHealthHandlerReturnsJSONForMethodNotAllowed(t *testing.T) {
	handler := NewHealthHandler("clusterdb", "dev", time.Now())

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("unexpected status code: %d", rr.Code)
	}
	if contentType := rr.Header().Get("Content-Type"); !strings.Contains(contentType, "application/json") {
		t.Fatalf("expected JSON content type, got %q", contentType)
	}
	if body := rr.Body.String(); !strings.Contains(body, `"error":"method not allowed"`) {
		t.Fatalf("expected method-not-allowed payload, got %q", body)
	}
}
