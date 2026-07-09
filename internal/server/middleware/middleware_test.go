package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestChainExecutesInOrder(t *testing.T) {
	var order []string

	handler := Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		order = append(order, "handler")
		w.WriteHeader(http.StatusOK)
	}),
		func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, "first")
				next.ServeHTTP(w, r)
			})
		},
		func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, "second")
				next.ServeHTTP(w, r)
			})
		},
	)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if len(order) != 3 || order[0] != "first" || order[1] != "second" || order[2] != "handler" {
		t.Fatalf("unexpected middleware order: %v", order)
	}
}

func TestRequestIDMiddlewareAddsHeaderAndContext(t *testing.T) {
	handler := Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := GetRequestID(r.Context()); got == "" {
			t.Fatal("expected request ID in context")
		}
		if got := w.Header().Get("X-Request-ID"); got == "" {
			t.Fatal("expected request ID response header")
		}
		w.WriteHeader(http.StatusNoContent)
	}), RequestID())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("unexpected status code: %d", rr.Code)
	}
}

func TestRecoveryMiddlewareReturnsJSONOnPanic(t *testing.T) {
	handler := Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}), Recovery(zap.NewNop()))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("unexpected status code: %d", rr.Code)
	}
	if contentType := rr.Header().Get("Content-Type"); !strings.Contains(contentType, "application/json") {
		t.Fatalf("expected JSON content type, got %q", contentType)
	}
	if body := rr.Body.String(); !strings.Contains(body, `"error":"internal server error"`) {
		t.Fatalf("expected recovery payload, got %q", body)
	}
}

func TestGetRequestIDReturnsEmptyForNilContext(t *testing.T) {
	if got := GetRequestID(nil); got != "" {
		t.Fatalf("expected empty request ID, got %q", got)
	}
}

func TestGetRequestIDReturnsEmptyForContextWithoutValue(t *testing.T) {
	if got := GetRequestID(context.Background()); got != "" {
		t.Fatalf("expected empty request ID, got %q", got)
	}
}
