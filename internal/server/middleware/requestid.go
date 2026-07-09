package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"
)

type contextKey string

const requestIDContextKey contextKey = "request_id"

// RequestID creates middleware that assigns a request ID to every request.
func RequestID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := newRequestID()
			ctx := context.WithValue(r.Context(), requestIDContextKey, id)
			w.Header().Set("X-Request-ID", id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// GetRequestID extracts the request ID from context.
func GetRequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	id, _ := ctx.Value(requestIDContextKey).(string)
	return id
}

func newRequestID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err == nil {
		return hex.EncodeToString(buf)
	}

	return fmt.Sprintf("%d", time.Now().UnixNano())
}
