package middleware

import (
	"net/http"
	"time"

	"go.uber.org/zap"

	clusterlogger "github.com/vaishnav-sp/cluster-db/internal/logger"
)

type statusResponseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusResponseWriter) WriteHeader(statusCode int) {
	if w.wroteHeader {
		return
	}

	w.status = statusCode
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *statusResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}

	return w.ResponseWriter.Write(b)
}

// Logging creates middleware that records request details.
func Logging(logger *zap.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if logger == nil {
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()
			wrapped := &statusResponseWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(wrapped, r)

			logger.Info("request completed",
				clusterlogger.RequestID(GetRequestID(r.Context())),
				zap.String("method", r.Method),
				zap.String("path", requestPath(r)),
				zap.Int("status_code", wrapped.status),
				clusterlogger.Duration(time.Since(start)),
				clusterlogger.Address(r.RemoteAddr),
				zap.String("user_agent", r.UserAgent()),
			)
		})
	}
}

func requestPath(r *http.Request) string {
	if r == nil || r.URL == nil {
		return "/"
	}

	if r.URL.Path != "" {
		return r.URL.Path
	}

	return "/"
}
