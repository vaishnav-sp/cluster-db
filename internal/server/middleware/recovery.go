package middleware

import (
	"encoding/json"
	"net/http"
	"runtime/debug"

	"go.uber.org/zap"

	clusterlogger "github.com/vaishnav-sp/cluster-db/internal/logger"
)

// Recovery creates middleware that handles panics and returns a 500 response.
func Recovery(logger *zap.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					if logger != nil {
						logger.Error("panic recovered",
							clusterlogger.RequestID(GetRequestID(r.Context())),
							zap.Any("panic", rec),
							zap.String("stack", string(debug.Stack())),
						)
					}

					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					_ = json.NewEncoder(w).Encode(map[string]string{"error": "internal server error"})
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}
