package handlers

import (
	"fmt"
	"net/http"
	"time"
)

// HealthHandler returns service health information.
type HealthHandler struct {
	Service   string
	Version   string
	StartedAt time.Time
}

// NewHealthHandler builds a health handler with required dependencies.
func NewHealthHandler(service, version string, startedAt time.Time) *HealthHandler {
	return &HealthHandler{
		Service:   service,
		Version:   version,
		StartedAt: startedAt,
	}
}

// ServeHTTP handles health requests.
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/health":
		h.handleHealth(w, r)
	case "/live":
		h.handleLive(w, r)
	case "/ready":
		h.handleReady(w, r)
	default:
		WriteJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	}
}

func (h *HealthHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	payload := map[string]any{
		"status":    "ok",
		"service":   h.Service,
		"version":   h.Version,
		"uptime":    formatUptime(time.Since(h.StartedAt)),
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
	}

	WriteJSON(w, http.StatusOK, payload)
}

func (h *HealthHandler) handleLive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	WriteJSON(w, http.StatusOK, map[string]string{"status": "alive"})
}

func (h *HealthHandler) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		WriteJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func formatUptime(d time.Duration) string {
	if d < 0 {
		d = 0
	}

	seconds := int(d / time.Second)
	minutes := seconds / 60
	hours := minutes / 60
	days := hours / 24

	return fmt.Sprintf("%dd%02dh%02dm%02ds", days, hours%24, minutes%60, seconds%60)
}
