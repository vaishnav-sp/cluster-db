package handlers

import (
	"encoding/json"
	"net/http"
)

// WriteJSON writes a JSON payload to the response writer.
func WriteJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
