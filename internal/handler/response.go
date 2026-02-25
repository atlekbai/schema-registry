package handler

import (
	"encoding/json"
	"net/http"
)

type ListResponse struct {
	TotalCount int64            `json:"total_count"`
	NextCursor *string          `json:"next_cursor,omitempty"`
	Results    []map[string]any `json:"results"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Code    string `json:"code"`
	Details string `json:"details,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message, details string) {
	writeJSON(w, status, ErrorResponse{
		Error:   message,
		Code:    code,
		Details: details,
	})
}
