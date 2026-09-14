package search

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

type Server struct {
	ready      func() bool
	embedder   Embedder
	repository SearchRepository
	metrics    *Metrics
}

func NewServer(ready func() bool) *Server {
	return NewSearchServer(ready, nil, nil)
}

func NewSearchServer(ready func() bool, embedder Embedder, repository SearchRepository) *Server {
	if ready == nil {
		ready = func() bool { return false }
	}
	return &Server{ready: ready, embedder: embedder, repository: repository, metrics: NewMetrics()}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeStatus(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready() {
			writeStatus(w, http.StatusServiceUnavailable, "not ready")
			return
		}
		writeStatus(w, http.StatusOK, "ready")
	})
	mux.HandleFunc("GET /v1/search", s.search)
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(s.metrics.Prometheus()))
	})
	return mux
}

func (s *Server) search(w http.ResponseWriter, request *http.Request) {
	started := time.Now()
	status := httpStatusOK
	resultCount := 0
	defer func() { s.metrics.Observe(status, resultCount, time.Since(started)) }()
	query, err := ValidateRequest(request.URL.Query().Get("q"), parseLimit(request.URL.Query().Get("limit")), request.URL.Query().Get("cursor"))
	if err != nil {
		status = httpStatusBadRequest
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	cursor, err := DecodeCursor(query.Query, query.Cursor)
	if err != nil {
		status = httpStatusBadRequest
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if s.embedder == nil || s.repository == nil {
		status = http.StatusServiceUnavailable
		writeJSONError(w, http.StatusServiceUnavailable, "search dependencies are not configured")
		return
	}
	vector, err := s.embedder.Embed(query.Query)
	if err != nil {
		status = http.StatusBadGateway
		writeJSONError(w, http.StatusBadGateway, "embedding service unavailable")
		return
	}
	results, err := s.repository.Search(request.Context(), vector, query.Limit, cursor.Offset)
	if err != nil {
		status = http.StatusBadGateway
		writeJSONError(w, http.StatusBadGateway, "qdrant unavailable")
		return
	}
	resultCount = len(results)
	response := SearchResponse{Results: results}
	if len(results) == query.Limit {
		response.NextCursor, _ = EncodeCursor(query.Query, cursor.Offset+len(results))
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func parseLimit(value string) int {
	if value == "" {
		return 0
	}
	limit, err := strconv.Atoi(value)
	if err != nil {
		return -1
	}
	return limit
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func writeStatus(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = fmt.Fprintln(w, message)
}
