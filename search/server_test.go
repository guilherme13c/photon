package search

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServerHealthAndReadiness(t *testing.T) {
	ready := false
	server := NewServer(func() bool { return ready })

	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("/healthz status = %d", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("unready /readyz status = %d", recorder.Code)
	}
	ready = true
	recorder = httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("ready /readyz status = %d", recorder.Code)
	}
}
