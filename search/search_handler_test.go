package search

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

type fakeEmbedder struct {
	vector []float32
	err    error
}

func (f fakeEmbedder) Embed(string) ([]float32, error) { return f.vector, f.err }

type fakeSearchRepository struct {
	vector []float32
	limit  int
	offset int
	result []SearchResult
	err    error
}

func (f *fakeSearchRepository) Search(_ context.Context, vector []float32, limit, offset int) ([]SearchResult, error) {
	f.vector, f.limit, f.offset = vector, limit, offset
	return f.result, f.err
}

func TestSearchHandlerReturnsResultsAndNextCursor(t *testing.T) {
	repository := &fakeSearchRepository{result: []SearchResult{{ID: "one", Score: 0.9}}}
	server := NewSearchServer(func() bool { return true }, fakeEmbedder{vector: []float32{1, 2}}, repository)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/search?q=%20photon%20&limit=1", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response SearchResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 || response.NextCursor == "" || repository.offset != 0 || repository.limit != 1 || !reflect.DeepEqual(repository.vector, []float32{1, 2}) {
		t.Fatalf("response = %#v, repository = %#v", response, repository)
	}
}

func TestSearchHandlerRejectsInvalidCursorAndUnavailableDependencies(t *testing.T) {
	server := NewSearchServer(func() bool { return true }, fakeEmbedder{vector: []float32{1}}, &fakeSearchRepository{})
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/search?q=photon&cursor=invalid", nil))
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid cursor status = %d", recorder.Code)
	}
	server = NewSearchServer(func() bool { return true }, nil, nil)
	recorder = httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/search?q=photon", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("unavailable status = %d", recorder.Code)
	}
}
