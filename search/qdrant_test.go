package search

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"testing"
)

func TestQdrantRepositorySearchMapsPayloadAndPagination(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) *http.Response {
		if r.Method != http.MethodPost || r.URL.Path != "/collections/docs/points/query" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		var request qdrantQueryRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request.Limit != 2 || request.Offset != 4 || !request.WithPayload || !reflect.DeepEqual(request.Vector, []float32{0.1, 0.2}) {
			t.Errorf("query request = %#v", request)
		}
		body := `{"result":{"points":[{"id":"doc#chunk:4","score":0.91,"payload":{"url":"https://example.com","title":"Example","text":"text","chunk_index":4}}]}}`
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewBufferString(body))}
	})}
	repository := NewQdrantRepository("http://qdrant", "docs", "", client)
	results, err := repository.Search(context.Background(), []float32{0.1, 0.2}, 2, 4)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	want := []SearchResult{{ID: "doc#chunk:4", Score: 0.91, URL: "https://example.com", Title: "Example", Text: "text", ChunkIndex: 4}}
	if !reflect.DeepEqual(results, want) {
		t.Fatalf("Search() = %#v, want %#v", results, want)
	}
}

func TestQdrantRepositorySearchReportsBackendErrors(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) *http.Response {
		return &http.Response{StatusCode: http.StatusBadGateway, Body: io.NopCloser(bytes.NewBufferString("failed"))}
	})}
	repository := NewQdrantRepository("http://qdrant", "docs", "", client)
	if _, err := repository.Search(context.Background(), []float32{0.1}, 1, 0); err == nil {
		t.Fatal("Search() unexpectedly succeeded")
	}
}
