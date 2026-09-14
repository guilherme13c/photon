package search

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"testing"
)

type roundTripFunc func(*http.Request) *http.Response

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request), nil
}

func TestHTTPEmbedderSendsTextAndReturnsVectors(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) *http.Response {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/embed" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		var request embedRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(request.Texts, []string{"photon"}) {
			t.Errorf("texts = %#v", request.Texts)
		}
		body, _ := json.Marshal(embedResponse{Embeddings: [][]float32{{0.1, 0.2}}, Dimensions: 2})
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body))}
	})}

	embedder := newHTTPEmbedder("http://embedding", 2, client)
	vector, err := embedder.Embed("photon")
	if err != nil {
		t.Fatalf("Embed() error = %v", err)
	}
	if !reflect.DeepEqual(vector, []float32{0.1, 0.2}) {
		t.Errorf("vector = %#v", vector)
	}
}

func TestHTTPEmbedderRejectsDimensionMismatchAndBadResponses(t *testing.T) {
	tests := []struct {
		name string
		hand func(*http.Request) *http.Response
	}{
		{name: "dimension mismatch", hand: func(_ *http.Request) *http.Response {
			body, _ := json.Marshal(embedResponse{Embeddings: [][]float32{{0.1}}, Dimensions: 1})
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body))}
		}},
		{name: "server error", hand: func(_ *http.Request) *http.Response {
			return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(bytes.NewReader(nil))}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(tc.hand)}
			if _, err := newHTTPEmbedder("http://embedding", 2, client).Embed("photon"); err == nil {
				t.Fatal("Embed() unexpectedly succeeded")
			}
		})
	}
}
