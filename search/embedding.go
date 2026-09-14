package search

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Embedder interface {
	Embed(text string) ([]float32, error)
}

type HTTPEmbedder struct {
	baseURL    string
	dimensions int
	client     *http.Client
}

type embedRequest struct {
	Texts []string `json:"texts"`
}

type embedResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
	Dimensions int         `json:"dimensions"`
}

func NewHTTPEmbedder(baseURL string, dimensions int) *HTTPEmbedder {
	return newHTTPEmbedder(baseURL, dimensions, &http.Client{Timeout: 10 * time.Second})
}

func newHTTPEmbedder(baseURL string, dimensions int, client *http.Client) *HTTPEmbedder {
	return &HTTPEmbedder{
		baseURL:    strings.TrimRight(baseURL, "/"),
		dimensions: dimensions,
		client:     client,
	}
}

func (e *HTTPEmbedder) Embed(text string) ([]float32, error) {
	payload, err := json.Marshal(embedRequest{Texts: []string{text}})
	if err != nil {
		return nil, fmt.Errorf("encode embedding request: %w", err)
	}
	request, err := http.NewRequest(http.MethodPost, e.baseURL+"/v1/embed", bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create embedding request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := e.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("embedding request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("embedding service returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read embedding response: %w", err)
	}
	var decoded embedResponse
	if err := json.Unmarshal(body, &decoded); err != nil || len(decoded.Embeddings) != 1 {
		return nil, fmt.Errorf("invalid embedding response")
	}
	vector := decoded.Embeddings[0]
	if decoded.Dimensions != e.dimensions || len(vector) != e.dimensions {
		return nil, fmt.Errorf("embedding dimension mismatch: got %d, want %d", len(vector), e.dimensions)
	}
	return vector, nil
}
