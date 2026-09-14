package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type QdrantRepository struct {
	baseURL    string
	collection string
	apiKey     string
	client     *http.Client
}

type SearchRepository interface {
	Search(ctx context.Context, vector []float32, limit, offset int) ([]SearchResult, error)
}

type qdrantQueryRequest struct {
	Vector      []float32 `json:"query"`
	Limit       int       `json:"limit"`
	Offset      int       `json:"offset"`
	WithPayload bool      `json:"with_payload"`
}

type qdrantQueryResponse struct {
	Result struct {
		Points []struct {
			ID      json.RawMessage `json:"id"`
			Score   float32         `json:"score"`
			Payload map[string]any  `json:"payload"`
		} `json:"points"`
	} `json:"result"`
}

func NewQdrantRepository(baseURL, collection, apiKey string, client *http.Client) *QdrantRepository {
	if client == nil {
		client = http.DefaultClient
	}
	return &QdrantRepository{baseURL: strings.TrimRight(baseURL, "/"), collection: collection, apiKey: apiKey, client: client}
}

func (q *QdrantRepository) Search(ctx context.Context, vector []float32, limit, offset int) ([]SearchResult, error) {
	payload, err := json.Marshal(qdrantQueryRequest{Vector: vector, Limit: limit, Offset: offset, WithPayload: true})
	if err != nil {
		return nil, fmt.Errorf("encode qdrant query: %w", err)
	}
	endpoint := q.baseURL + "/collections/" + q.collection + "/points/query"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create qdrant query: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if q.apiKey != "" {
		request.Header.Set("api-key", q.apiKey)
	}
	response, err := q.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("qdrant query: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("qdrant returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read qdrant response: %w", err)
	}
	var decoded qdrantQueryResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("decode qdrant response: %w", err)
	}
	results := make([]SearchResult, 0, len(decoded.Result.Points))
	for _, point := range decoded.Result.Points {
		id, err := qdrantID(point.ID)
		if err != nil {
			return nil, err
		}
		results = append(results, SearchResult{ID: id, Score: point.Score, URL: stringValue(point.Payload["url"]), Title: stringValue(point.Payload["title"]), Text: stringValue(point.Payload["text"]), ChunkIndex: intValue(point.Payload["chunk_index"])})
	}
	return results, nil
}

func qdrantID(raw json.RawMessage) (string, error) {
	var stringID string
	if json.Unmarshal(raw, &stringID) == nil {
		return stringID, nil
	}
	var number json.Number
	if json.Unmarshal(raw, &number) == nil {
		return number.String(), nil
	}
	return "", fmt.Errorf("invalid qdrant point id")
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func intValue(value any) int {
	switch number := value.(type) {
	case float64:
		return int(number)
	case string:
		parsed, _ := strconv.Atoi(number)
		return parsed
	default:
		return 0
	}
}
