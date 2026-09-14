package search

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	DefaultPageSize = 10
	MaxPageSize     = 50
	MaxQueryBytes   = 4096
)

var (
	ErrEmptyQuery       = errors.New("query must not be empty")
	ErrQueryTooLong     = errors.New("query is too long")
	ErrInvalidPageSize  = errors.New("limit must be between 1 and 50")
	ErrInvalidCursor    = errors.New("invalid cursor")
	ErrCursorQueryMatch = errors.New("cursor does not match query")
)

type SearchRequest struct {
	Query  string
	Limit  int
	Cursor string
}

type SearchResult struct {
	ID         string  `json:"id"`
	Score      float32 `json:"score"`
	URL        string  `json:"url,omitempty"`
	Title      string  `json:"title,omitempty"`
	Text       string  `json:"text,omitempty"`
	ChunkIndex int     `json:"chunk_index"`
}

type SearchResponse struct {
	Results    []SearchResult `json:"results"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

type Cursor struct {
	QueryHash string `json:"q"`
	Offset    int    `json:"o"`
}

type encodedCursor struct {
	QueryHash string `json:"q"`
	Offset    int    `json:"o"`
}

func ValidateRequest(query string, limit int, cursor string) (SearchRequest, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return SearchRequest{}, ErrEmptyQuery
	}
	if len(query) > MaxQueryBytes || !utf8.ValidString(query) {
		return SearchRequest{}, ErrQueryTooLong
	}
	if limit == 0 {
		limit = DefaultPageSize
	}
	if limit < 1 || limit > MaxPageSize {
		return SearchRequest{}, ErrInvalidPageSize
	}
	return SearchRequest{Query: query, Limit: limit, Cursor: cursor}, nil
}

func EncodeCursor(query string, offset int) (string, error) {
	if offset < 0 {
		return "", ErrInvalidCursor
	}
	payload, err := json.Marshal(encodedCursor{QueryHash: queryHash(query), Offset: offset})
	if err != nil {
		return "", fmt.Errorf("encode cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func DecodeCursor(query string, value string) (Cursor, error) {
	if value == "" {
		return Cursor{}, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return Cursor{}, ErrInvalidCursor
	}
	var decoded encodedCursor
	if err := json.Unmarshal(payload, &decoded); err != nil || decoded.Offset < 0 || decoded.QueryHash == "" {
		return Cursor{}, ErrInvalidCursor
	}
	if decoded.QueryHash != queryHash(query) {
		return Cursor{}, ErrCursorQueryMatch
	}
	return Cursor{QueryHash: decoded.QueryHash, Offset: decoded.Offset}, nil
}

func queryHash(query string) string {
	hash := sha256.Sum256([]byte(strings.TrimSpace(query)))
	return base64.RawURLEncoding.EncodeToString(hash[:])
}
