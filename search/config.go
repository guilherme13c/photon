package search

import (
	"os"
	"strconv"
)

type Config struct {
	ListenAddr       string
	EmbedderURL      string
	QdrantURL        string
	QdrantAPIKey     string
	QdrantCollection string
	VectorDimensions int
}

func LoadConfig(getenv func(string) string) Config {
	if getenv == nil {
		getenv = os.Getenv
	}
	return Config{
		ListenAddr:       valueOr(getenv("SEARCH_LISTEN_ADDR"), ":8082"),
		EmbedderURL:      valueOr(getenv("SEARCH_EMBEDDER_URL"), "http://embedder:8002"),
		QdrantURL:        valueOr(getenv("QDRANT_URL"), "http://qdrant:6333"),
		QdrantAPIKey:     getenv("QDRANT_API_KEY"),
		QdrantCollection: valueOr(getenv("QDRANT_COLLECTION_NAME"), "photon_documents"),
		VectorDimensions: intOr(getenv("SEARCH_VECTOR_DIMENSIONS"), 384),
	}
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func intOr(value string, fallback int) int {
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
