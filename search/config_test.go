package search

import "testing"

func TestLoadConfigUsesDefaults(t *testing.T) {
	config := LoadConfig(func(string) string { return "" })
	if config.ListenAddr != ":8082" || config.EmbedderURL != "http://embedder:8002" || config.QdrantURL != "http://qdrant:6333" {
		t.Fatalf("LoadConfig() = %#v", config)
	}
	if config.QdrantCollection != "photon_documents" || config.VectorDimensions != 384 {
		t.Fatalf("LoadConfig() storage defaults = %#v", config)
	}
}

func TestLoadConfigReadsOverrides(t *testing.T) {
	values := map[string]string{
		"SEARCH_LISTEN_ADDR": ":9092", "SEARCH_EMBEDDER_URL": "http://embedder",
		"QDRANT_URL": "http://qdrant:6333", "QDRANT_API_KEY": "secret",
		"QDRANT_COLLECTION_NAME": "docs", "SEARCH_VECTOR_DIMENSIONS": "768",
	}
	config := LoadConfig(func(key string) string { return values[key] })
	if config.ListenAddr != ":9092" || config.EmbedderURL != "http://embedder" || config.QdrantAPIKey != "secret" || config.VectorDimensions != 768 {
		t.Fatalf("LoadConfig() = %#v", config)
	}
}
