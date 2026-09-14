package main

import (
	"log"
	"net/http"

	"github.com/guilherme13c/photon/search"
)

func main() {
	config := search.LoadConfig(nil)
	embedder := search.NewHTTPEmbedder(config.EmbedderURL, config.VectorDimensions)
	qdrant := search.NewQdrantRepository(config.QdrantURL, config.QdrantCollection, config.QdrantAPIKey, nil)
	server := &http.Server{Addr: config.ListenAddr, Handler: search.NewSearchServer(func() bool { return true }, embedder, qdrant).Handler()}
	log.Printf("search service listening on %s", config.ListenAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
