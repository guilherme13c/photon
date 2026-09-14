package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/guilherme13c/photon/search"
)

func main() {
	config := search.LoadConfig(nil)
	embedder := search.NewHTTPEmbedder(config.EmbedderURL, config.VectorDimensions)
	qdrant := search.NewQdrantRepository(config.QdrantURL, config.QdrantCollection, config.QdrantAPIKey, nil)
	ready := func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return embedder.Ready(ctx) && qdrant.Ready(ctx)
	}
	server := &http.Server{Addr: config.ListenAddr, Handler: search.NewSearchServer(ready, embedder, qdrant).Handler()}
	log.Printf("search service listening on %s", config.ListenAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
