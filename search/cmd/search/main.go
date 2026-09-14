package main

import (
	"log"
	"net/http"

	"github.com/guilherme13c/photon/search"
)

func main() {
	config := search.LoadConfig(nil)
	server := &http.Server{Addr: config.ListenAddr, Handler: search.NewServer(func() bool { return true }).Handler()}
	log.Printf("search service listening on %s", config.ListenAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
