package main

import (
    "context"
    "fmt"
    "log"

    "github.com/guilherme13c/renderer/repository/headless_client"
)

func main() {
    client := headless_client.NewClient()
    body, err := client.Fetch(context.Background(), "http://localhost:9999/index.html")
    if err != nil {
        log.Fatalf("Error: %v", err)
    }
    fmt.Printf("Fetched Body:\n%s\n", string(body))
}
