# Documentation for `service.go`

**Path:** `fetcher/service/service.go`

## Overview

This file is part of the `fetcher` component.

## Structs
- `Service`

## Functions
- `NewService`
- `Process`
- `isDynamic`

## Source Code

```go
package service

import (
	"context"
	"log"

	"strings"

	"github.com/guilherme13c/fetcher/repository/http_client"
	"github.com/guilherme13c/fetcher/repository/kafka/consumer"
	"github.com/guilherme13c/fetcher/repository/kafka/producer"
	"github.com/guilherme13c/fetcher/repository/storage"
)

type Service struct {
	client        http_client.Client
	storage       storage.Storage
	producer      producer.Producer
	producerTopic string
	dynamicTopic  string
}

func NewService(client http_client.Client, st storage.Storage, pr producer.Producer, producerTopic string, dynamicTopic string) *Service {
	return &Service{
		client:        client,
		storage:       st,
		producer:      pr,
		producerTopic: producerTopic,
		dynamicTopic:  dynamicTopic,
	}
}

func (s *Service) Process(ctx context.Context, msg consumer.Message) {
	url := string(msg.Value)
	log.Printf("Fetching URL: %s", url)

	// 1. Fetch HTML
	content, err := s.client.Fetch(ctx, url)
	if err != nil {
		log.Printf("Failed to fetch %s: %v", url, err)
		return
	}

	contentStr := string(content)

	// 2. Heuristic Check
	if s.isDynamic(contentStr) {
		log.Printf("URL %s classified as dynamic, routing to renderer...", url)
		if err := s.producer.Produce(ctx, s.dynamicTopic, []byte(url), []byte(url)); err != nil {
			log.Printf("Failed to produce to dynamic topic for %s: %v", url, err)
		}
		return
	}

	// 3. Save to Storage (Static)
	doc := storage.Document{
		URL:     url,
		Content: contentStr,
	}
	if err := s.storage.Save(ctx, doc); err != nil {
		log.Printf("Failed to save doc %s: %v", url, err)
		return
	}

	// 4. Produce to Kafka for the parser service
	if err := s.producer.Produce(ctx, s.producerTopic, []byte(url), content); err != nil {
		log.Printf("Failed to produce message for %s: %v", url, err)
		return
	}

	log.Printf("Successfully processed %s", url)
}

func (s *Service) isDynamic(html string) bool {
	// Simple heuristic: check for common SPA mount points or framework signatures
	lowerHtml := strings.ToLower(html)
	if strings.Contains(lowerHtml, `<div id="root"></div>`) || strings.Contains(lowerHtml, `<div id="app"></div>`) {
		return true
	}
	if strings.Contains(html, "window.__INITIAL_STATE__") || strings.Contains(html, "__NEXT_DATA__") {
		return true
	}
	return false
}


```
