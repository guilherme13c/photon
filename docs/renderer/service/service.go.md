# Documentation for `service.go`

**Path:** `renderer/service/service.go`

## Overview

This file is part of the `renderer` component.

## Structs
- `Service`

## Functions
- `NewService`
- `Process`

## Source Code

```go
package service

import (
	"context"
	"log"

	"github.com/guilherme13c/renderer/repository/headless_client"
	"github.com/guilherme13c/renderer/repository/kafka/consumer"
	"github.com/guilherme13c/renderer/repository/kafka/producer"
	"github.com/guilherme13c/renderer/repository/storage"
)

type Service struct {
	client        headless_client.Client
	storage       storage.Storage
	producer      producer.Producer
	producerTopic string
}

func NewService(client headless_client.Client, st storage.Storage, pr producer.Producer, producerTopic string) *Service {
	return &Service{
		client:        client,
		storage:       st,
		producer:      pr,
		producerTopic: producerTopic,
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

	// 2. Save to Storage
	doc := storage.Document{
		URL:     url,
		Content: string(content),
	}
	if err := s.storage.Save(ctx, doc); err != nil {
		log.Printf("Failed to save doc %s: %v", url, err)
		return
	}

	// 3. Produce to Kafka for the parser service
	// We pass the URL as the key and content as the value
	if err := s.producer.Produce(ctx, s.producerTopic, []byte(url), content); err != nil {
		log.Printf("Failed to produce message for %s: %v", url, err)
		return
	}


	log.Printf("Successfully processed %s", url)
}


```
