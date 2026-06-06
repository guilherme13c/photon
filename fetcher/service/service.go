package service

import (
	"context"
	"fmt"
	"log"

	"strings"

	"github.com/guilherme13c/fetcher/repository/http_client"
	"github.com/guilherme13c/fetcher/repository/kafka/consumer"
	"github.com/guilherme13c/fetcher/repository/kafka/producer"
	"github.com/guilherme13c/fetcher/repository/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	urlsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fetcher_urls_processed_total",
		Help: "The total number of processed URLs",
	}, []string{"status"})
)

type Service struct {
	client        http_client.Client
	storage       storage.Storage
	producer      producer.Producer
	producerTopic string
	dynamicTopic  string
	dlqTopic      string
}

func NewService(client http_client.Client, st storage.Storage, pr producer.Producer, producerTopic string, dynamicTopic string, dlqTopic string) *Service {
	return &Service{
		client:        client,
		storage:       st,
		producer:      pr,
		producerTopic: producerTopic,
		dynamicTopic:  dynamicTopic,
		dlqTopic:      dlqTopic,
	}
}

func (s *Service) Process(ctx context.Context, msg consumer.Message) {
	url := string(msg.Value)
	log.Printf("Fetching URL: %s", url)

	// 1. Fetch HTML
	content, err := s.client.Fetch(ctx, url)
	if err != nil {
		urlsProcessed.WithLabelValues("fetch_error").Inc()
		log.Printf("Failed to fetch %s: %v, sending to DLQ...", url, err)
		if dlqErr := s.producer.Produce(ctx, s.dlqTopic, []byte(url), []byte(err.Error())); dlqErr != nil {
			log.Printf("Failed to send %s to DLQ: %v", url, dlqErr)
		}
		return
	}

	contentStr := string(content)

	// 2. Heuristic Check
	if s.isDynamic(contentStr) {
		urlsProcessed.WithLabelValues("dynamic").Inc()
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
	s3Key, err := s.storage.Save(ctx, doc)
	if err != nil {
		urlsProcessed.WithLabelValues("storage_error").Inc()
		log.Printf("Failed to save doc %s: %v", url, err)
		return
	}

	// 4. Produce to Kafka for the parser service
	payload := fmt.Sprintf(`{"url": "%s", "s3_key": "%s"}`, url, s3Key)
	if err := s.producer.Produce(ctx, s.producerTopic, []byte(url), []byte(payload)); err != nil {
		urlsProcessed.WithLabelValues("kafka_error").Inc()
		log.Printf("Failed to produce message for %s: %v", url, err)
		return
	}

	urlsProcessed.WithLabelValues("success").Inc()
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

