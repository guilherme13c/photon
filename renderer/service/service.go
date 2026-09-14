package service

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/guilherme13c/renderer/repository/headless_client"
	"github.com/guilherme13c/renderer/repository/kafka/consumer"
	"github.com/guilherme13c/renderer/repository/kafka/producer"
	"github.com/guilherme13c/renderer/repository/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	pagesRenderedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "renderer_pages_rendered_total",
		Help: "The total number of pages rendered",
	}, []string{"status"})
	renderDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "renderer_process_duration_seconds", Help: "End-to-end Renderer message processing duration",
		Buckets: prometheus.DefBuckets,
	})
	renderInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "renderer_in_flight", Help: "Renderer messages currently being processed",
	})
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
	started := time.Now()
	renderInFlight.Inc()
	defer func() { renderInFlight.Dec(); renderDuration.Observe(time.Since(started).Seconds()) }()
	url := string(msg.Value)
	log.Printf("Fetching URL: %s", url)

	// 1. Fetch HTML
	content, err := s.client.Fetch(ctx, url)
	if err != nil {
		log.Printf("Failed to fetch %s: %v", url, err)
		pagesRenderedTotal.WithLabelValues("fetch_error").Inc()
		return
	}

	// 2. Save to Storage
	doc := storage.Document{
		URL:     url,
		Content: string(content),
	}
	s3Key, err := s.storage.Save(ctx, doc)
	if err != nil {
		log.Printf("Failed to save doc %s: %v", url, err)
		pagesRenderedTotal.WithLabelValues("storage_error").Inc()
		return
	}

	// 3. Produce the same storage reference envelope consumed by the extractor.
	payload, err := json.Marshal(struct {
		URL                 string `json:"url"`
		S3Key               string `json:"s3_key"`
		PipelineStartedAtMS int64  `json:"pipeline_started_at_ms"`
	}{
		URL:                 url,
		S3Key:               s3Key,
		PipelineStartedAtMS: started.UnixMilli(),
	})
	if err != nil {
		log.Printf("Failed to marshal message for %s: %v", url, err)
		pagesRenderedTotal.WithLabelValues("produce_error").Inc()
		return
	}
	if err := s.producer.Produce(ctx, s.producerTopic, []byte(url), payload); err != nil {
		log.Printf("Failed to produce message for %s: %v", url, err)
		pagesRenderedTotal.WithLabelValues("produce_error").Inc()
		return
	}

	log.Printf("Successfully processed %s", url)
	pagesRenderedTotal.WithLabelValues("success").Inc()
}
