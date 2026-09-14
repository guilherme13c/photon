package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"time"

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
	processDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "fetcher_process_duration_seconds", Help: "End-to-end Fetcher message processing duration",
		Buckets: prometheus.DefBuckets,
	})
	stageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "fetcher_stage_duration_seconds",
		Help:    "Duration of bounded Fetcher pipeline stages",
		Buckets: prometheus.DefBuckets,
	}, []string{"stage"})
	inFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "fetcher_in_flight", Help: "Fetcher messages currently being processed",
	})
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

// Process returns an error only when the input has not reached a durable next
// state. Callers must not commit the Kafka offset in that case.
func (s *Service) Process(ctx context.Context, msg consumer.Message) error {
	started := time.Now()
	inFlight.Inc()
	defer func() { inFlight.Dec(); processDuration.Observe(time.Since(started).Seconds()) }()
	url := string(msg.Value)
	correlationID := newCorrelationID()
	log.Printf("event=fetch_started correlation_id=%s url=%q partition=%d offset=%d", correlationID, url, msg.Partition, msg.Offset)

	// 1. Fetch HTML
	fetchStarted := time.Now()
	response, err := s.client.Fetch(ctx, url)
	stageDuration.WithLabelValues("origin_fetch").Observe(time.Since(fetchStarted).Seconds())
	if err != nil {
		urlsProcessed.WithLabelValues("fetch_error").Inc()
		log.Printf("Failed to fetch %s: %v, sending to DLQ...", url, err)
		if dlqErr := s.producer.Produce(ctx, s.dlqTopic, []byte(url), []byte(err.Error())); dlqErr != nil {
			log.Printf("Failed to send %s to DLQ: %v", url, dlqErr)
			return fmt.Errorf("publish fetch failure to DLQ: %w", dlqErr)
		}
		return nil
	}
	if response.RedirectURL != "" {
		// The target is a new candidate, not an in-worker follow-up. Frontier
		// normalizes it, applies robots, deduplicates it, and reserves the
		// target host before it can be fetched.
		if err := s.producer.Produce(ctx, s.dynamicTopic, admissionKey(response.RedirectURL), []byte(response.RedirectURL)); err != nil {
			return fmt.Errorf("publish redirect target: %w", err)
		}
		urlsProcessed.WithLabelValues("redirect").Inc()
		return nil
	}

	contentStr := string(response.Body)

	// 2. Heuristic Check
	if s.isDynamic(contentStr) {
		urlsProcessed.WithLabelValues("dynamic").Inc()
		log.Printf("URL %s classified as dynamic, routing to renderer...", url)
		// Rendering is a second request to this host. Send it back through the
		// frontier so it receives another shared politeness slot.
		if err := s.producer.Produce(ctx, s.dynamicTopic, admissionKey(url), []byte("render:"+url)); err != nil {
			log.Printf("Failed to produce to dynamic topic for %s: %v", url, err)
			return fmt.Errorf("publish dynamic URL: %w", err)
		}
		return nil
	}

	// 3. Save to Storage (Static)
	doc := storage.Document{
		URL:     url,
		Content: contentStr,
	}
	storageStarted := time.Now()
	s3Key, err := s.storage.Save(ctx, doc)
	stageDuration.WithLabelValues("object_store_save").Observe(time.Since(storageStarted).Seconds())
	if err != nil {
		urlsProcessed.WithLabelValues("storage_error").Inc()
		log.Printf("Failed to save doc %s: %v", url, err)
		return fmt.Errorf("save document: %w", err)
	}

	// 4. Produce to Kafka for the parser service
	payload, err := json.Marshal(struct {
		URL                 string `json:"url"`
		S3Key               string `json:"s3_key"`
		PipelineStartedAtMS int64  `json:"pipeline_started_at_ms"`
		CorrelationID       string `json:"correlation_id"`
	}{URL: url, S3Key: s3Key, PipelineStartedAtMS: started.UnixMilli(), CorrelationID: correlationID})
	if err != nil {
		urlsProcessed.WithLabelValues("serialize_error").Inc()
		return fmt.Errorf("serialize fetched page: %w", err)
	}
	produceStarted := time.Now()
	if err := s.producer.Produce(ctx, s.producerTopic, []byte(url), payload); err != nil {
		stageDuration.WithLabelValues("produce_fetched_page").Observe(time.Since(produceStarted).Seconds())
		urlsProcessed.WithLabelValues("kafka_error").Inc()
		log.Printf("Failed to produce message for %s: %v", url, err)
		return fmt.Errorf("publish fetched page: %w", err)
	}
	stageDuration.WithLabelValues("produce_fetched_page").Observe(time.Since(produceStarted).Seconds())

	urlsProcessed.WithLabelValues("success").Inc()
	log.Printf("event=fetch_succeeded correlation_id=%s url=%q", correlationID, url)
	return nil
}

// newCorrelationID is deliberately opaque and carried in event envelopes and
// logs. It is never used as a Prometheus label, where its cardinality would be
// unbounded. A trace backend may use it as a searchable attribute.
func newCorrelationID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		// crypto/rand failures are exceptional; retain a non-empty ID so an
		// individual item is still searchable while avoiding a process crash.
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("%x", value)
}

func admissionKey(rawURL string) []byte {
	parsed, err := url.Parse(rawURL)
	if err == nil && parsed.Host != "" {
		return []byte(parsed.Host)
	}
	return []byte(rawURL)
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
