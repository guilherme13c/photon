package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/guilherme13c/fetcher/repository/http_client"
	"github.com/guilherme13c/fetcher/repository/kafka/consumer"
	"github.com/guilherme13c/fetcher/repository/storage"
)

type mockClient struct {
	content     []byte
	redirectURL string
	err         error
}

func (m *mockClient) Fetch(ctx context.Context, url string) (http_client.Response, error) {
	return http_client.Response{Body: m.content, RedirectURL: m.redirectURL}, m.err
}

type mockStorage struct {
	savedDoc *storage.Document
	err      error
}

func (m *mockStorage) Save(ctx context.Context, doc storage.Document) (string, error) {
	m.savedDoc = &doc
	return "mocked-s3-key.html", m.err
}

func (m *mockStorage) Close() error {
	return nil
}

type mockProducer struct {
	topic string
	key   []byte
	value []byte
	err   error
}

func (m *mockProducer) Produce(ctx context.Context, topic string, key []byte, value []byte) error {
	m.topic = topic
	m.key = key
	m.value = value
	return m.err
}

func (m *mockProducer) Close() error {
	return nil
}

func TestServiceProcess_Success(t *testing.T) {
	client := &mockClient{content: []byte("<html>Hello</html>")}
	store := &mockStorage{}
	prod := &mockProducer{}
	producerTopic := "test-topic"

	svc := NewService(client, store, prod, producerTopic, "dynamic-urls", "fetcher-dlq")

	msg := consumer.Message{Value: []byte("http://example.com")}
	svc.Process(context.Background(), msg)

	if store.savedDoc == nil {
		t.Fatal("expected document to be saved")
	}
	if store.savedDoc.URL != "http://example.com" {
		t.Errorf("expected URL http://example.com, got %s", store.savedDoc.URL)
	}
	if store.savedDoc.Content != "<html>Hello</html>" {
		t.Errorf("expected content <html>Hello</html>, got %s", store.savedDoc.Content)
	}

	if prod.topic != "test-topic" {
		t.Errorf("expected producer topic test-topic, got %s", prod.topic)
	}
	if string(prod.key) != "http://example.com" {
		t.Errorf("expected producer key http://example.com, got %s", string(prod.key))
	}
	var payload struct {
		URL                 string `json:"url"`
		S3Key               string `json:"s3_key"`
		PipelineStartedAtMS int64  `json:"pipeline_started_at_ms"`
	}
	if err := json.Unmarshal(prod.value, &payload); err != nil {
		t.Fatalf("expected JSON storage-reference envelope, got %q: %v", prod.value, err)
	}
	if payload.URL != "http://example.com" || payload.S3Key != "mocked-s3-key.html" || payload.PipelineStartedAtMS <= 0 {
		t.Errorf("unexpected producer payload: %+v", payload)
	}
}

func TestServiceProcess_FetchError(t *testing.T) {
	client := &mockClient{err: errors.New("fetch error")}
	store := &mockStorage{}
	prod := &mockProducer{}
	producerTopic := "test-topic"

	svc := NewService(client, store, prod, producerTopic, "dynamic-urls", "fetcher-dlq")

	msg := consumer.Message{Value: []byte("http://example.com")}
	svc.Process(context.Background(), msg)

	if store.savedDoc != nil {
		t.Fatal("expected document NOT to be saved on fetch error")
	}
	if prod.topic != "fetcher-dlq" {
		t.Errorf("expected producer to send to fetcher-dlq on fetch error, got %s", prod.topic)
	}
	if string(prod.value) != "fetch error" {
		t.Errorf("expected dlq message to be fetch error, got %s", string(prod.value))
	}
}

func TestServiceProcess_DynamicPageReturnsToFrontierScheduler(t *testing.T) {
	client := &mockClient{content: []byte(`<div id="root"></div>`)}
	store := &mockStorage{}
	prod := &mockProducer{}
	svc := NewService(client, store, prod, "fetched-pages", "discovered-urls", "fetcher-dlq")

	svc.Process(context.Background(), consumer.Message{Value: []byte("http://example.com/app")})

	if store.savedDoc != nil {
		t.Fatal("dynamic page should not be stored by the fetcher")
	}
	if prod.topic != "discovered-urls" {
		t.Fatalf("expected dynamic request to return to the frontier, got topic %q", prod.topic)
	}
	if got, want := string(prod.value), "render:http://example.com/app"; got != want {
		t.Errorf("expected scheduled render marker %q, got %q", want, got)
	}
}

func TestServiceProcess_RedirectReturnsTargetToFrontierScheduler(t *testing.T) {
	client := &mockClient{redirectURL: "http://example.com/target"}
	store := &mockStorage{}
	prod := &mockProducer{}
	svc := NewService(client, store, prod, "fetched-pages", "discovered-urls", "fetcher-dlq")

	if err := svc.Process(context.Background(), consumer.Message{Value: []byte("http://example.com/source")}); err != nil {
		t.Fatalf("process redirect: %v", err)
	}
	if store.savedDoc != nil {
		t.Fatal("redirect response must not be stored")
	}
	if prod.topic != "discovered-urls" || string(prod.value) != "http://example.com/target" {
		t.Fatalf("redirect target bypassed Frontier: topic=%q value=%q", prod.topic, prod.value)
	}
}
