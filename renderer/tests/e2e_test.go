package tests

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/guilherme13c/renderer/repository/kafka/consumer"
	"github.com/guilherme13c/renderer/repository/kafka/producer"
	"github.com/guilherme13c/renderer/repository/storage"
	"github.com/guilherme13c/renderer/service"
	kafkalib "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

type testStorage struct {
	savedDocument storage.Document
}

type testHeadlessClient struct{}

func (testHeadlessClient) Fetch(_ context.Context, _ string) ([]byte, error) {
	return []byte("<html><body>E2E Test Content</body></html>"), nil
}

func (s *testStorage) Save(ctx context.Context, doc storage.Document) (string, error) {
	s.savedDocument = doc
	return "e2e-rendered-page.html", nil
}

func (s *testStorage) Close() error {
	return nil
}

func TestFetcherE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e test in short mode")
	}
	ctx := context.Background()

	// 1. Spin up Kafka using testcontainers
	kafkaContainer, err := kafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		t.Fatalf("failed to start kafka container: %s", err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}()

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get brokers: %s", err)
	}
	broker := brokers[0]

	inputTopic := "e2e-urls"
	outputTopic := "e2e-fetched-pages"

	const fixtureURL = "http://fixture.test/dynamic"

	// 3. Initialize Fetcher components
	c := consumer.NewConsumer(broker, inputTopic, "fetcher-group")
	defer c.Close()

	p := producer.NewProducer(broker)
	defer p.Close()

	hClient := testHeadlessClient{}
	st := &testStorage{}

	svc := service.NewService(hClient, st, p, outputTopic)

	// 4. Start Fetcher logic in a goroutine
	ctxCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		for {
			select {
			case <-ctxCancel.Done():
				return
			default:
				msg, err := c.Consume(ctxCancel)
				if err != nil {
					continue
				}
				svc.Process(ctxCancel, msg)
			}
		}
	}()

	// 6. Produce a URL to the input topic with retry for auto-topic creation

	testProducer := producer.NewProducer(broker)
	defer testProducer.Close()

	var errProduce error
	for i := 0; i < 5; i++ {
		time.Sleep(2 * time.Second) // wait for topics/consumer to settle
		errProduce = testProducer.Produce(ctx, inputTopic, []byte("key"), []byte(fixtureURL))
		if errProduce == nil {
			break
		}
		t.Logf("produce failed (retrying): %v", errProduce)
	}
	if errProduce != nil {
		t.Fatalf("failed to produce test URL after retries: %v", errProduce)
	}

	// Give fetcher some time to process the URL and produce the output message
	time.Sleep(5 * time.Second)

	// 7. Assert output from the output topic using a raw Reader to read from FirstOffset
	reader := kafkalib.NewReader(kafkalib.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       outputTopic,
		Partition:   0,
		MinBytes:    10e3,
		MaxBytes:    10e6,
		StartOffset: kafkalib.FirstOffset,
	})
	defer reader.Close()

	ctxTimeout, cancelTimeout := context.WithTimeout(ctx, 15*time.Second)
	defer cancelTimeout()

	msg, err := reader.ReadMessage(ctxTimeout)
	if err != nil {
		t.Fatalf("failed to read output message: %v", err)
	}

	if string(msg.Key) != fixtureURL {
		t.Errorf("expected key %s, got %s", fixtureURL, string(msg.Key))
	}
	var payload struct {
		URL   string `json:"url"`
		S3Key string `json:"s3_key"`
	}
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		t.Fatalf("expected JSON storage-reference envelope, got %q: %v", msg.Value, err)
	}
	if payload.URL != fixtureURL || payload.S3Key != "e2e-rendered-page.html" {
		t.Errorf("expected payload for %s with key e2e-rendered-page.html, got %+v", fixtureURL, payload)
	}
	if st.savedDocument.URL != fixtureURL || st.savedDocument.Content != "<html><body>E2E Test Content</body></html>" {
		t.Errorf("expected rendered content to be saved before publishing, got %+v", st.savedDocument)
	}
}
