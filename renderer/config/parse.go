package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

func Parse() (*Cfg, error) {
	_ = godotenv.Load() // Ignore error as it might be set in environment directly

	maxRoutines, _ := strconv.Atoi(os.Getenv("MAX_ROUTINES"))
	if maxRoutines == 0 {
		maxRoutines = 10
	}
	browserConcurrency, _ := strconv.Atoi(os.Getenv("RENDERER_BROWSER_CONCURRENCY"))
	if browserConcurrency <= 0 {
		// A Chromium renderer is substantially more expensive than the Kafka
		// message handler surrounding it. Keep the default deliberately small.
		browserConcurrency = 2
	}
	if browserConcurrency > maxRoutines {
		browserConcurrency = maxRoutines
	}

	return &Cfg{
		MaxRoutines:        maxRoutines,
		BrowserConcurrency: browserConcurrency,
		KafkaBroker:        os.Getenv("KAFKA_BROKER"),
		KafkaTopic:         os.Getenv("KAFKA_TOPIC"),
		KafkaProducerTopic: os.Getenv("KAFKA_PRODUCER_TOPIC"),
		KafkaGroup:         os.Getenv("KAFKA_GROUP"),
		FrontierURL:        envOrDefault("FRONTIER_URL", "http://frontier:8080"),
	}, nil

}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
