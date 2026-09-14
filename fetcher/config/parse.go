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

	promPort := os.Getenv("PROMETHEUS_PORT")
	if promPort == "" {
		promPort = "2112"
	}

	return &Cfg{
		MaxRoutines:           maxRoutines,
		KafkaBroker:           os.Getenv("KAFKA_BROKER"),
		KafkaTopic:            os.Getenv("KAFKA_TOPIC"),
		KafkaProducerTopic:    os.Getenv("KAFKA_PRODUCER_TOPIC"),
		KafkaDynamicUrlsTopic: os.Getenv("KAFKA_DYNAMIC_URLS_TOPIC"),
		KafkaGroup:            os.Getenv("KAFKA_GROUP"),
		KafkaDlqTopic:         os.Getenv("KAFKA_DLQ_TOPIC"),
		FrontierURL:           envOrDefault("FRONTIER_URL", "http://frontier:8080"),
		PrometheusPort:        promPort,
	}, nil

}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
