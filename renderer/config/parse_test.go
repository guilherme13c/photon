package config

import (
	"os"
	"testing"
)

func TestParse(t *testing.T) {
	// Set environment variables for testing
	os.Setenv("MAX_ROUTINES", "20")
	os.Setenv("KAFKA_BROKER", "localhost:9092")
	os.Setenv("KAFKA_TOPIC", "test-topic")
	os.Setenv("KAFKA_PRODUCER_TOPIC", "test-producer-topic")
	os.Setenv("KAFKA_GROUP", "test-group")
	defer func() {
		os.Unsetenv("MAX_ROUTINES")
		os.Unsetenv("KAFKA_BROKER")
		os.Unsetenv("KAFKA_TOPIC")
		os.Unsetenv("KAFKA_PRODUCER_TOPIC")
		os.Unsetenv("KAFKA_GROUP")
	}()

	cfg, err := Parse()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if cfg.MaxRoutines != 20 {
		t.Errorf("expected MaxRoutines to be 20, got %d", cfg.MaxRoutines)
	}
	if cfg.KafkaBroker != "localhost:9092" {
		t.Errorf("expected KafkaBroker to be 'localhost:9092', got %s", cfg.KafkaBroker)
	}
	if cfg.KafkaTopic != "test-topic" {
		t.Errorf("expected KafkaTopic to be 'test-topic', got %s", cfg.KafkaTopic)
	}
	if cfg.KafkaProducerTopic != "test-producer-topic" {
		t.Errorf("expected KafkaProducerTopic to be 'test-producer-topic', got %s", cfg.KafkaProducerTopic)
	}
	if cfg.KafkaGroup != "test-group" {
		t.Errorf("expected KafkaGroup to be 'test-group', got %s", cfg.KafkaGroup)
	}
}

func TestParse_DefaultMaxRoutines(t *testing.T) {
	os.Unsetenv("MAX_ROUTINES")
	
	cfg, err := Parse()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if cfg.MaxRoutines != 10 {
		t.Errorf("expected default MaxRoutines to be 10, got %d", cfg.MaxRoutines)
	}
}
