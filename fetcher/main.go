package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/guilherme13c/fetcher/config"
	"github.com/guilherme13c/fetcher/repository/http_client"
	"github.com/guilherme13c/fetcher/repository/kafka/consumer"
	"github.com/guilherme13c/fetcher/repository/kafka/producer"
	"github.com/guilherme13c/fetcher/repository/storage"
	"github.com/guilherme13c/fetcher/service"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	cfg := config.Load()

	// Initialize dependencies
	httpRepo := http_client.NewClient()
	storageRepo := storage.NewMinIOStorage(cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioSecretKey)

	kafkaConsumer, err := consumer.NewKafkaConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.KafkaIngestTopic)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer kafkaConsumer.Close()

	kafkaProducer, err := producer.NewKafkaProducer(cfg.KafkaBrokers, cfg.KafkaUrlsTopic)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()
	
	dlqProducer, err := producer.NewKafkaProducer(cfg.KafkaBrokers, cfg.KafkaDlqTopic)
	if err != nil {
		log.Fatalf("Failed to create Kafka DLQ producer: %v", err)
	}
	defer dlqProducer.Close()

	svc := service.NewService(httpRepo, kafkaProducer, storageRepo, cfg.RendererURL, dlqProducer, cfg.KafkaDlqTopic)

	// Start Prometheus metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Starting Prometheus metrics server on :2112")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Fatalf("Failed to start metrics server: %v", err)
		}
	}()

	log.Println("Fetcher service started")

	// starts loop to consume messages and process them
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sem := make(chan struct{}, cfg.MaxRoutines)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := kafkaConsumer.Consume(ctx)
				if err != nil {
					log.Printf("consumer error: %v", err)
					continue
				}

				sem <- struct{}{} // Block if limit reached
				go func(m consumer.Message) {
					defer func() { <-sem }() // Release semaphore
					svc.Process(ctx, m)
				}(msg)
			}
		}
	}()

	// graceful shutdown
	<-sigChan
	log.Println("Shutting down gracefully...")
	cancel()
}
