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
	cfg, _ := config.Parse()

	// Initialize dependencies
	httpRepo := http_client.NewClient()
	storageRepo := storage.NewStorage()

	kafkaConsumer := consumer.NewConsumer(cfg.KafkaBroker, cfg.KafkaTopic, cfg.KafkaGroup)
	defer kafkaConsumer.Close()

	kafkaProducer := producer.NewProducer(cfg.KafkaBroker)
	defer kafkaProducer.Close()

	svc := service.NewService(httpRepo, storageRepo, kafkaProducer, cfg.KafkaProducerTopic, cfg.KafkaDynamicUrlsTopic, cfg.KafkaDlqTopic)

	// Start Prometheus metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("Starting Prometheus metrics server on :%s\n", cfg.PrometheusPort)
		if err := http.ListenAndServe(":"+cfg.PrometheusPort, nil); err != nil {
			log.Fatalf("Metrics server failed: %v", err)
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
