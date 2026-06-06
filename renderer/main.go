package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/guilherme13c/renderer/config"
	"github.com/guilherme13c/renderer/repository/headless_client"
	"github.com/guilherme13c/renderer/repository/kafka/consumer"
	"github.com/guilherme13c/renderer/repository/kafka/producer"
	"github.com/guilherme13c/renderer/repository/storage"
	"github.com/guilherme13c/renderer/service"
)

func main() {
	// parses .env generating cfg instance
	cfg, err := config.Parse()
	if err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}

	// instantiates repositories
	kafkaConsumer := consumer.NewConsumer(cfg.KafkaBroker, cfg.KafkaTopic, cfg.KafkaGroup)
	defer kafkaConsumer.Close()

	kafkaProducer := producer.NewProducer(cfg.KafkaBroker)
	defer kafkaProducer.Close()

	headlessClient := headless_client.NewClient()
	storageRepo := storage.NewStorage()
	defer storageRepo.Close()

	// instantiates service using repo instances
	svc := service.NewService(headlessClient, storageRepo, kafkaProducer, cfg.KafkaProducerTopic)

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
