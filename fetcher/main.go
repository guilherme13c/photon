package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	httpRepo := http_client.NewClient(cfg.FrontierURL)
	storageRepo := storage.NewStorage()

	kafkaConsumer := consumer.NewConsumer(cfg.KafkaBroker, cfg.KafkaTopic, cfg.KafkaGroup)
	defer kafkaConsumer.Close()

	kafkaProducer := producer.NewProducer(cfg.KafkaBroker)
	defer kafkaProducer.Close()

	svc := service.NewService(httpRepo, storageRepo, kafkaProducer, cfg.KafkaProducerTopic, cfg.KafkaDynamicUrlsTopic, cfg.KafkaDlqTopic)

	// Start Prometheus metrics server
	go func() {
		if os.Getenv("PHOTON_ENABLE_PPROF") != "1" {
			return
		}
		log.Printf("Starting fetcher pprof server on :%s", os.Getenv("PHOTON_PPROF_PORT"))
		port := os.Getenv("PHOTON_PPROF_PORT")
		if port == "" {
			port = "6060"
		}
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Printf("pprof server failed: %v", err)
		}
	}()

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

	// Each Kafka partition is processed in order. This matters because Kafka
	// commits are cumulative per partition: committing a later offset would also
	// acknowledge every earlier offset. Different partitions can still fetch in
	// parallel, bounded by MAX_ROUTINES.
	sem := make(chan struct{}, cfg.MaxRoutines)
	completed := make(chan consumer.Message, cfg.MaxRoutines)
	go commitLoop(ctx, kafkaConsumer, completed)
	go func() {
		workers := make(map[int]chan consumer.Message)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := kafkaConsumer.Fetch(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("consumer error: %v", err)
					continue
				}

				worker, ok := workers[msg.Partition]
				if !ok {
					// Do not let one hot Kafka partition stop the reader from
					// receiving work for every other assigned partition. Processing
					// remains serial *within* a partition, so commit ordering and
					// host-key ordering are unchanged.
					worker = make(chan consumer.Message, cfg.MaxRoutines*4)
					workers[msg.Partition] = worker
					go processPartition(ctx, svc, worker, sem, completed)
				}

				select {
				case worker <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// graceful shutdown
	<-sigChan
	log.Println("Shutting down gracefully...")
	cancel()
}

func processPartition(ctx context.Context, svc *service.Service, messages <-chan consumer.Message, sem chan struct{}, completed chan<- consumer.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-messages:
			for {
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				err := svc.Process(ctx, msg)
				<-sem
				if err == nil {
					select {
					case completed <- msg:
					case <-ctx.Done():
					}
					break
				}
				if dlqErr := svc.DeadLetter(ctx, msg, err); dlqErr == nil {
					select { case completed <- msg: case <-ctx.Done(): }
					break
				}
				log.Printf("processing partition %d offset %d failed: %v; retrying", msg.Partition, msg.Offset, err)
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func commitLoop(ctx context.Context, kafkaConsumer consumer.Consumer, completed <-chan consumer.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-completed:
			for {
				if err := kafkaConsumer.Commit(ctx, msg); err == nil {
					break
				} else {
					log.Printf("commit partition %d offset %d failed: %v; retrying", msg.Partition, msg.Offset, err)
				}
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
					return
				}
			}
		}
	}
}
