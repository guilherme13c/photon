package producer

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type producerImpl struct {
	writer *kafka.Writer
}

func NewProducer(broker string) Producer {
	writer := newWriter(broker)

	return &producerImpl{
		writer: writer,
	}
}

func newWriter(broker string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		AllowAutoTopicCreation: true,
		// A Fetcher input offset is committed only after Produce returns. The
		// kafka-go default is RequireNone, which merely queues the write locally;
		// a process loss at that point would acknowledge the input while losing
		// its fetched-pages handoff. Wait for the broker's full ISR instead.
		// Photon currently uses one replica in Compose, so RequireAll means the
		// sole durable broker replica has acknowledged the record.
		RequiredAcks: kafka.RequireAll,
		// kafka-go otherwise waits up to one second to fill a batch. The
		// Fetcher waits for durable publication before committing its input
		// offset, so that default turns every small controlled-origin page
		// into roughly one second of artificial service time. Keep batching
		// under load, but flush sparse batches promptly.
		BatchTimeout: 10 * time.Millisecond,
	}
}

func (p *producerImpl) Produce(ctx context.Context, topic string, key []byte, value []byte) error {
	msg := kafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	return p.writer.WriteMessages(ctx, msg)
}

func (p *producerImpl) Close() error {
	return p.writer.Close()
}
