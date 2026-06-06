package producer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type producerImpl struct {
	writer *kafka.Writer
}

func NewProducer(broker string) Producer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		AllowAutoTopicCreation: true,
	}

	return &producerImpl{
		writer: writer,
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
