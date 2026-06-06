package consumer

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type consumerImpl struct {
	reader *kafka.Reader
}

func NewConsumer(broker, topic, groupID string) Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	return &consumerImpl{
		reader: reader,
	}
}

func (c *consumerImpl) Consume(ctx context.Context) (Message, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return Message{}, err
	}
	return Message{
		Key:   msg.Key,
		Value: msg.Value,
	}, nil

}

func (c *consumerImpl) Close() error {
	return c.reader.Close()
}
