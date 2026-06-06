package producer

import "context"

type Producer interface {
	Produce(ctx context.Context, topic string, key []byte, value []byte) error
	Close() error
}

