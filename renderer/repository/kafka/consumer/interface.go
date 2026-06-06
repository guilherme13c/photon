package consumer

import "context"

type Consumer interface {
	Consume(ctx context.Context) (Message, error)
	Close() error
}

type Message struct {
	Key   []byte
	Value []byte
}

