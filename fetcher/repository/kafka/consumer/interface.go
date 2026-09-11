package consumer

import "context"

type Consumer interface {
	Fetch(ctx context.Context) (Message, error)
	Commit(ctx context.Context, msg Message) error
	Close() error
}

type Message struct {
	Key       []byte
	Value     []byte
	Partition int
	Offset    int64
}
