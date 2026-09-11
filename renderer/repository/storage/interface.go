package storage

import "context"

type Document struct {
	URL     string
	Content string
}

type Storage interface {
	Save(ctx context.Context, doc Document) (string, error)
	Close() error
}
