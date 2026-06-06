package http_client

import "context"

type Client interface {
	Fetch(ctx context.Context, url string) ([]byte, error)
}

