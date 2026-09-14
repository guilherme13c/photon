package http_client

import "context"

// Response is deliberately limited to a single network hop. Redirect targets
// must be returned to the Frontier rather than followed by a Fetcher worker,
// otherwise the target host would bypass its reserved politeness slot.
type Response struct {
	Body        []byte
	RedirectURL string
}

type Client interface {
	Fetch(ctx context.Context, url string) (Response, error)
}
