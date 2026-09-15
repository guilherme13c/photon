package http_client

import (
	"net/http"
	"testing"
)

func TestNewClientUsesPooledTransport(t *testing.T) {
	c := NewClient().(*clientImpl)
	transport, ok := c.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected explicit http transport, got %T", c.client.Transport)
	}
	if transport.MaxIdleConnsPerHost != 16 || transport.MaxIdleConns != 100 {
		t.Fatalf("unexpected pool limits: idle=%d per_host=%d", transport.MaxIdleConns, transport.MaxIdleConnsPerHost)
	}
}
