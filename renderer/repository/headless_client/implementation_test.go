package headless_client

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
)

// This test is opt-in because Chromium is installed in the renderer image, not
// necessarily on a developer's host. The benchmark harness runs it against the
// controlled fixture origin before it starts a broad dynamic workload.
func TestHeadlessClientFetchDynamicFixture(t *testing.T) {
	fixtureURL := os.Getenv("PHOTON_HEADLESS_FIXTURE_URL")
	if fixtureURL == "" {
		t.Skip("set PHOTON_HEADLESS_FIXTURE_URL to run Chromium fixture validation")
	}

	ctx, cancel := context.WithTimeout(context.Background(), renderTimeout+5*time.Second)
	defer cancel()
	content, err := NewClient(1).Fetch(ctx, fixtureURL)
	if err != nil {
		t.Fatalf("Fetch(%q): %v", fixtureURL, err)
	}
	if !strings.Contains(string(content), "window.__INITIAL_STATE__") {
		t.Fatalf("dynamic fixture content missing from rendered document: %q", content)
	}
}

func TestURLsMatchForNavigation(t *testing.T) {
	tests := []struct {
		name    string
		request string
		allowed string
		want    bool
	}{
		{
			name:    "browser canonicalization",
			request: "HTTP://EXAMPLE.COM:80#section",
			allowed: "http://example.com/",
			want:    true,
		},
		{
			name:    "query remains part of reservation",
			request: "https://example.com/page?cursor=next",
			allowed: "https://example.com/page",
			want:    false,
		},
		{
			name:    "different origin is denied",
			request: "https://other.example/page",
			allowed: "https://example.com/page",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := urlsMatchForNavigation(tt.request, tt.allowed); got != tt.want {
				t.Fatalf("urlsMatchForNavigation(%q, %q) = %v, want %v", tt.request, tt.allowed, got, tt.want)
			}
		})
	}
}

func TestIsReservedDocumentRequest(t *testing.T) {
	allowed := "https://example.com/page"
	tests := []struct {
		name   string
		paused *fetch.EventRequestPaused
		want   bool
	}{
		{
			name: "initial document is allowed",
			paused: &fetch.EventRequestPaused{
				ResourceType: network.ResourceTypeDocument,
				Request:      &network.Request{URL: allowed},
			},
			want: true,
		},
		{
			name: "script is blocked",
			paused: &fetch.EventRequestPaused{
				ResourceType: network.ResourceTypeScript,
				Request:      &network.Request{URL: allowed},
			},
		},
		{
			name: "redirect is blocked",
			paused: &fetch.EventRequestPaused{
				ResourceType:        network.ResourceTypeDocument,
				Request:             &network.Request{URL: allowed},
				RedirectedRequestID: "previous-request",
			},
		},
		{
			name: "different document is blocked",
			paused: &fetch.EventRequestPaused{
				ResourceType: network.ResourceTypeDocument,
				Request:      &network.Request{URL: "https://other.example/page"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isReservedDocumentRequest(tt.paused, allowed); got != tt.want {
				t.Fatalf("isReservedDocumentRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}
