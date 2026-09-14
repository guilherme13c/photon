package http_client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type clientImpl struct {
	client      *http.Client
	frontierURL string
}

func NewClient(frontierURL ...string) Client {
	permitURL := ""
	if len(frontierURL) > 0 {
		permitURL = strings.TrimRight(frontierURL[0], "/")
	}
	return &clientImpl{
		frontierURL: permitURL,
		client: &http.Client{
			Timeout: 10 * time.Second,
			// A redirect is another origin request. Never make it implicitly.
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (c *clientImpl) Fetch(ctx context.Context, url string) (Response, error) {
	if err := c.acquireStartPermit(ctx, url); err != nil {
		return Response{}, fmt.Errorf("acquire origin start permit: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Response{}, fmt.Errorf("failed to create request: %w", err)
	}

	// Add user-agent to avoid simple blocks
	req.Header.Set("User-Agent", "Photon-Fetcher/1.0")

	resp, err := c.client.Do(req)
	if err != nil {
		return Response{}, fmt.Errorf("failed to fetch url %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		location, err := resp.Location()
		if err != nil {
			return Response{}, fmt.Errorf("redirect response without a valid Location for %s: %w", url, err)
		}
		return Response{RedirectURL: location.String()}, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Response{}, fmt.Errorf("unexpected status code %d for url %s", resp.StatusCode, url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Response{}, fmt.Errorf("failed to read response body: %w", err)
	}

	return Response{Body: body}, nil
}

// acquireStartPermit is deliberately adjacent to Client.Do: dispatcher timing
// cannot guarantee when an asynchronous Kafka worker reaches the origin.
func (c *clientImpl) acquireStartPermit(ctx context.Context, targetURL string) error {
	if c.frontierURL == "" { // Enables isolated unit tests without Frontier.
		return nil
	}
	body, err := json.Marshal(struct {
		URL string `json:"url"`
	}{URL: targetURL})
	if err != nil {
		return err
	}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.frontierURL+"/permits/start", strings.NewReader(string(body)))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := c.client.Do(req)
		if err != nil {
			return err
		}
		var permit struct {
			RetryAtMS int64 `json:"retry_at_ms"`
		}
		_ = json.NewDecoder(response.Body).Decode(&permit)
		response.Body.Close()
		if response.StatusCode == http.StatusOK {
			return nil
		}
		if response.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("Frontier returned %s", response.Status)
		}
		wait := time.Until(time.UnixMilli(permit.RetryAtMS))
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
