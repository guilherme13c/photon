# Documentation for `implementation.go`

**Path:** `fetcher/repository/http_client/implementation.go`

## Overview

This file is part of the `fetcher` component.

## Structs
- `clientImpl`

## Functions
- `NewClient`
- `Fetch`

## Source Code

```go
package http_client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type clientImpl struct {
	client *http.Client
}

func NewClient() Client {
	return &clientImpl{
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *clientImpl) Fetch(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add user-agent to avoid simple blocks
	req.Header.Set("User-Agent", "Photon-Fetcher/1.0")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch url %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("unexpected status code %d for url %s", resp.StatusCode, url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return body, nil
}


```
