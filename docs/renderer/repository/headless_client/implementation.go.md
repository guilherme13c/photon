# Documentation for `implementation.go`

**Path:** `renderer/repository/headless_client/implementation.go`

## Overview

This file is part of the `renderer` component.

## Structs
- `headlessClientImpl`

## Functions
- `NewClient`
- `Fetch`

## Source Code

```go
package headless_client

import (
	"context"
	"fmt"
	"time"

	"github.com/chromedp/chromedp"
)

type headlessClientImpl struct{}

func NewClient() Client {
	return &headlessClientImpl{}
}

func (c *headlessClientImpl) Fetch(ctx context.Context, url string) ([]byte, error) {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Create headless browser context
	allocCtx, cancelAlloc := chromedp.NewExecAllocator(ctx, chromedp.DefaultExecAllocatorOptions[:]...)
	defer cancelAlloc()

	taskCtx, cancelTask := chromedp.NewContext(allocCtx)
	defer cancelTask()

	var htmlContent string

	err := chromedp.Run(taskCtx,
		chromedp.Navigate(url),
		// Wait a bit for JS to execute (in production we would wait for network idle)
		chromedp.Sleep(2*time.Second),
		chromedp.WaitReady("body", chromedp.ByQuery),
		chromedp.OuterHTML("html", &htmlContent, chromedp.ByQuery),
	)

	if err != nil {
		return nil, fmt.Errorf("headless fetch failed for %s: %w", url, err)
	}

	return []byte(htmlContent), nil
}

```
