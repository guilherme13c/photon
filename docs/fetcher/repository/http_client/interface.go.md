# Documentation for `interface.go`

**Path:** `fetcher/repository/http_client/interface.go`

## Overview

This file is part of the `fetcher` component.

## Interfaces
- `Client`

## Source Code

```go
package http_client

import "context"

type Client interface {
	Fetch(ctx context.Context, url string) ([]byte, error)
}


```
