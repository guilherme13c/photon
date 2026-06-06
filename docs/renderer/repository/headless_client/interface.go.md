# Documentation for `interface.go`

**Path:** `renderer/repository/headless_client/interface.go`

## Overview

This file is part of the `renderer` component.

## Interfaces
- `Client`

## Source Code

```go
package headless_client

import "context"

type Client interface {
	Fetch(ctx context.Context, url string) ([]byte, error)
}

```
