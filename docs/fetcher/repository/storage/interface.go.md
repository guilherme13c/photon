# Documentation for `interface.go`

**Path:** `fetcher/repository/storage/interface.go`

## Overview

This file is part of the `fetcher` component.

## Structs
- `Document`

## Interfaces
- `Storage`

## Source Code

```go
package storage

import "context"

type Document struct {
	URL     string
	Content string
}

type Storage interface {
	Save(ctx context.Context, doc Document) error
	Close() error
}


```
