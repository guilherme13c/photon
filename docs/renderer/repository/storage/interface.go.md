# Documentation for `interface.go`

**Path:** `renderer/repository/storage/interface.go`

## Overview

This file is part of the `renderer` component.

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
