# Documentation for `interface.go`

**Path:** `fetcher/repository/kafka/consumer/interface.go`

## Overview

This file is part of the `fetcher` component.

## Structs
- `Message`

## Interfaces
- `Consumer`

## Source Code

```go
package consumer

import "context"

type Consumer interface {
	Consume(ctx context.Context) (Message, error)
	Close() error
}

type Message struct {
	Key   []byte
	Value []byte
}


```
