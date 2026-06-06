# Documentation for `interface.go`

**Path:** `renderer/repository/kafka/producer/interface.go`

## Overview

This file is part of the `renderer` component.

## Interfaces
- `Producer`

## Source Code

```go
package producer

import "context"

type Producer interface {
	Produce(ctx context.Context, topic string, key []byte, value []byte) error
	Close() error
}


```
