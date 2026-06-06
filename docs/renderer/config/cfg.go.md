# Documentation for `cfg.go`

**Path:** `renderer/config/cfg.go`

## Overview

This file is part of the `renderer` component.

## Structs
- `Cfg`

## Source Code

```go
package config

type Cfg struct {
	MaxRoutines        int
	KafkaBroker        string
	KafkaTopic         string
	KafkaProducerTopic string
	KafkaGroup         string
}

```
