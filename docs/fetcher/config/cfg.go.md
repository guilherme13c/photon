# Documentation for `cfg.go`

**Path:** `fetcher/config/cfg.go`

## Overview

This file is part of the `fetcher` component.

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
	KafkaDynamicUrlsTopic string
	KafkaGroup         string
}

```
