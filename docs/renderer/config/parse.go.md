# Documentation for `parse.go`

**Path:** `renderer/config/parse.go`

## Overview

This file is part of the `renderer` component.

## Functions
- `Parse`

## Source Code

```go
package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

func Parse() (*Cfg, error) {
	_ = godotenv.Load() // Ignore error as it might be set in environment directly

	maxRoutines, _ := strconv.Atoi(os.Getenv("MAX_ROUTINES"))
	if maxRoutines == 0 {
		maxRoutines = 10
	}

	return &Cfg{
		MaxRoutines:        maxRoutines,
		KafkaBroker:        os.Getenv("KAFKA_BROKER"),
		KafkaTopic:         os.Getenv("KAFKA_TOPIC"),
		KafkaProducerTopic: os.Getenv("KAFKA_PRODUCER_TOPIC"),
		KafkaGroup:         os.Getenv("KAFKA_GROUP"),
	}, nil

}

```
