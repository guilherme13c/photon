package storage

import (
	"context"
	"log"
)

type storageImpl struct {
	// Add DB connection here, e.g., *sql.DB
}

func NewStorage() Storage {
	return &storageImpl{}
}

func (s *storageImpl) Save(ctx context.Context, doc Document) error {
	// Stub: In a real scenario, execute an INSERT query
	log.Printf("Storage stub: Saved document for URL %s (size: %d bytes)", doc.URL, len(doc.Content))
	return nil
}

func (s *storageImpl) Close() error {
	// Stub: Close DB connection
	return nil
}

