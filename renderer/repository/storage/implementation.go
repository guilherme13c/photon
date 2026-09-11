package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"log"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type storageImpl struct {
	client *minio.Client
	bucket string
}

func NewStorage() Storage {
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:9000"
	} else if len(endpoint) > 7 && endpoint[:7] == "http://" {
		endpoint = endpoint[7:]
	}
	accessKeyID := os.Getenv("MINIO_ACCESS_KEY")
	if accessKeyID == "" {
		accessKeyID = "minioadmin"
	}
	secretAccessKey := os.Getenv("MINIO_SECRET_KEY")
	if secretAccessKey == "" {
		secretAccessKey = "minioadmin"
	}

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalf("failed to initialize minio client: %v", err)
	}

	return &storageImpl{
		client: minioClient,
		bucket: "html-payloads",
	}
}

func (s *storageImpl) Save(ctx context.Context, doc Document) (string, error) {
	objectName := base64.URLEncoding.EncodeToString([]byte(doc.URL)) + ".html"
	_, err := s.client.PutObject(ctx, s.bucket, objectName, bytes.NewReader([]byte(doc.Content)), int64(len(doc.Content)), minio.PutObjectOptions{
		ContentType: "text/html",
	})
	if err != nil {
		log.Printf("Storage error: Failed to save document for URL %s: %v", doc.URL, err)
		return "", err
	}

	log.Printf("Storage: Saved document for URL %s to MinIO bucket %s with key %s", doc.URL, s.bucket, objectName)
	return objectName, nil
}

func (s *storageImpl) Close() error {
	// Stub: Close DB connection
	return nil
}
