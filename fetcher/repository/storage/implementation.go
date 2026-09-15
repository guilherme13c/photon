package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"html"
	"log"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type storageImpl struct {
	client *minio.Client
	bucket string
}

// objectNameForURL produces a MinIO-safe, deterministic key. Extracted HTML
// commonly contains escaped query separators (&amp;); decode those before
// encoding so retries and canonical URLs map to the same object.
func objectNameForURL(rawURL string) string {
	canonical := html.UnescapeString(rawURL)
	return base64.RawURLEncoding.EncodeToString([]byte(canonical)) + ".html"
}

func NewStorage() Storage {
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:9000"
	} else {
		// remove http:// or https://
		if len(endpoint) > 7 && endpoint[:7] == "http://" {
			endpoint = endpoint[7:]
		}
	}
	accessKeyID := os.Getenv("MINIO_ACCESS_KEY")
	if accessKeyID == "" {
		accessKeyID = "minioadmin"
	}
	secretAccessKey := os.Getenv("MINIO_SECRET_KEY")
	if secretAccessKey == "" {
		secretAccessKey = "minioadmin"
	}

	useSSL := false

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
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
	reader := bytes.NewReader([]byte(doc.Content))
	
	objectName := objectNameForURL(doc.URL)
	
	_, err := s.client.PutObject(ctx, s.bucket, objectName, reader, int64(len(doc.Content)), minio.PutObjectOptions{
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
	// MinIO client doesn't need to be explicitly closed
	return nil
}
