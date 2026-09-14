package service

import (
	"encoding/json"
	"testing"
)

func BenchmarkStorageEnvelopeMarshal(b *testing.B) {
	payload := struct {
		URL   string `json:"url"`
		S3Key string `json:"s3_key"`
	}{URL: "https://example.test/article", S3Key: "pages/0123456789.html"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(payload); err != nil {
			b.Fatal(err)
		}
	}
}
