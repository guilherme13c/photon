package service

import (
	"encoding/json"
	"testing"
)

// Renderer publishes this envelope to a different language runtime. Fuzzing
// validates that arbitrary string fields always remain valid JSON and preserve
// the two required keys.
func FuzzFetchedPageEnvelope(f *testing.F) {
	f.Add("https://example.test", "pages/example.html")
	f.Add("\x00", "\xff")
	f.Fuzz(func(t *testing.T, url, key string) {
		payload, err := json.Marshal(struct {
			URL   string `json:"url"`
			S3Key string `json:"s3_key"`
		}{url, key})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var decoded map[string]string
		if err := json.Unmarshal(payload, &decoded); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if _, ok := decoded["url"]; !ok {
			t.Fatal("envelope lost url")
		}
		if _, ok := decoded["s3_key"]; !ok {
			t.Fatal("envelope lost s3_key")
		}
	})
}
