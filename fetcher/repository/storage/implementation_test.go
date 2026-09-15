package storage

import (
	"encoding/base64"
	"testing"
)

func TestObjectNameForURLUnescapesHTMLQuerySeparators(t *testing.T) {
	got := objectNameForURL("https://example.com/?a=1&amp;b=2")
	want := base64.RawURLEncoding.EncodeToString([]byte("https://example.com/?a=1&b=2")) + ".html"
	if got != want { t.Fatalf("object key = %q, want %q", got, want) }
}
