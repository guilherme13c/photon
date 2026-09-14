package search

import "testing"

func TestValidateRequestAppliesDefaults(t *testing.T) {
	req, err := ValidateRequest("  photon  ", 0, "")
	if err != nil {
		t.Fatalf("ValidateRequest() error = %v", err)
	}
	if req.Query != "photon" || req.Limit != DefaultPageSize || req.Cursor != "" {
		t.Fatalf("ValidateRequest() = %#v", req)
	}
}

func TestValidateRequestRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		query string
		limit int
	}{
		{name: "empty query", query: "   ", limit: 10},
		{name: "negative limit", query: "photon", limit: -1},
		{name: "limit too large", query: "photon", limit: MaxPageSize + 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ValidateRequest(tc.query, tc.limit, ""); err == nil {
				t.Fatal("ValidateRequest() unexpectedly succeeded")
			}
		})
	}
}

func TestCursorRoundTripAndQueryBinding(t *testing.T) {
	cursor, err := EncodeCursor("photon", 20)
	if err != nil {
		t.Fatalf("EncodeCursor() error = %v", err)
	}
	decoded, err := DecodeCursor("photon", cursor)
	if err != nil {
		t.Fatalf("DecodeCursor() error = %v", err)
	}
	if decoded.Offset != 20 {
		t.Fatalf("DecodeCursor() offset = %d, want 20", decoded.Offset)
	}
	if _, err := DecodeCursor("different query", cursor); err == nil {
		t.Fatal("DecodeCursor() accepted a cursor for another query")
	}
}

func TestDecodeCursorRejectsMalformedAndNegativeOffsets(t *testing.T) {
	if _, err := DecodeCursor("photon", "not-a-cursor"); err == nil {
		t.Fatal("DecodeCursor() accepted malformed cursor")
	}
	cursor, err := EncodeCursor("photon", -1)
	if err == nil || cursor != "" {
		t.Fatalf("EncodeCursor() = (%q, %v), want empty cursor and error", cursor, err)
	}
}
