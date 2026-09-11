package service

import "testing"

// The classifier receives arbitrary bytes from remote origins. Keep a small
// regression corpus and require that classification remains total.
func FuzzDynamicClassifier(f *testing.F) {
	for _, seed := range []string{"", "<div id=\"root\"></div>", "__NEXT_DATA__", "\xff\x00<script>"} {
		f.Add(seed)
	}
	svc := &Service{}
	f.Fuzz(func(t *testing.T, html string) {
		_ = svc.isDynamic(html)
	})
}
