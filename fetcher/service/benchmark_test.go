package service

import "testing"

func BenchmarkIsDynamic(b *testing.B) {
	svc := &Service{}
	cases := map[string]string{
		"static": "<html><body>plain content</body></html>",
		"root":   "<html><div id=\"root\"></div></html>",
		"next":   "<script>window.__INITIAL_STATE__={}</script>",
		"large":  "<html><body>" + string(make([]byte, 32*1024)) + "</body></html>",
	}
	for name, html := range cases {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = svc.isDynamic(html)
			}
		})
	}
}
