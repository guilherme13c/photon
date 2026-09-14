package search

import (
	"strings"
	"testing"
	"time"
)

func TestMetricsExposeCountersAndLatencyHistogram(t *testing.T) {
	metrics := NewMetrics()
	metrics.Observe(httpStatusOK, 3, 25*time.Millisecond)
	metrics.Observe(httpStatusBadRequest, 0, 2*time.Millisecond)
	output := metrics.Prometheus()
	for _, expected := range []string{
		`photon_search_requests_total 2`,
		`photon_search_requests_status_total{status="200"} 1`,
		`photon_search_requests_status_total{status="400"} 1`,
		`photon_search_results_total 3`,
		`photon_search_request_duration_seconds_count 2`,
		`photon_search_request_duration_seconds_bucket{le="0.05"} 2`,
	} {
		if !strings.Contains(output, expected) {
			t.Errorf("metrics missing %q in:\n%s", expected, output)
		}
	}
}
