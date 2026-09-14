package search

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	httpStatusOK         = 200
	httpStatusBadRequest = 400
)

var latencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type Metrics struct {
	mu         sync.Mutex
	requests   uint64
	results    uint64
	status     map[int]uint64
	latency    []uint64
	latencySum float64
}

func NewMetrics() *Metrics {
	return &Metrics{status: make(map[int]uint64), latency: make([]uint64, len(latencyBuckets)+1)}
}

func (m *Metrics) Observe(status, results int, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests++
	m.results += uint64(results)
	m.status[status]++
	seconds := duration.Seconds()
	m.latencySum += seconds
	index := sort.SearchFloat64s(latencyBuckets, seconds)
	m.latency[index]++
}

func (m *Metrics) Prometheus() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var output strings.Builder
	fmt.Fprintf(&output, "# TYPE photon_search_requests_total counter\nphoton_search_requests_total %d\n", m.requests)
	statuses := make([]int, 0, len(m.status))
	for status := range m.status {
		statuses = append(statuses, status)
	}
	sort.Ints(statuses)
	for _, status := range statuses {
		fmt.Fprintf(&output, "photon_search_requests_status_total{status=\"%s\"} %d\n", strconv.Itoa(status), m.status[status])
	}
	fmt.Fprintf(&output, "# TYPE photon_search_results_total counter\nphoton_search_results_total %d\n", m.results)
	fmt.Fprintf(&output, "# TYPE photon_search_request_duration_seconds histogram\n")
	var cumulative uint64
	for index, bucket := range latencyBuckets {
		cumulative += m.latency[index]
		fmt.Fprintf(&output, "photon_search_request_duration_seconds_bucket{le=\"%g\"} %d\n", bucket, cumulative)
	}
	cumulative += m.latency[len(latencyBuckets)]
	fmt.Fprintf(&output, "photon_search_request_duration_seconds_bucket{le=\"+Inf\"} %d\nphoton_search_request_duration_seconds_sum %g\nphoton_search_request_duration_seconds_count %d\n", cumulative, m.latencySum, m.requests)
	return output.String()
}
