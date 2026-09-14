from src.metrics import Metrics


def test_metrics_render_latency_histogram_and_counters():
    metrics = Metrics()
    metrics.observe(200, 2, 0.025)
    output = metrics.render()
    assert "photon_search_requests_total 1" in output
    assert 'photon_search_requests_status_total{status="200"} 1' in output
    assert "photon_search_results_total 2" in output
    assert 'photon_search_request_duration_seconds_bucket{le="0.05"} 1' in output
    assert "photon_search_request_duration_seconds_count 1" in output
