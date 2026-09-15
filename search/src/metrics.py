class Metrics:
    def __init__(self):
        self.requests = 0
        self.results = 0
        self.status = {}
        self.latencies = []
        self.retrieval_modes = {}

    def observe(self, status, results, duration_seconds=0, retrieval="dense"):
        self.requests += 1
        self.results += results
        self.status[status] = self.status.get(status, 0) + 1
        self.latencies.append(duration_seconds)
        self.retrieval_modes[retrieval] = self.retrieval_modes.get(retrieval, 0) + 1

    def render(self):
        buckets = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)
        lines = [f"photon_search_requests_total {self.requests}", f"photon_search_results_total {self.results}"]
        lines.extend(f'photon_search_requests_status_total{{status="{status}"}} {count}' for status, count in sorted(self.status.items()))
        lines.extend(f'photon_search_retrieval_requests_total{{mode="{mode}"}} {count}' for mode, count in sorted(self.retrieval_modes.items()))
        lines.append("# TYPE photon_search_request_duration_seconds histogram")
        for bucket in buckets:
            lines.append(f'photon_search_request_duration_seconds_bucket{{le="{bucket:g}"}} {sum(value <= bucket for value in self.latencies)}')
        lines.extend([
            f"photon_search_request_duration_seconds_bucket{{le=\"+Inf\"}} {len(self.latencies)}",
            f"photon_search_request_duration_seconds_sum {sum(self.latencies)}",
            f"photon_search_request_duration_seconds_count {len(self.latencies)}",
        ])
        return "\n".join(lines) + "\n"
