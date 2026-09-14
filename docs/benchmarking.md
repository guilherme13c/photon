# Benchmarking and profiling

Photon benchmarks are report-only. They run only on a disposable, pinned staging
runner and use the controlled fixture origin for all high-rate traffic. A public
URL snapshot remains a low-rate, robots-compliant representative report; it is
not a capacity target.

## Commands

`make benchmark-smoke` validates the benchmark result calculations without
Docker or network access and runs in the PR fast gate. `make benchmark-functions`
runs native Frontier/Extractor Zig benchmarks, Fetcher/Renderer Go benchmarks,
and the dependency-free Embedder parsing benchmark. `make benchmark-services`
and `make benchmark-e2e` require `PHOTON_FRONTIER_URL` and
`PHOTON_BENCHMARK_ORIGIN`.

`benchmark-services` is a true service-isolation tier. At every scale point it
recreates Fetcher, Renderer, Extractor, and Embedder with dedicated input and
output topics (where the service produces Kafka) plus a unique consumer group.
The runner injects the versioned contract directly and checks the target's
durable result: `{url,s3_key}` on `fetched-pages` for Fetcher and Renderer, a
cleaned document for Extractor, and a matching Qdrant vector for Embedder.
Extractor receives a fixture object in the isolated MinIO volume. Frontier
remains an HTTP admission case verified by its durable admission response; the
E2E tier remains responsible for cross-service throughput and politeness.
It requires the isolated Compose project created by `make benchmark`; invoking
the Python service runner against an arbitrary developer stack is deliberately
rejected.

`make benchmark-search` runs the report-only semantic-search benchmark against
the disposable service URL in `PHOTON_SEARCH_URL`. It uses a fixed versioned
query corpus, configurable concurrency and duration, and reports successful
throughput plus p50, p75, p90, p95, and p99 request latency. Errors are excluded
from successful throughput and the JSON report is written under `artifacts/`.

Each downstream case sends eight records by default, measures wall-clock time
from Kafka production to verified output, and records throughput. Set
`PHOTON_SERVICE_BENCHMARK_MESSAGES` only when creating a separately labelled
baseline; it changes the workload and makes results incomparable.

The authoritative entry point is:

```bash
PHOTON_ALLOW_BENCHMARKS=1 make benchmark
```

It creates an isolated Compose project, enables Go pprof only within that test
deployment, executes the function/service/end-to-end tiers, and writes results
and diagnostics beneath `artifacts/<project>/`. It must never point at a
developer or production deployment.

To capture CPU flame graphs during one controlled E2E workload, run
`PHOTON_ALLOW_BENCHMARKS=1 make benchmark-flamegraphs`. The command records
Frontier, admission workers, Fetcher, Renderer, Extractor, Embedder, and the
cleanup worker, then writes `flamegraphs/*.flame.html` and the underlying perf
data beneath the run artifact directory. It requires the operator to configure
Linux perf permissions beforehand; the scripts do not change kernel settings.
All suite output is written to the run's `*.stdout.log` files, including
failures, so the terminal stays quiet except for Make's exit status.

The suite combines `docker-compose.yml` with `docker-compose.benchmark.yml`.
The override uses fresh named volumes for Redis, MinIO, and Qdrant, preventing
old deduplication keys, objects, or vectors from contaminating a result. Kafka
and ZooKeeper also use named volumes in the base topology because their images
run as non-root users.

## Workload and result contract

`tests/performance/mixed-crawl.v1.json` is the versioned, seeded workload. It
mixes static, dynamic, redirect, slow, robots-blocked, and three payload-size
cases; duplicates and a hot-host skew intentionally exercise Redis admission
and politeness. Origin aliases provide multiple controlled hosts without public
DNS traffic. The benchmark runner records the workload version and seed so a
result is comparable only to runs with the same workload and runner shape.

Each JSON report has version `1`, `report_only: true`, runner metadata, raw
samples, percentile/dispersion summaries, diagnostics, and an explicit `valid`
flag. Missing completion evidence or a failed required benchmark marks a report
invalid instead of emitting a plausible-looking score. Optional profile capture
is recorded independently because absence of a pprof endpoint must not turn an
otherwise valid throughput observation into fabricated data.

## What is captured

The diagnostic collector saves Compose logs/state, Docker resource snapshots,
runner metadata, Frontier metrics and host diagnostics, fixture-origin request
records, Prometheus target/query snapshots, Kafka consumer-group state, Redis
INFO/slowlog output, Qdrant metrics, and Go CPU/heap/goroutine/block/mutex
profiles. The origin records start and completion time, response status, payload
size, request concurrency, user agent, host, and correlation query/header. Its
timestamps are the source of truth for politeness checks.

Set `PHOTON_BENCHMARK_BASELINE` to a previously published compatible JSON
result to emit a compact `comparison.md`; otherwise the current baseline is
explicitly reported as a candidate rather than compared to an incompatible run.

Run default, 2×, and 4× service/worker configurations through the staging
Compose override used by the runner. Keep each override's CPU/memory limits and
image digests pinned, because Docker `deploy.resources` and host scheduling can
otherwise make comparisons meaningless. Results establish baselines first;
only after stable history exists should a scheduled comparison become a gate.
