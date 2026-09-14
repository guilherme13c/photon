# Testing Photon

`make test-fast` is the PR-fast gate: unit tests (including the cleanup-worker
contract parser), versioned Kafka contracts, deterministic scheduler simulation,
and short fuzzing. `make test-integration` runs the Kafka-backed Go service
integration tests. `make test-functional` starts the complete Compose stack
with the `test` profile and only crawls the controlled fixture origin.

`make test-performance` and `make test-chaos` are intentionally blocked until
their respective `PHOTON_ALLOW_PUBLIC_PERFORMANCE=1` and
`PHOTON_ALLOW_CHAOS=1` opt-ins are set. Run them only on disposable staging.
They write reports and diagnostics to `artifacts/`, never use performance
thresholds as a gate, and public performance traffic obeys robots.txt and its
per-host crawl delay.

Kafka fixtures live in `tests/contracts/v1`. Changes to a wire format require
a new versioned fixture directory rather than a silent edit to v1.

`make test-capacity` submits report-only `load`, `spike`, `stress`, or `volume`
profiles to a staging Frontier. It requires `PHOTON_FRONTIER_URL` and a
controlled `PHOTON_CAPACITY_ORIGIN`; it never uses the public corpus.

See [benchmarking.md](benchmarking.md) for the separate report-only profiling
suite. `make benchmark-smoke` is safe for PRs; `make benchmark` is opt-in and
requires a disposable staging runner. `make benchmark-flamegraphs` is the
separate opt-in CPU profiling command; it also requires Linux perf access and
writes its output only to the run artifact directory.
