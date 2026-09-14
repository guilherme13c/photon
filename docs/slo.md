# Photon reliability objectives

This is Photon’s operational contract. The objectives measure the crawler’s
own service, not whether a third-party origin responds successfully. Origin
4xx/5xx responses and robots exclusions are expected crawl outcomes; failures
to durably hand a record to the next pipeline stage are Photon failures.

## Service-level indicators and objectives

| User journey | SLI | 30-day SLO | Source |
| --- | --- | --- | --- |
| Static fetch handoff | successfully processed fetcher records / all terminal fetcher records | 99.9% | `fetcher_urls_processed_total` |
| Dynamic render handoff | successful renderer records / all renderer records | 99.5% | `renderer_pages_rendered_total` |
| Vector persistence | successful embedder records / all embedder records | 99.9% | `embedder_messages_processed_total` |
| Stage latency | p95 processing latency: fetcher / renderer / embedder | <= 5s / 30s / 10s | `*_process_duration_seconds` |
| Pipeline freshness | oldest consumer-group lag remains below 10,000 records | 99% of 5-minute windows | `kafka_consumergroup_lag` |

## Short-window objectives

These objectives catch a live pipeline regression before it materially spends
the 30-day error budget. They evaluate a rolling 15-minute window and alert
only after the condition persists, which avoids paging on normal Kafka batch
bursts or a just-started crawl.

| User journey | Short-term SLI | SLO | Guardrail |
| --- | --- | --- | --- |
| Document delivery throughput | successful Qdrant documents per second / Extractor-produced documents per second | at least **95%** whenever Extractor produces at least **1 doc/s** | 15-minute rate; sustained for 15 minutes |
| End-to-end latency | p95 from Fetcher/Renderer start to successful Qdrant upsert | **<= 60 seconds** | 15-minute histogram; sustained for 10 minutes |

The throughput SLI uses Embedder success as the terminal durable outcome and
Extractor-produced documents as its eligible input, so it detects losses or
stalls in Embedder and Qdrant without treating empty or filtered pages as a
failure. Its active-crawl gate prevents an idle pipeline from being classified
as a throughput failure. The 95% target is an initial delivery floor; tune it
against a pinned controlled-origin capacity baseline once one exists.

### Short-window throughput objective

`photon:terminal_throughput_per_second:15m` must stay at or above 95% of
`photon:extractor_document_throughput_per_second:15m` while the latter is at
least 1. First inspect Kafka lag and the stage-specific success/error counters;
then compare the current worker replica count and model configuration with the
capacity baseline.

### Short-window end-to-end latency objective

`photon:end_to_end_p95_seconds:15m` must remain at or below 60 seconds. Start
with Kafka consumer lag, then use the Fetcher, Renderer, and Embedder stage
histograms to locate whether queueing, origin fetches, storage, inference, or
Qdrant is responsible. The SLI ignores records without a valid ingress
timestamp, so preserve `pipeline_started_at_ms` in every envelope.

The latency objectives intentionally exclude queue time. Queueing and
politeness are part of crawler behavior, so freshness is separately protected
by consumer lag. `photon_end_to_end_duration_seconds` remains a diagnostic
histogram rather than an SLO until every ingress path carries a timestamp.

## Error-budget policy

The budgets are 0.1% for fetcher and embedder, and 0.5% for renderer, over a
rolling 30 days. A 14.4x burn rate for ten minutes pages the on-call engineer;
this exhausts a monthly budget in about two hours if it continues. A p95
latency breach or high Kafka lag creates a ticket after 15 minutes.

Before a production rollout, send `PhotonPipelineErrorBudgetBurn` and
`PhotonServiceUnavailable` to the paging route in Alertmanager. All alerts
include a linkable runbook section in their annotations.

## Logs, correlation, and traces

Each processing record gets a 128-bit opaque `correlation_id` at Fetcher or
Renderer ingress. It is propagated in `fetched-pages` and `cleaned_documents`
envelopes through Extractor to Embedder. It is a log and trace-search field,
never a Prometheus label and never an S3 object key, URL, or other user data.

Go worker lifecycle logs use stable `key=value` fields, including
`event=...` and `correlation_id=...`. Promtail captures Compose container logs
to Loki with only low-cardinality `service` and `container` labels. Search a
correlation with:

```logql
{job="photon"} |= "correlation_id=<id>"
```

Tempo is available at `tempo:4317` (OTLP/gRPC) and `tempo:4318` (OTLP/HTTP).
Instrumented clients must set `service.name`, create a consumer span for each
Kafka record and a producer span before publish, and attach `correlation_id`
as a span attribute. Do not put raw URLs, HTML, document text, Kafka payloads,
credentials, or headers in spans.

The stack is intentionally configured with Loki and Tempo even while the
non-Go services are being instrumented. This lets logs be correlated today and
gives each service one common OTLP endpoint rather than a per-service backend.

## Local operations

`docker compose up --build` starts Prometheus, Grafana, Loki, Promtail, and
Tempo. Grafana provisions all three data sources. Prometheus loads recording
and alert rules from `config/prometheus-rules/photon-slo.yml`.

For any new topic envelope, preserve `correlation_id` unchanged. Producers
must generate one only when none is present; consumers must not create a new
ID. This is the compatibility rule that maintains one traceable crawl item
across retries and stages.
