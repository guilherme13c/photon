#!/usr/bin/env python3
"""Report-only function, service, and end-to-end Photon benchmark runner.

This intentionally uses only controlled origins. It writes an invalid result when
observability or completion evidence is unavailable instead of inventing a score.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import random
import subprocess
import sys
import time
from pathlib import Path
from urllib import error as urlerror, parse, request

from benchmark_lib import consumer_group_members, expected_document_urls, invalidate, new_result, output_contract, politeness_violations as find_politeness_violations, summarize, timed, write_result

ROOT = Path(__file__).resolve().parents[1]
WORKLOAD = json.loads((ROOT / "tests/performance/mixed-crawl.v1.json").read_text())


def get_json(url: str, timeout: int = 20):
    with request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read())


def get_text(url: str, timeout: int = 20) -> str:
    with request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def post_json(url: str, payload: object) -> object:
    body = json.dumps(payload).encode()
    req = request.Request(url, data=body, method="POST", headers={"Content-Type": "application/json"})
    with request.urlopen(req, timeout=60) as response:
        return json.loads(response.read())


def submit_at_rate(frontier: str, urls: list[str], rate: int) -> tuple[list[float], list[object]]:
    """Submit bounded batches at a deterministic target rate and retain latency samples."""
    batch_size = min(20, max(1, rate))
    interval = batch_size / rate
    samples, responses = [], []
    started = time.monotonic()
    for offset in range(0, len(urls), batch_size):
        response, elapsed = timed(lambda offset=offset: post_json(frontier.rstrip("/") + "/ingest", {"urls": urls[offset:offset + batch_size]}))
        responses.append(response)
        samples.append(elapsed)
        target = started + ((offset // batch_size) + 1) * interval
        if target > time.monotonic(): time.sleep(target - time.monotonic())
    return samples, responses


def controlled_urls(origin: str, profile: str) -> list[str]:
    config = WORKLOAD["profiles"][profile]
    seed = int(os.environ.get("PHOTON_BENCHMARK_SEED", WORKLOAD["seed"]))
    rng = random.Random(seed)
    weighted = [kind for kind, weight in WORKLOAD["mix"].items() for _ in range(weight)]
    hosts = [f"origin-{number}" for number in range(WORKLOAD["host_distribution"]["cold_hosts"])]
    hot = hosts[:WORKLOAD["host_distribution"]["hot_hosts"]]
    urls = []
    for number in range(config["url_count"]):
        kind = rng.choice(weighted)
        host = rng.choice(hot if rng.randrange(100) < WORKLOAD["host_distribution"]["hot_share_percent"] else hosts)
        # Host aliases resolve to the controlled origin only inside the benchmark Compose network.
        path = {"static": "/static", "dynamic": "/dynamic", "redirect": "/redirect", "slow": "/slow/0.05", "blocked": "/blocked"}.get(kind, f"/payload/{kind}")
        urls.append(f"http://{host}:8088{path}?trace_id={profile}-{number}")
    duplicate_count = len(urls) * WORKLOAD["duplicate_percent"] // 100
    urls.extend(urls[:duplicate_count])
    rng.shuffle(urls)
    return urls


def fixture_control_url(origin: str, crawl_url: str) -> str:
    """Address a generated fixture path from the host-side benchmark runner.

    Crawl URLs deliberately use ``origin-N`` host names so Frontier has distinct
    host queues.  Those aliases exist only in the Compose network; the runner
    itself executes on the host and must use the origin's published endpoint.
    """
    endpoint = parse.urlsplit(origin)
    target = parse.urlsplit(crawl_url)
    return parse.urlunsplit((endpoint.scheme, endpoint.netloc, target.path, target.query, ""))


def politeness_violations(rows: list[dict], trace_prefix: str | None = None) -> list[dict]:
    delay_ms = float(os.environ.get("PHOTON_BENCHMARK_CRAWL_DELAY_MS", float(os.environ.get("PHOTON_ORIGIN_CRAWL_DELAY", "1")) * 1000))
    tolerance_ms = float(os.environ.get("PHOTON_BENCHMARK_POLITENESS_TOLERANCE_MS", "25"))
    return find_politeness_violations(rows, delay_ms, tolerance_ms, trace_prefix)


def qdrant_points(collection: dict) -> int | None:
    try:
        return int(collection["result"]["points_count"])
    except (KeyError, TypeError, ValueError):
        return None


def qdrant_document_urls(qdrant: str, collection: str) -> set[str]:
    response = post_json(
        f"{qdrant}/collections/{collection}/points/scroll",
        {"limit": 10_000, "with_payload": ["url"], "with_vector": False},
    )
    return {
        str(point["payload"]["url"])
        for point in response.get("result", {}).get("points", [])
        if isinstance(point.get("payload"), dict) and point["payload"].get("url")
    }


def compose_command(arguments: list[str], *, input_text: str | None = None, env: dict[str, str] | None = None, timeout: int = 180) -> str:
    """Run a Compose command inside this benchmark's isolated project."""
    project = os.environ.get("COMPOSE_PROJECT_NAME")
    if not project:
        raise RuntimeError("service isolation requires COMPOSE_PROJECT_NAME; run it through `PHOTON_ALLOW_BENCHMARKS=1 make benchmark`")
    command = [
        "docker", "compose", "-p", project,
        "-f", str(ROOT / "docker-compose.yml"), "-f", str(ROOT / "docker-compose.benchmark.yml"),
        *arguments,
    ]
    command_env = os.environ.copy()
    if env:
        command_env.update(env)
    completed = subprocess.run(command, input=input_text, text=True, capture_output=True, env=command_env, timeout=timeout, check=False)
    if completed.returncode:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"{' '.join(command)} failed: {detail[-1000:]}")
    return completed.stdout


def create_topic(topic: str, partitions: int = 1) -> None:
    compose_command([
        "exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", "kafka:29092",
        "--create", "--if-not-exists", "--topic", topic, "--replication-factor", "1", "--partitions", str(partitions),
    ])


def produce(topic: str, values: list[str]) -> None:
    compose_command([
        "exec", "-T", "kafka", "kafka-console-producer", "--bootstrap-server", "kafka:29092", "--topic", topic,
    ], input_text="\n".join(values) + "\n")


def consume_records(topic: str, max_records: int, timeout_ms: int = 120_000) -> list[dict]:
    output = compose_command([
        "exec", "-T", "kafka", "kafka-console-consumer", "--bootstrap-server", "kafka:29092", "--topic", topic,
        "--from-beginning", "--max-messages", str(max_records), "--timeout-ms", str(timeout_ms),
    ], timeout=(timeout_ms // 1000) + 900)
    records = []
    for line in output.splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            records.append(value)
    return records


def wait_for_consumer_group(group: str, topic: str, expected_members: int, expected_partitions: int, timeout: int = 90) -> None:
    """Do not publish a service case while its Kafka group is rebalancing."""
    deadline = time.monotonic() + timeout
    latest = ""
    while time.monotonic() < deadline:
        try:
            latest = compose_command([
                "exec", "-T", "kafka", "kafka-consumer-groups", "--bootstrap-server", "kafka:29092",
                "--describe", "--group", group,
            ], timeout=30)
        except RuntimeError as error:
            latest = str(error)
            time.sleep(1)
            continue
        members = consumer_group_members(latest, group, topic)
        assigned_partitions = {
            fields[2]
            for line in latest.splitlines()
            if len(fields := line.split()) >= 7 and fields[0] == group and fields[1] == topic and fields[6] != "-"
        }
        if len(members) == expected_members and len(assigned_partitions) == expected_partitions:
            return
        time.sleep(1)
    raise RuntimeError(
        f"Kafka group {group} did not stabilize with {expected_members} members and "
        f"{expected_partitions} partitions: {latest[-1000:]}"
    )


def consume_until_contract(topic: str, expected_urls: set[str], timeout: int = 120) -> tuple[list[dict], dict]:
    """Observe until all unique work completes; retain at-least-once duplicates."""
    deadline = time.monotonic() + timeout
    max_records = max(len(expected_urls) * 4, len(expected_urls) + 16)
    latest_records: list[dict] = []
    while time.monotonic() < deadline:
        remaining_ms = max(1_000, min(5_000, int((deadline - time.monotonic()) * 1000)))
        latest_records = consume_records(topic, max_records, remaining_ms)
        contract = output_contract(latest_records, expected_urls)
        if not contract["missing_urls"]:
            return latest_records, contract
    contract = output_contract(latest_records, expected_urls)
    raise RuntimeError(
        f"output topic {topic} did not complete all unique inputs: "
        f"missing={sorted(contract['missing_urls'])}, observed={sorted(contract['observed_urls'])}, "
        f"duplicates={contract['duplicate_records']}"
    )


def restart_service(service: str, environment: dict[str, str]) -> None:
    arguments = ["up", "-d", "--no-deps", "--force-recreate"]
    # Renderer scales its Chromium worker pool internally; the remaining
    # stateless consumers scale as replicas and need matching topic partitions.
    if service != "renderer":
        arguments.extend(["--scale", f"{service}={os.environ.get('PHOTON_BENCHMARK_SCALE', '1')}"])
    arguments.append(service)
    compose_command(arguments, env=environment)


def put_fixture_object(key: str, content: str) -> None:
    """Use the disposable MinIO helper image to write an Extractor fixture."""
    if not key.replace("-", "").replace("_", "").replace(".", "").isalnum():
        raise ValueError("benchmark object key contains unsupported characters")
    encoded = base64.b64encode(content.encode()).decode()
    script = (
        "mc alias set photon http://minio:9000 \"$MINIO_ROOT_USER\" \"$MINIO_ROOT_PASSWORD\" >/dev/null && "
        f"base64 -d | mc pipe photon/html-payloads/{key} >/dev/null"
    )
    compose_command([
        "run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "init-minio", "-ec", script,
    ], input_text=encoded, timeout=120)


def wait_for_documents(qdrant: str, collection: str, urls: set[str], timeout: int = 180) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if urls.issubset(qdrant_document_urls(qdrant, collection)):
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError(f"Qdrant did not contain {len(urls)} benchmark documents before timeout")


def run_function(output: Path) -> int:
    result = new_result("function", {"corpus": "mixed-crawl.v1", "warmup_seconds": 0, "samples_per_case": 5})
    commands = {
        "frontier": ["zig", "build", "bench", "-Doptimize=ReleaseFast"],
        "extractor": ["zig", "build", "bench", "-Doptimize=ReleaseFast"],
        "fetcher": ["go", "test", "-run=^$", "-bench=.", "-benchmem", "./service"],
        "renderer": ["go", "test", "-run=^$", "-bench=.", "-benchmem", "./service"],
        "embedder": [sys.executable, "tests/performance/microbench_embedder.py"],
    }
    import subprocess
    for name, command in commands.items():
        cwd = ROOT / name if name in {"frontier", "extractor", "fetcher", "renderer"} else ROOT
        started = time.perf_counter()
        try:
            completed = subprocess.run(command, cwd=cwd, text=True, capture_output=True, timeout=300, check=False)
            result["samples"].append({"case": name, "elapsed_ms": (time.perf_counter() - started) * 1000, "exit_code": completed.returncode, "output": completed.stdout[-12000:], "stderr": completed.stderr[-4000:]})
            if completed.returncode:
                invalidate(result, f"{name} microbenchmark failed")
        except (OSError, subprocess.SubprocessError) as error:
            invalidate(result, f"{name} microbenchmark unavailable: {error}")
    result["summary"] = {"cases": len(result["samples"])}
    write_result(result, output)
    return 0 if result["valid"] else 1


def run_service(output: Path, frontier: str, origin: str, profile: str) -> int:
    scale = os.environ.get("PHOTON_BENCHMARK_SCALE", "1")
    result = new_result("service", {
        "profile": profile, "scale": int(scale), "services": ["frontier", "fetcher", "renderer", "extractor", "embedder"],
        "isolation": "Each downstream case recreates its target with dedicated input/output topics and a unique consumer group."
    })
    qdrant = os.environ.get("PHOTON_QDRANT_URL", "http://localhost:6333").rstrip("/")
    collection = os.environ.get("QDRANT_COLLECTION_NAME", "photon_documents")
    message_count = int(os.environ.get("PHOTON_SERVICE_BENCHMARK_MESSAGES", "100"))
    if message_count < 1:
        raise ValueError("PHOTON_SERVICE_BENCHMARK_MESSAGES must be positive")

    # Frontier is inherently an HTTP admission service. Its downstream stream is
    # deliberately not observed here: the scheduler and full pipeline are covered
    # by the deterministic and E2E tiers respectively.
    try:
        urls = [url.replace(f"trace_id={profile}-", f"trace_id=service-frontier-{scale}-") for url in controlled_urls(origin, profile)[:30]]
        _, elapsed = timed(lambda: post_json(frontier.rstrip("/") + "/ingest", {"urls": urls}))
        result["samples"].append({"service": "frontier", "dependency_mode": "real", "completion": "durable admission response", "latency": summarize([elapsed]), "raw_latency_ms": [elapsed]})
        result["diagnostics"]["frontier_metrics"] = get_text(frontier.rstrip("/") + "/metrics")
        result["diagnostics"]["hosts"] = get_json(frontier.rstrip("/") + "/debug/hosts?limit=100")
    except Exception as error:
        invalidate(result, f"Frontier service benchmark failed: {error}")

    def topic(service: str, role: str) -> str:
        return f"benchmark-{service}-{role}-{scale}"

    def record(service: str, started: float, completion: str, detail: dict) -> None:
        elapsed = (time.monotonic() - started) * 1000
        result["samples"].append({
            "service": service, "dependency_mode": "real", "completion": completion,
            "latency": summarize([elapsed]), "raw_latency_ms": [elapsed], "message_count": message_count,
            "throughput_per_second": message_count / (elapsed / 1000), "evidence": detail,
        })

    try:
        input_topic, output_topic = topic("fetcher", "input"), topic("fetcher", "output")
        for name in (input_topic, output_topic, topic("fetcher", "dynamic"), topic("fetcher", "dlq")): create_topic(name, int(scale))
        restart_service("fetcher", {
            "FETCHER_KAFKA_TOPIC": input_topic, "FETCHER_KAFKA_PRODUCER_TOPIC": output_topic,
            "FETCHER_KAFKA_DYNAMIC_URLS_TOPIC": topic("fetcher", "dynamic"), "FETCHER_KAFKA_DLQ_TOPIC": topic("fetcher", "dlq"),
            "FETCHER_KAFKA_GROUP": f"benchmark-fetcher-{scale}",
        })
        urls = [f"http://origin-{index % 8}:8088/static?trace_id=service-fetcher-{scale}-{index}" for index in range(message_count)]
        wait_for_consumer_group(f"benchmark-fetcher-{scale}", input_topic, int(scale), int(scale))
        started = time.monotonic(); produce(input_topic, urls); envelopes, contract = consume_until_contract(output_topic, set(urls))
        record("fetcher", started, "fetched-pages envelopes", {
            "input_topic": input_topic, "output_topic": output_topic,
            "s3_keys": [record["s3_key"] for record in envelopes if record.get("s3_key")],
            "duplicate_records": contract["duplicate_records"], "records_observed": contract["records_observed"],
        })
    except Exception as error:
        invalidate(result, f"Fetcher service benchmark failed: {error}")

    try:
        input_topic, output_topic = topic("renderer", "input"), topic("renderer", "output")
        for name in (input_topic, output_topic): create_topic(name, int(scale))
        restart_service("renderer", {
            "RENDERER_KAFKA_TOPIC": input_topic, "RENDERER_KAFKA_PRODUCER_TOPIC": output_topic,
            "RENDERER_KAFKA_GROUP": f"benchmark-renderer-{scale}",
        })
        urls = [f"http://origin-{8 + index % 8}:8088/static?trace_id=service-renderer-{scale}-{index}" for index in range(message_count)]
        wait_for_consumer_group(f"benchmark-renderer-{scale}", input_topic, 1, int(scale))
        started = time.monotonic(); produce(input_topic, urls); envelopes, contract = consume_until_contract(output_topic, set(urls), 900)
        record("renderer", started, "fetched-pages envelopes", {
            "input_topic": input_topic, "output_topic": output_topic,
            "s3_keys": [record["s3_key"] for record in envelopes if record.get("s3_key")],
            "duplicate_records": contract["duplicate_records"], "records_observed": contract["records_observed"],
        })
    except Exception as error:
        invalidate(result, f"Renderer service benchmark failed: {error}")

    try:
        input_topic, output_topic = topic("extractor", "input"), topic("extractor", "output")
        for name in (input_topic, output_topic, topic("extractor", "discovered"), topic("extractor", "dlq")): create_topic(name, int(scale))
        restart_service("extractor", {
            "EXTRACTOR_KAFKA_INGEST_TOPIC": input_topic, "EXTRACTOR_KAFKA_CLEANED_TOPIC": output_topic,
            "EXTRACTOR_KAFKA_DISCOVERED_URLS_TOPIC": topic("extractor", "discovered"), "EXTRACTOR_KAFKA_DLQ_TOPIC": topic("extractor", "dlq"),
            "EXTRACTOR_KAFKA_GROUP_ID": f"benchmark-extractor-{scale}",
        })
        key = f"benchmark-extractor-{scale}.html"
        put_fixture_object(key, "<html><title>Extractor benchmark</title><body>benchmark document</body></html>")
        urls = [f"https://benchmark.invalid/extractor/{scale}/{index}" for index in range(message_count)]
        wait_for_consumer_group(f"benchmark-extractor-{scale}", input_topic, int(scale), int(scale))
        started = time.monotonic(); produce(input_topic, [json.dumps({"url": url, "s3_key": key}) for url in urls]); documents, contract = consume_until_contract(output_topic, set(urls))
        if {record.get("url") for record in documents} != set(urls) or any(not record.get("text") for record in documents): raise RuntimeError("Extractor output did not contain the benchmark documents")
        record("extractor", started, "cleaned document", {
            "input_topic": input_topic, "output_topic": output_topic, "s3_key": key,
            "duplicate_records": contract["duplicate_records"], "records_observed": contract["records_observed"],
        })
    except Exception as error:
        invalidate(result, f"Extractor service benchmark failed: {error}")

    try:
        input_topic = topic("embedder", "input")
        for name in (input_topic, topic("embedder", "dlq"), topic("embedder", "cleanup")): create_topic(name, int(scale))
        restart_service("embedder", {
            "EMBEDDER_KAFKA_INPUT_TOPIC": input_topic, "EMBEDDER_KAFKA_GROUP_ID": f"benchmark-embedder-{scale}",
            "EMBEDDER_KAFKA_DLQ_TOPIC": topic("embedder", "dlq"), "EMBEDDER_KAFKA_CLEANUP_TOPIC": topic("embedder", "cleanup"),
        })
        urls = {f"https://benchmark.invalid/embedder/{scale}/{index}" for index in range(message_count)}
        wait_for_consumer_group(f"benchmark-embedder-{scale}", input_topic, int(scale), int(scale))
        started = time.monotonic(); produce(input_topic, [json.dumps({"url": url, "title": "Embedder benchmark", "text": "benchmark document"}) for url in urls]); wait_for_documents(qdrant, collection, urls)
        record("embedder", started, "Qdrant vectors", {"input_topic": input_topic, "collection": collection, "urls": sorted(urls)})
    except Exception as error:
        invalidate(result, f"Embedder service benchmark failed: {error}")

    result["summary"] = {"scaling_note": "Run this target once per 1x/2x/4x Compose override; each result records dedicated Kafka-topic contracts for all services."}
    write_result(result, output)
    return 0 if result["valid"] else 1


def run_e2e(output: Path, frontier: str, origin: str, profile: str) -> int:
    result = new_result("end_to_end", {"profile": profile, "workload_version": WORKLOAD["version"], "seed": WORKLOAD["seed"]})
    urls = controlled_urls(origin, profile)
    started = time.monotonic()
    try:
        # A suite normally resets this before every profile. Keeping the reset
        # here as well makes a direct `benchmark-e2e` invocation self-contained
        # and prevents stale requests from contaminating politeness evidence.
        get_text(origin.rstrip("/") + "/__reset")
        qdrant = os.environ.get("PHOTON_QDRANT_URL", "http://localhost:6333").rstrip("/")
        collection = os.environ.get("QDRANT_COLLECTION_NAME", "photon_documents")
        try:
            before_collection = get_json(f"{qdrant}/collections/{collection}")
        except urlerror.HTTPError as request_error:
            # A disposable Qdrant volume has no collection until the Embedder
            # creates it. Treat that expected 404 as an empty baseline rather
            # than invalidating an otherwise complete end-to-end run.
            if request_error.code == 404:
                before_collection = {"result": {"points_count": 0}, "missing": True}
            else:
                before_collection = {"error": str(request_error)}
        except Exception as request_error:
            before_collection = {"error": str(request_error)}
        before_points = qdrant_points(before_collection)
        if before_points is None:
            invalidate(result, "unable to establish the initial Qdrant point count")
        rate = WORKLOAD["profiles"][profile]["arrival_rate_per_second"]
        samples, ingest = submit_at_rate(frontier, urls, rate)
        result["diagnostics"]["ingest"] = ingest
        result["samples"].append({"case": "ingest", "url_count": len(urls), "arrival_rate_per_second": rate, "latency": summarize(samples), "raw_latency_ms": samples})
        deadline = time.monotonic() + int(os.environ.get("PHOTON_BENCHMARK_COMPLETION_TIMEOUT", "300"))
        expected_origins = {url for url in urls if parse.urlsplit(url).path != "/blocked"}
        expected_documents = expected_document_urls(urls)
        after_collection = None
        observed_origins: set[str] = set()
        observed_documents: set[str] = set()
        while time.monotonic() < deadline:
            requests = get_json(origin.rstrip("/") + "/__requests")
            observed = [row for row in requests.get("requests", []) if row.get("path") not in ("/__requests", "/robots.txt")]
            observed_origins = {f"http://{row.get('host', '')}{row.get('path', '')}" for row in observed}
            try:
                after_collection = get_json(f"{qdrant}/collections/{collection}")
                observed_documents = qdrant_document_urls(qdrant, collection)
            except Exception:
                after_collection = None
                observed_documents = set()
            if expected_origins.issubset(observed_origins) and expected_documents.issubset(observed_documents):
                break
            time.sleep(2)
        else:
            result["diagnostics"]["missing_origin_urls"] = sorted(expected_origins - observed_origins)
            result["diagnostics"]["missing_document_urls"] = sorted(expected_documents - observed_documents)
            invalidate(result, "pipeline did not reach all expected origin requests and document identities before timeout")
        result["diagnostics"]["origin"] = get_json(origin.rstrip("/") + "/__requests")
        result["diagnostics"]["frontier_metrics"] = get_text(frontier.rstrip("/") + "/metrics")
        result["diagnostics"]["hosts"] = get_json(frontier.rstrip("/") + "/debug/hosts?limit=100")
        result["diagnostics"]["qdrant_before"] = before_collection
        result["diagnostics"]["qdrant_after"] = after_collection
        result["diagnostics"]["expected_document_urls"] = sorted(expected_documents)
        result["diagnostics"]["observed_document_urls"] = sorted(observed_documents)
        if after_collection is None:
            invalidate(result, "Qdrant collection verification failed")
        violations = politeness_violations(result["diagnostics"]["origin"].get("requests", []), profile)
        result["diagnostics"]["politeness_violations"] = violations
        if violations:
            invalidate(result, f"observed {len(violations)} host politeness violations")
    except Exception as error:
        invalidate(result, f"end-to-end benchmark failed: {error}")
    result["summary"] = {"elapsed_ms": (time.monotonic() - started) * 1000, "completion_accounting": "origin requests; Qdrant verification is captured by Compose diagnostics"}
    write_result(result, output)
    return 0 if result["valid"] else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("tier", choices=("function", "service", "e2e"))
    parser.add_argument("--profile", default=os.environ.get("PHOTON_BENCHMARK_PROFILE", "baseline"), choices=WORKLOAD["profiles"].keys())
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or Path(os.environ.get("PHOTON_BENCHMARK_REPORT", f"artifacts/benchmark-{args.tier}-{args.profile}.json"))
    if args.tier == "function": return run_function(output)
    frontier = os.environ.get("PHOTON_FRONTIER_URL")
    origin = os.environ.get("PHOTON_BENCHMARK_ORIGIN")
    if not frontier or not origin:
        raise SystemExit("set PHOTON_FRONTIER_URL and PHOTON_BENCHMARK_ORIGIN for service/e2e benchmarks")
    return run_service(output, frontier, origin, args.profile) if args.tier == "service" else run_e2e(output, frontier, origin, args.profile)


if __name__ == "__main__":
    raise SystemExit(main())
