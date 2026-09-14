#!/usr/bin/env bash
# Capture simultaneous, workload-time CPU flame graphs for every Photon worker.
set -euo pipefail
source "$(dirname "$0")/test-harness.sh"
redirect_stdout_to_artifact flamegraph-capture.stdout.log

duration="${PHOTON_FLAMEGRAPH_SECONDS:-30}"
out="$PHOTON_ARTIFACT_DIR/flamegraphs"
template="$(dirname "$0")/flamegraph-template.html"
services=(frontier admission-worker fetcher renderer extractor embedder cleanup-worker)
mkdir -p "$out"

command -v perf >/dev/null || { echo "perf is required" >&2; exit 2; }
[[ -r "$template" ]] || { echo "missing flame-graph template: $template" >&2; exit 2; }

# perf_event_paranoid controls event availability, but attaching to Docker-owned
# service processes also requires ptrace-level access. Reuse a pre-authorized
# sudo session when available; otherwise retain perf's diagnostic in the
# per-service record error file.
perf_record=(perf)
perf_record_uses_sudo=0
if sudo -n true 2>/dev/null; then
  perf_record=(sudo -n perf)
  perf_record_uses_sudo=1
fi

pids=()
names=()
for service in "${services[@]}"; do
  containers="$(compose ps -q "$service")"
  [[ -n "$containers" ]] || { echo "$service: no running container" >"$out/$service.error"; continue; }
  service_pids="$(docker inspect --format '{{.State.Pid}}' $containers | paste -sd, -)"
  "${perf_record[@]}" record --quiet --freq "${PHOTON_FLAMEGRAPH_HZ:-99}" --call-graph dwarf \
    --pid "$service_pids" --output "$out/$service.perf.data" -- sleep "$duration" \
    >"$out/$service.record.out" 2>"$out/$service.record.err" &
  pids+=("$!")
  names+=("$service")
done

render_failed=0
for index in "${!pids[@]}"; do
  wait "${pids[$index]}" || { echo "${names[$index]}: perf capture failed" >>"$out/${names[$index]}.error"; continue; }
  service="${names[$index]}"
  if (( perf_record_uses_sudo )); then
    # perf creates its data file as root when it attaches to Docker-owned PIDs.
    # Return ownership before rendering so the generated graphs are usable by
    # the invoking developer and the normal perf-script invocation can read it.
    sudo -n chown "$(id -u):$(id -g)" "$out/$service.perf.data"
  fi
  if ! perf script -s /usr/lib/perf/scripts/python/flamegraph.py -i "$out/$service.perf.data" -- \
    --format json --output "$out/$service.flame.json" \
    >"$out/$service.render.out" 2>"$out/$service.render.err"; then
    render_failed=1
  fi
  if ! perf script -s /usr/lib/perf/scripts/python/flamegraph.py -i "$out/$service.perf.data" -- \
    --format html --template "$template" --output "$out/$service.flame.html" \
    >>"$out/$service.render.out" 2>>"$out/$service.render.err"; then
    render_failed=1
  fi
done

printf '%s\n' "$out"
(( render_failed == 0 ))
