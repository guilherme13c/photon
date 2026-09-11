#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
corpus_file="${CORPUS_FILE:-${script_dir}/../corpus.txt}"
frontier_url="${FRONTIER_URL:-http://localhost:8080}"
limit=""

usage() {
  cat <<'EOF'
Usage: ./scripts/seed-urls.sh [--n <count>]

Submits URLs from corpus.txt to the Frontier ingestion endpoint. By default,
every usable URL in the corpus is submitted. Set CORPUS_FILE or FRONTIER_URL
to override their default locations.

Examples:
  ./scripts/seed-urls.sh
  ./scripts/seed-urls.sh --n 20
  CORPUS_FILE=./my-corpus.txt ./scripts/seed-urls.sh --n=10
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --n)
      [[ $# -ge 2 ]] || { printf '%s requires a count\n' "$1" >&2; exit 2; }
      limit="$2"
      shift 2
      ;;
    --n=*)
      limit="${1#*=}"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -n "$limit" && ! "$limit" =~ ^[1-9][0-9]*$ ]]; then
  printf '%s must be a positive integer\n' '--n' >&2
  exit 2
fi

[[ -r "$corpus_file" ]] || { printf 'Corpus file is not readable: %s\n' "$corpus_file" >&2; exit 1; }

urls=()
while IFS= read -r raw_url || [[ -n "$raw_url" ]]; do
  url="${raw_url%$'\r'}"
  [[ -z "$url" || "$url" == \#* ]] && continue
  urls+=("$url")
  [[ -n "$limit" && ${#urls[@]} -ge $limit ]] && break
done < "$corpus_file"

if [[ ${#urls[@]} -eq 0 ]]; then
  printf 'No usable URLs found in %s\n' "$corpus_file" >&2
  exit 1
fi

payload='{"urls":['
for index in "${!urls[@]}"; do
  (( index > 0 )) && payload+=','
  payload+="\"${urls[index]}\""
done
payload+=']}'

curl --fail-with-body --silent --show-error \
  --request POST "${frontier_url}/ingest" \
  --header 'Content-Type: application/json' \
  --data "$payload"

printf '\nSubmitted %d URL(s) from %s to %s.\n' "${#urls[@]}" "$corpus_file" "$frontier_url"
