#!/usr/bin/env bash
# Measures query API latency with and without the Redis search cache.
#
# Needs the stack up with the bench overlay and data indexed:
#   docker compose -f compose.yaml -f compose.bench.yaml up -d --wait
# and target files from scripts/gen_query_targets.py in <targets-dir>.
#
# Usage: scripts/bench-query.sh <targets-dir> <outdir> [rounds]
# ONLY="distinct" limits which target files run. Each round runs every (targets, workers) pair against the uncached API (port
# 8081) and the cached API (port 8080), alternating which goes first.
set -euo pipefail
targets="$1"; outdir="$2"; rounds="${3:-5}"
root="$(cd "$(dirname "$0")/.." && pwd)"; cd "$root"
mkdir -p "$outdir" bin
go build -o bin/ ./cmd/querybench
duration="${DURATION:-10s}"; warmup="${WARMUP:-2s}"

{
  echo "# date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "# commit: $(git rev-parse --short HEAD 2>/dev/null || echo none)$(git diff --quiet HEAD 2>/dev/null || echo ' (dirty)')"
  echo "# host: $(uname -sm), $(sysctl -n hw.ncpu 2>/dev/null || nproc) CPUs; load at start: $(uptime | sed 's/.*load averages*: //')"
  echo "# docs: $(curl -s 'localhost:9200/logs-*/_count' | python3 -c 'import sys,json; print(json.load(sys.stdin)["count"])')"
  echo "# duration=$duration warmup=$warmup rounds=$rounds"
  for f in "$targets"/*.txt; do echo "# targets $(basename "$f"): $(wc -l < "$f") lines, sha256 $(shasum -a 256 "$f" | cut -c1-12)"; done
} > "$outdir/header.txt"

for r in $(seq 1 "$rounds"); do
  for t in ${ONLY:-repeat distinct pages}; do
    for w in 1 16; do
      if (( r % 2 )); then order="nocache cache"; else order="cache nocache"; fi
      for mode in $order; do
        port=8081; [[ $mode == cache ]] && port=8080
        label="$t-w$w-$mode-r$r"
        echo "round $r $label" >&2
        bin/querybench -url "http://localhost:$port" -workers "$w" -duration "$duration" -warmup "$warmup" \
          -targets "@$targets/$t.txt" -json "$outdir/$label.json" > /dev/null
      done
    done
  done
done
echo "$outdir"
