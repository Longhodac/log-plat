#!/usr/bin/env bash
# Replays Loghub lines through the running compose stack, then runs the
# zero-loss checker. Writes the command, environment, and raw output to
# results/e2e-<timestamp>.{txt,json}.
# Usage: scripts/e2e.sh [lines] [rate]   (rate 0 = as fast as possible)
set -euo pipefail

lines="${1:-100000}"
rate="${2:-0}"
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
input="${INPUT:-data/loghub/HDFS.log}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
file="data/run/replay-$run_id.log"
result="results/e2e-$run_id"

[[ -s "$input" ]] || { echo "missing $input; run 'make data' first" >&2; exit 1; }
mkdir -p data/run results bin
go build -o bin/ ./cmd/loggen ./cmd/zerolosscheck

cpus="$(sysctl -n hw.ncpu 2>/dev/null || nproc)"
{
  echo "# command: scripts/e2e.sh $lines $rate"
  echo "# date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "# commit: $(git rev-parse --short HEAD 2>/dev/null || echo none)$(git diff --quiet HEAD 2>/dev/null || echo ' (dirty)')"
  echo "# host: $(uname -sm), $cpus CPUs"
  echo "# docker: $(docker info --format '{{.ServerVersion}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes memory')"
  echo "# input: $input"
  echo "\$ bin/loggen -in $input -out $file -rate $rate -count $lines"
  bin/loggen -in "$input" -out "$file" -rate "$rate" -count "$lines"
  echo "\$ bin/zerolosscheck -file $file -source /var/log/ingest/$(basename "$file") -agent-id agent-1 -json $result.json"
  bin/zerolosscheck -file "$file" -source "/var/log/ingest/$(basename "$file")" -agent-id agent-1 -json "$result.json"
} 2>&1 | tee "$result.txt"
