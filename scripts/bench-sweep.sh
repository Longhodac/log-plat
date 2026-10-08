#!/usr/bin/env bash
# Measures end-to-end latency at fixed offered loads, plus one full-speed point.
# Below capacity a line should not wait in a queue, so latency there is the
# pipeline's own delay. At full speed latency is mostly queue depth.
#
# Each run replays about 20 seconds of load. Rates are visited in a rotated
# order each round so that no rate always runs first or last.
# Results land in results/phase4/sweep-<timestamp>/.
# Usage: scripts/bench-sweep.sh [rounds]
set -euo pipefail
rounds="${1:-5}"
root="$(cd "$(dirname "$0")/.." && pwd)"; cd "$root"
dir="results/phase4/sweep-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$dir"
export VIA_VOLUME=1
rates=(5000 10000 20000 40000 60000 0)

for r in $(seq 1 "$rounds"); do
  for k in "${!rates[@]}"; do
    rate="${rates[$(( (k + r - 1) % ${#rates[@]} ))]}"
    if [[ "$rate" == 0 ]]; then lines=1000000; label="rate-max-r$r"; else lines=$(( rate * 20 )); label="rate-$rate-r$r"; fi
    echo "round $r $label" >&2
    # A fresh indexer and index per run, a short warmup, then a settle, so no run
    # inherits the previous run's backlog or OpenSearch's background work.
    scripts/bench-ingest.sh "$label" 4 1 "$lines" "$rate" "$dir"
  done
done
docker compose up -d --wait --force-recreate indexer > /dev/null 2>&1
echo "$dir"
