#!/usr/bin/env bash
# Compares indexer and shard settings by replaying the same lines at full speed.
# Variants run interleaved (A B C D, A B C D, ...) so drift and warmup do not
# favor one of them. Results land in results/phase4/capacity-<timestamp>/.
# Usage: [VARIANTS="B-w4-s1:4:1 C-w4-s3:4:3"] scripts/bench-capacity.sh [rounds] [lines]
set -euo pipefail
rounds="${1:-5}"; lines="${2:-1000000}"
root="$(cd "$(dirname "$0")/.." && pwd)"; cd "$root"
dir="results/phase4/capacity-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$dir"
# name:workers:shards
read -ra variants <<< "${VARIANTS:-A-w1-s1:1:1 B-w4-s1:4:1 C-w4-s3:4:3 D-w1-s3:1:3}"
for r in $(seq 1 "$rounds"); do
  for v in "${variants[@]}"; do
    IFS=: read -r name workers shards <<< "$v"
    echo "round $r $name" >&2
    scripts/bench-ingest.sh "$name-r$r" "$workers" "$shards" "$lines" 0 "$dir"
  done
done
# Leave the stack on the defaults.
docker compose up -d --wait --force-recreate indexer > /dev/null 2>&1
echo "$dir"
