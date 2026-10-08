#!/usr/bin/env bash
# One measured ingest run against the running compose stack.
#
# Usage: scripts/bench-ingest.sh <label> <workers> <shards> <lines> <rate> <outdir>
#   workers  INDEXER_BULK_WORKERS for this run
#   shards   INDEX_SHARDS for this run
#   rate     lines per second offered by loggen; 0 writes as fast as possible
#
# It points the indexer at a fresh index prefix, replays a short unmeasured
# warmup, then the measured replay, and saves next to <outdir>/<label>:
#   .txt     command, environment, and raw output
#   .json    the zero-loss checker's report (throughput, latency percentiles)
#   .stats   docker stats samples taken every 2 s during the measured replay
#   .metrics Prometheus counters from the services after the run
#
# VIA_VOLUME=1 writes the replay inside Docker onto the agent's named volume
# (compose.bench.yaml) instead of the host bind mount. Use it for paced runs,
# because a bind-mounted file that grows is seen late and in bursts on Docker
# Desktop for Mac.
#
# REUSE_PREFIX=<prefix> skips the indexer restart, the warmup, and the index
# cleanup, and uses that prefix. scripts/bench-sweep.sh uses it to keep one
# warm indexer across many runs.
set -euo pipefail

label="$1"; workers="$2"; shards="$3"; lines="$4"; rate="$5"; outdir="$6"
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
input="${INPUT:-data/loghub/HDFS.log}"
nonce="$(date +%s)"
prefix="bench-$(echo "$label" | tr "[:upper:]" "[:lower:]")"
mkdir -p "$outdir" bin data/run
go build -o bin/ ./cmd/loggen ./cmd/zerolosscheck
out="$outdir/$label"

if [[ -n "${VIA_VOLUME:-}" ]]; then
  export COMPOSE_FILE=compose.yaml:compose.bench.yaml
fi
# write_log <name> <rate> <count>: replay lines into data/run (bind mount) or,
# with VIA_VOLUME, into the named volume and then copy the finished file to the
# host so the checker can frame it.
write_log() {
  local name="$1" rate="$2" count="$3"
  if [[ -n "${VIA_VOLUME:-}" ]]; then
    docker compose run --rm -T loggen -in /in/$(basename "$input") -out "/out/$name" -rate "$rate" -count "$count"
    docker run --rm -v logplat_ingest:/d:ro alpine cat "/d/$name" > "data/run/$name"
  else
    bin/loggen -in "$input" -out "data/run/$name" -rate "$rate" -count "$count"
  fi
}
drop_log() {
  rm -f "data/run/$1"
  [[ -n "${VIA_VOLUME:-}" ]] && docker run --rm -v logplat_ingest:/d alpine rm -f "/d/$1" > /dev/null 2>&1 || true
}

warm=""
if [[ -n "${REUSE_PREFIX:-}" ]]; then
  prefix="$REUSE_PREFIX"
else
  INDEX_PREFIX="$prefix" INDEX_SHARDS="$shards" INDEXER_BULK_WORKERS="$workers" \
    docker compose up -d --wait --force-recreate indexer > /dev/null 2>&1

  # Warmup: not measured. Gives the new index, the JVM, and the connections a first pass.
  warm="warm-$label-$nonce.log"
  write_log "$warm" 0 20000 > /dev/null 2>&1
  bin/zerolosscheck -file "data/run/$warm" -source "/var/log/ingest/$warm" -agent-id agent-1 \
    -index-prefix "$prefix" -stall 30s > /dev/null 2>&1 || { echo "warmup failed for $label" >&2; exit 1; }
fi

# Settle: OpenSearch keeps merging and refreshing after a heavy ingest, and that
# background work slows whatever runs next. Wait until it has been idle for
# three seconds in a row, up to two minutes.
quiet=0
for _ in $(seq 1 120); do
  merging="$(curl -s 'localhost:9200/_nodes/stats/indices?filter_path=nodes.*.indices.merges.current' | python3 -c 'import sys,json; print(sum(n["indices"]["merges"]["current"] for n in json.load(sys.stdin)["nodes"].values()))' 2>/dev/null || echo 1)"
  queued="$(curl -s 'localhost:9200/_cat/thread_pool/write?h=active,queue' | awk '{s+=$1+$2} END{print s+0}')"
  if [[ "$merging" == 0 && "$queued" == 0 ]]; then quiet=$((quiet+1)); else quiet=0; fi
  (( quiet >= 3 )) && break
  sleep 1
done

name="bench-$label-$nonce.log"
file="data/run/$name"
src="/var/log/ingest/$name"
(
  [[ -n "${NO_STATS:-}" ]] && exit 0
  while :; do
    echo "--- $(date -u +%H:%M:%S)"
    docker stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}' \
      logplat-agent-1 logplat-collector-1 logplat-indexer-1 logplat-kafka-1 logplat-opensearch-1
  done
) > "$out.stats" 2>/dev/null &
statspid=$!
disown $statspid 2>/dev/null || true
trap 'kill $statspid 2>/dev/null || true' EXIT

{
  echo "# label: $label  workers=$workers shards=$shards lines=$lines rate=$rate"
  echo "# date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "# commit: $(git rev-parse --short HEAD 2>/dev/null || echo none)$(git diff --quiet HEAD 2>/dev/null || echo ' (dirty)')"
  echo "# host: $(uname -sm), $(sysctl -n hw.ncpu 2>/dev/null || nproc) CPUs; load: $(uptime | sed 's/.*load averages*: //')"
  echo "# docker: $(docker info --format '{{.ServerVersion}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes')"
  echo "\$ loggen -in $input -out $name -rate $rate -count $lines  (via_volume=${VIA_VOLUME:-no})"
  write_log "$name" "$rate" "$lines"
  echo "\$ bin/zerolosscheck -file $file -source $src -agent-id agent-1 -index-prefix $prefix -json $out.json"
  bin/zerolosscheck -file "$file" -source "$src" -agent-id agent-1 -index-prefix "$prefix" -stall 60s -timeout 15m -json "$out.json"
} > "$out.txt" 2>&1 || { cat "$out.txt" >&2; exit 1; }

kill $statspid 2>/dev/null || true
{
  echo "# after $label, $(date -u +%H:%M:%SZ)"
  for p in 9101 9102 9103; do
    curl -s "localhost:$p/metrics" | grep -E '^logplat_(collector_(publish_errors|auth_failures|throttle)|indexer_(records_total|bulk_retries|commit_errors)|agent_(stream_errors|spool_full|corrupt))' || true
  done
  curl -s localhost:9102/metrics | grep -E '^logplat_indexer_bulk_seconds_(sum|count)' || true
} > "$out.metrics"

# Free the disk: this run's indices have been measured and saved.
if [[ -z "${REUSE_PREFIX:-}" ]]; then
  curl -s -X DELETE "localhost:9200/$prefix-*" > /dev/null || true
fi
drop_log "$name"
[[ -n "$warm" ]] && drop_log "$warm" || true
tail -1 "$out.txt"
