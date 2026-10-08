#!/usr/bin/env python3
"""Per-stage latency quantiles for one run, from Prometheus histogram deltas.

Run it with `snap` before and after a load, then with `diff`:
  scripts/stage_latency.py snap > before.txt
  ...load...
  scripts/stage_latency.py snap > after.txt
  scripts/stage_latency.py diff before.txt after.txt

Quantiles are linearly interpolated inside a bucket, so they are approximate:
the error is at most one bucket width (about 40% of the value for these buckets).
"""
import json
import re
import sys
import urllib.request

SOURCES = {
    9103: ["logplat_agent_ack_seconds", "logplat_agent_spool_append_seconds"],
    9101: ["logplat_collector_agent_dwell_seconds", "logplat_collector_publish_seconds"],
    9102: ['logplat_indexer_phase_seconds{phase="poll"}', 'logplat_indexer_phase_seconds{phase="process"}', 'logplat_indexer_phase_seconds{phase="commit"}', "logplat_indexer_kafka_dwell_seconds", "logplat_indexer_bulk_seconds", "logplat_indexer_end_to_end_seconds"],
}


def scrape():
    out = {}
    for port, names in SOURCES.items():
        text = urllib.request.urlopen(f"http://localhost:{port}/metrics").read().decode()
        for n in names:
            buckets, count, total = {}, 0.0, 0.0
            base, _, lbl = n.partition("{")
            lbl = lbl.rstrip("}")
            for line in text.splitlines():
                if lbl:
                    m = re.match(rf'{base}_bucket\{{{lbl},le="([^"]+)"\}} ([\d.e+]+)', line)
                    cnt_prefix, sum_prefix = f"{base}_count{{{lbl}}} ", f"{base}_sum{{{lbl}}} "
                else:
                    m = re.match(rf'{base}_bucket\{{le="([^"]+)"\}} ([\d.e+]+)', line)
                    cnt_prefix, sum_prefix = f"{base}_count ", f"{base}_sum "
                if m:
                    buckets[m.group(1)] = float(m.group(2))
                elif line.startswith(cnt_prefix):
                    count = float(line.split()[1])
                elif line.startswith(sum_prefix):
                    total = float(line.split()[1])
            out[n] = {"buckets": buckets, "count": count, "sum": total}
    return out


def quantile(delta, q):
    pairs = sorted(((float("inf") if le == "+Inf" else float(le)), c) for le, c in delta.items())
    total = pairs[-1][1]
    if total == 0:
        return float("nan")
    target, prev_le, prev_c = q * total, 0.0, 0.0
    for le, c in pairs:
        if c >= target:
            if le == float("inf"):
                return prev_le
            span = c - prev_c
            return prev_le + (le - prev_le) * ((target - prev_c) / span if span else 1)
        prev_le, prev_c = le, c
    return prev_le


if sys.argv[1] == "snap":
    print(json.dumps(scrape()))
else:
    a, b = json.load(open(sys.argv[2])), json.load(open(sys.argv[3]))
    print(f"{'stage':44} {'samples':>9} {'mean ms':>9} {'p50 ms':>8} {'p99 ms':>8}")
    for n in b:
        d = {le: b[n]["buckets"][le] - a[n]["buckets"].get(le, 0) for le in b[n]["buckets"]}
        cnt, s = b[n]["count"] - a[n]["count"], b[n]["sum"] - a[n]["sum"]
        mean = 1000 * s / cnt if cnt else float("nan")
        print(f"{n:44} {cnt:>9.0f} {mean:>9.1f} {1000 * quantile(d, .5):>8.1f} {1000 * quantile(d, .99):>8.1f}")
