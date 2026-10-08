#!/usr/bin/env python3
"""Summarize a scripts/bench-query.sh results directory.

Prints, per (targets, workers, cache mode), the median and range over rounds of
requests per second and of p50 and p99 latency, plus status codes, cache
headers, and error counts. Nothing is computed that the raw JSON does not hold.

Usage: scripts/summarize_query.py results/phase4/query/run-<timestamp>
"""
import json
import re
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

d = Path(sys.argv[1])
groups = defaultdict(list)
for jf in sorted(d.glob("*.json")):
    m = re.match(r"(\w+)-w(\d+)-(cache|nocache)-r(\d+)$", jf.stem)
    if m:
        groups[(m.group(1), int(m.group(2)), m.group(3))].append(json.loads(jf.read_text()))


def rng(xs, fmt="{:.1f}"):
    return f"{fmt.format(st.median(xs))} ({fmt.format(min(xs))}-{fmt.format(max(xs))})"


print(f"{'targets':9} {'wk':>3} {'mode':8} {'runs':>4} {'req/s':>22} {'p50 ms':>20} {'p99 ms':>22}  status / cache / errors")
for key in sorted(groups):
    rs = groups[key]
    t, w, mode = key
    status = defaultdict(int)
    hit = miss = err = bad = 0
    for r in rs:
        for code, n in r["status_counts"].items():
            status[code] += n
        hit += r["cache_hit_responses"]
        miss += r["cache_miss_responses"]
        err += r["transport_errors"]
        bad += r["empty_or_short_bodies"]
    total = sum(status.values())
    ok = status.get("200", 0)
    tail = f"200={ok}/{total}"
    if mode == "cache":
        tail += f" hit={100 * hit / max(hit + miss, 1):.0f}%"
    tail += f" transport_err={err} short_bodies={bad}"
    print(f"{t:9} {w:>3} {mode:8} {len(rs):>4} {rng([r['requests_per_sec'] for r in rs], '{:.0f}'):>22} "
          f"{rng([r['p50_ms'] for r in rs], '{:.2f}'):>20} {rng([r['p99_ms'] for r in rs], '{:.1f}'):>22}  {tail}")
