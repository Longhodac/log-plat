#!/usr/bin/env python3
"""Summarize a scripts/bench-sweep.sh results directory: per offered rate, the
median and range over rounds of achieved rate and exact end-to-end latency.
Usage: scripts/summarize_sweep.py results/phase4/sweep-<timestamp>
"""
import glob
import json
import re
import statistics as st
import sys
from collections import defaultdict

runs = defaultdict(list)
for f in sorted(glob.glob(sys.argv[1].rstrip("/") + "/*.json")):
    m = re.match(r"rate-(\w+)-r(\d+)\.json$", f.split("/")[-1])
    if m:
        runs[m.group(1)].append(json.load(open(f)))


def rg(xs):
    return f"{st.median(xs):.0f} ({min(xs):.0f}-{max(xs):.0f})"


print(f"{'offered/s':>9} {'runs':>4} {'zero loss':>9} {'achieved lines/s':>20} {'p50 ms':>16} {'p99 ms':>18} {'max ms (median)':>16}")
for k in ["5000", "10000", "20000", "40000", "60000", "max"]:
    rs = runs.get(k, [])
    if not rs:
        continue
    print(f"{k:>9} {len(rs):>4} {sum(r['zero_loss'] for r in rs):>6}/{len(rs)} "
          f"{rg([r['ingest_lines_per_sec'] for r in rs]):>20} {rg([r['latency']['p50_ms'] for r in rs]):>16} "
          f"{rg([r['latency']['p99_ms'] for r in rs]):>18} {st.median([r['latency']['max_ms'] for r in rs]):>16.0f}")
