#!/usr/bin/env python3
"""Compare ingest throughput between capacity-benchmark variants.

Pools every run of a variant across the given capacity-* directories and prints
the medians, the ratio, and a two-sided Mann-Whitney U test p-value for each
pair. A p-value below 0.05 means the gap is larger than run-to-run variation
would usually produce.

Usage: scripts/compare_variants.py results/phase4/capacity-* -- A-w1-s1:B-w4-s1 B-w4-s1:C-w4-s3
"""
import glob
import json
import re
import statistics as st
import sys

from scipy.stats import mannwhitneyu

sep = sys.argv.index("--")
dirs, pairs = sys.argv[1:sep], sys.argv[sep + 1:]
runs = {}
for d in dirs:
    for f in glob.glob(d.rstrip("/") + "/*.json"):
        v = re.sub(r"-r\d+$", "", f.split("/")[-1][:-5])
        runs.setdefault(v, []).append(json.load(open(f))["ingest_lines_per_sec"])
for v, x in sorted(runs.items()):
    print(f"{v:10} runs={len(x):<3} median={st.median(x):>7.0f} min={min(x):>7.0f} max={max(x):>7.0f}")
for p in pairs:
    a, b = p.split(":")
    r = mannwhitneyu(runs[a], runs[b], alternative="two-sided")
    print(f"{a} vs {b}: median ratio {st.median(runs[b]) / st.median(runs[a]):.2f}, U={r.statistic:.0f}, p={r.pvalue:.4f}")
