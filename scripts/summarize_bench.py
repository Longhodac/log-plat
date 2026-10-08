#!/usr/bin/env python3
"""Summarize a bench-capacity or bench-sweep results directory.

Reads the <label>.json, <label>.stats, and <label>.metrics files that
scripts/bench-ingest.sh wrote, and prints per-variant medians and ranges, the
busiest container, and error counters. It computes nothing the raw files do not
contain.

Usage: scripts/summarize_bench.py results/phase4/capacity-<timestamp>
"""
import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def cpu_by_container(stats_path):
    """Return {container: [cpu% samples]} from a docker stats capture."""
    out = defaultdict(list)
    for line in stats_path.read_text().splitlines():
        m = re.match(r"logplat-(\w+)-1 cpu=([\d.]+)%", line)
        if m:
            out[m.group(1)].append(float(m.group(2)))
    return out


def variant_of(label):
    return re.sub(r"-r\d+$", "", label)


def main(path):
    d = Path(path)
    runs = defaultdict(list)
    for jf in sorted(d.glob("*.json")):
        label = jf.stem
        r = json.loads(jf.read_text())
        cpu = cpu_by_container(d / f"{label}.stats") if (d / f"{label}.stats").exists() else {}
        errors = {}
        mf = d / f"{label}.metrics"
        if mf.exists():
            for line in mf.read_text().splitlines():
                m = re.match(r"(logplat_\w+(?:\{[^}]*\})?) ([\d.e+]+)$", line)
                if m and any(k in m.group(1) for k in ("errors", "retries", "spool_full", "auth_fail", "corrupt")):
                    errors[m.group(1)] = float(m.group(2))
        runs[variant_of(label)].append((label, r, cpu, errors))

    print(f"{'variant':10} {'runs':>4} {'median lines/s':>15} {'min':>8} {'max':>8} {'zero loss':>10} "
          f"{'p50 ms':>8} {'p99 ms':>8}")
    for v, rs in sorted(runs.items()):
        tp = [r["ingest_lines_per_sec"] for _, r, _, _ in rs]
        p50 = [r["latency"]["p50_ms"] for _, r, _, _ in rs]
        p99 = [r["latency"]["p99_ms"] for _, r, _, _ in rs]
        ok = sum(1 for _, r, _, _ in rs if r["zero_loss"])
        print(f"{v:10} {len(rs):>4} {statistics.median(tp):>15.0f} {min(tp):>8.0f} {max(tp):>8.0f} "
              f"{ok:>4}/{len(rs):<5} {statistics.median(p50):>8.0f} {statistics.median(p99):>8.0f}")

    print("\nper-run throughput (lines/s):")
    for v, rs in sorted(runs.items()):
        print(f"  {v:10}", "  ".join(f"{r['ingest_lines_per_sec']:.0f}" for _, r, _, _ in rs))

    print("\nCPU while the measured replay ran (mean / peak of docker stats samples, % of one core):")
    for v, rs in sorted(runs.items()):
        agg = defaultdict(list)
        for _, _, cpu, _ in rs:
            for c, xs in cpu.items():
                agg[c].extend(xs)
        cells = []
        for c in ("agent", "collector", "kafka", "indexer", "opensearch"):
            xs = agg.get(c, [])
            if xs:
                cells.append(f"{c} {statistics.mean(xs):.0f}/{max(xs):.0f}")
        print(f"  {v:10}", "  ".join(cells), f"  (samples per container: {len(agg.get('opensearch', []))})")

    bad = [(label, k, val) for rs in runs.values() for label, _, _, e in rs for k, val in e.items()
           if val > 0 and "stream_errors" not in k]
    print("\nnon-zero error counters (excluding agent stream errors, which accumulate from earlier tests):",
          bad if bad else "none")


if __name__ == "__main__":
    main(sys.argv[1])
