#!/usr/bin/env python3
"""Write query target files for scripts/bench-query.sh.

repeat.txt    one query, repeated (what a dashboard refreshing does)
distinct.txt  many different queries: service/level filters, text searches on
              real tokens from the data, and one-to-five minute time windows
              spread over the dataset's whole time span
pages.txt     one request per page of a 30-page walk through a single result
              set, so it measures the cost of deep cursor pagination

Usage: gen_query_targets.py <sample-log-file> <outdir> <min-iso> <max-iso> [api-url] [api-key]
"""
import json
import random
import re
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

sample, outdir, lo, hi = sys.argv[1:5]
api = sys.argv[5] if len(sys.argv) > 5 else "http://localhost:8081"
key = sys.argv[6] if len(sys.argv) > 6 else "dev-query-key"
random.seed(7)
out = Path(outdir)
out.mkdir(parents=True, exist_ok=True)
lo_t = datetime.fromisoformat(lo.replace("Z", "+00:00"))
hi_t = datetime.fromisoformat(hi.replace("Z", "+00:00"))
span = (hi_t - lo_t).total_seconds()

blocks = []
with open(sample) as f:
    for i, line in enumerate(f):
        m = re.search(r"blk_(-?\d+)", line)
        if m:
            blocks.append(m.group(1))
        if i > 200000:
            break
blocks = list(dict.fromkeys(blocks))
words = ["PacketResponder", "addStoredBlock", "Receiving", "terminating", "allocateBlock", "Verification", "Served"]

def fmt(t):
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def window(minutes=(1, 2, 5)):
    m = random.choice(minutes)
    a = lo_t + timedelta(seconds=random.uniform(0, span - m * 60))
    return fmt(a), fmt(a + timedelta(minutes=m))

distinct = []
for i in range(int(__import__("os").environ.get("DISTINCT", "20000"))):
    kind = i % 4
    if kind == 0:
        distinct.append(f"/v1/logs?service=hdfs&level={random.choice(['info','warn','error'])}&limit=50")
        a, b = window((15, 30, 60))
        distinct[-1] += f"&from={a}&to={b}"
    elif kind == 1:
        distinct.append(f"/v1/logs?q=blk_{random.choice(blocks)}&limit=50")
    elif kind == 2:
        a, b = window()
        distinct.append(f"/v1/logs?service=hdfs&from={a}&to={b}&limit=100")
    else:
        a, b = window((15, 30, 60))
        distinct.append(f"/v1/logs?q={random.choice(words)}&from={a}&to={b}&limit=50")
(out / "distinct.txt").write_text("\n".join(distinct) + "\n")
(out / "repeat.txt").write_text("/v1/logs?service=hdfs&level=warn&limit=50\n")

pages, url = [], "/v1/logs?service=hdfs&level=info&limit=100"
for _ in range(30):
    pages.append(url)
    req = urllib.request.Request(api + url, headers={"X-API-Key": key})
    body = json.load(urllib.request.urlopen(req))
    cur = body.get("next_cursor")
    if not cur:
        break
    url = f"/v1/logs?service=hdfs&level=info&limit=100&cursor={cur}"
(out / "pages.txt").write_text("\n".join(pages) + "\n")
print(f"repeat=1 distinct={len(distinct)} pages={len(pages)}")
