#!/usr/bin/env python3
"""Connect to the live-tail endpoint and report what arrives.

  scripts/tailwatch.py <name> <seconds> <query> [--slow-ms N] [--show N]

Prints one summary line when it finishes: events received per second, events
the server reported dropping, and the first few messages. --slow-ms sleeps
that long after each event, to imitate a client that cannot keep up.
"""
import argparse
import json
import sys
import time
import urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("name")
ap.add_argument("seconds", type=float)
ap.add_argument("query")
ap.add_argument("--url", default="http://localhost:8082")
ap.add_argument("--key", default="dev-tail-key")
ap.add_argument("--slow-ms", type=float, default=0)
ap.add_argument("--show", type=int, default=3)
a = ap.parse_args()

req = urllib.request.Request(f"{a.url}/v1/tail?{a.query}", headers={"X-API-Key": a.key})
resp = urllib.request.urlopen(req, timeout=40)
end = time.time() + a.seconds
got = dropped = 0
shown, per_sec = [], {}
event = None
start = time.time()
while time.time() < end:
    line = resp.readline()
    if not line:
        break
    line = line.decode().rstrip("\n")
    if line.startswith("event: "):
        event = line[7:]
    elif line.startswith("data: "):
        if event == "dropped":
            dropped += json.loads(line[6:])["dropped"]
        elif event == "log":
            # Counting is all most clients need, so parse only the few that are shown.
            got += 1
            sec = int(time.time() - start)
            per_sec[sec] = per_sec.get(sec, 0) + 1
            if len(shown) < a.show:
                d = json.loads(line[6:])
                shown.append(f'{d["level"]:5} {d["host"] or "-":10} {d["message"][:90]}')
            if a.slow_ms:
                time.sleep(a.slow_ms / 1000)
resp.close()
rates = [per_sec.get(i, 0) for i in range(int(a.seconds))]
print(f"{a.name}: received {got} events, server reported {dropped} dropped; per-second: {rates}")
for s in shown:
    print(f"  first events: {s}")
