# Results

Every number reported for this project links to the raw output that produced it. Each `e2e-<timestamp>.txt` file starts with the command, the commit, and the host, followed by the raw output. The matching `.json` file is the checker's report.

## Phase 1: zero-loss replay through the local stack

**Setup.** `make up` stack on a MacBook (Darwin arm64, 10 CPUs). Docker Desktop VM with 10 CPUs and 8 GB of memory. One agent, one collector, one indexer, one Kafka broker, and one OpenSearch node with a single shard. Input is the first lines of Loghub `HDFS_v1`. `loggen -rate 0` writes the whole file at once, so the run measures how fast the pipeline drains a backlog.

**Metric.** Throughput is `lines / (last indexed_at − first observed_at)`. The interval runs from the agent's first read of the file to the indexer's last successful bulk write, using timestamps stored on each document. The checker verifies every expected ID before it reports the number.

**Command.** `scripts/e2e.sh 100000 0`, which `make e2e` also runs.

| Run | Lines | Zero loss | Seconds | Lines/s |
|---|---|---|---|---|
| [e2e-20261005T223334Z](e2e-20261005T223334Z.txt) | 100,000 | yes | 2.824 | 35,411 |
| [e2e-20261005T223350Z](e2e-20261005T223350Z.txt) | 100,000 | yes | 2.792 | 35,817 |
| [e2e-20261005T223358Z](e2e-20261005T223358Z.txt) | 100,000 | yes | 2.523 | 39,635 |
| [e2e-20261005T223409Z](e2e-20261005T223409Z.txt) | 100,000 | yes | 2.605 | 38,388 |
| [e2e-20261005T223422Z](e2e-20261005T223422Z.txt) | 100,000 | yes | 2.296 | 43,554 |
| [e2e-20261005T223435Z](e2e-20261005T223435Z.txt) | 1,000,000 | yes | 24.45 | 40,900 |

The median of the five 100k runs is 38,388 lines/s, with a range of 35,411 to 43,554. The single 1M run gave 40,900 lines/s. The load average was 9.35 when the runs started, from image builds and desktop apps, so treat the range as noisy.

**Limiter.** The indexer-to-OpenSearch stage limits throughput.

- [Prometheus counters](e2e-20261005T223435Z-prometheus.txt) for the 1M run show the collector publishing all 1,000,000 entries to Kafka within one 5-second scrape interval. The indexer then took about 25 seconds, and consumer lag peaked at 632,833.
- [`docker stats`](e2e-20261005T223435Z-docker-stats.txt) for the same run shows OpenSearch at about 100% CPU (one core) and the indexer at about 25%, while the agent and collector sat idle.
- The indexer sends one bulk request of up to 5,000 documents at a time, into one shard, with p50 bulk latency of 88 ms. The agent and collector can therefore drain the backlog at least 5 times faster than the indexer.
- Error counters stayed at 0 across all runs: publish errors, stream errors, rejections, bulk retries, and commit errors.

Phase 4 tunes this stage: concurrent bulk requests, more shards and partitions, and refresh settings.

## Phase 2: chaos tests

**Setup.** The `make chaos-up` stack: the Phase 1 services with Toxiproxy between the agent and collector, the services and Kafka, and the indexer and OpenSearch (`compose.chaos.yaml`). Same MacBook as above. Each scenario writes a 60,000-line file at 3,000 lines/s, injects a fault at 4 s, holds it, heals it, and runs the zero-loss checker. Faults are SIGKILLs and Toxiproxy toxics.

**Command.** `make chaos`. The raw output of the final run is [20261005T235058Z/run.txt](chaos/20261005T235058Z/run.txt), and each scenario has a JSON report next to it.

**Result.** All 17 scenarios passed, and the checker found 60,000 of 60,000 lines (zero missing, zero unexpected) in every one.

| Scenario | Lines indexed during the fault | Seconds from last write to all indexed | Redundant writes absorbed |
|---|---|---|---|
| kill-collector | 0% | 4.5 | 0 |
| kill-kafka | 0% | 0.5 | 0 |
| kill-indexer | 0% | 4.5 | 0 |
| kill-agent | 0% | 2.5 | 0 |
| kill-opensearch | 0% | 4.9 | not measurable, OpenSearch's counter resets on restart |
| net-collector-down | 0% | 10.8 | 0 |
| net-kafka-down | 0% | 2.6 | 0 |
| net-opensearch-down | 0% | 4.8 | 0 |
| net-collector-latency (800 ms +/- 400 ms) | 0% | 4.6 | 0 |
| net-collector-resets (reset after 300 ms) | 0% | 2.7 | 0 |
| net-kafka-bandwidth (50 KB/s) | 0% | 4.6 | 0 |
| net-opensearch-blackhole | 0% | 4.6 | 0 |
| **net-collector-ack-loss** | 6% | 8.5 | **1,407** |
| **kill-indexer-after-write-before-commit** | 3% | 8.6 | **1,402** |
| crash-loop-collector (3 kills) | 5% | 3.1 | 0 |
| crash-loop-indexer (3 kills) | 0% | 11.7 | 0 |
| kill-everything-in-sequence | 0% | 28.8 | 0 |

"Lines indexed during the fault" is the share of the lines written during the fault window that were already in OpenSearch when the fault healed. For hard outages the test requires it to stay under 50%, so a fault that did nothing fails the test. "Redundant writes absorbed" is OpenSearch's `index_total` delta minus 60,000. It is above zero only when the same log ID was written more than once. The two bold scenarios are built to force that, and the test fails if they do not.

**Repeatability of the redelivery scenario.** `kill-indexer-after-write-before-commit` ran five more times, saved in [repeat-kill-indexer-after-write-before-commit/](chaos/repeat-kill-indexer-after-write-before-commit/). All five passed with zero loss and 1,399, 1,404, 1,793, 2,772, and 2,796 redundant writes absorbed. An earlier version of this scenario killed after a fixed delay and failed to force redelivery in two of three runs (not saved), so it now waits for the exact window.

**Before and after the indexer change.** The [baseline run](chaos/20261005T233018Z/run.txt) predates the 10 s session timeout and has no redundant-write counter. In it, `kill-indexer` took 32.6 s from the last write to all lines indexed, and `kill-everything-in-sequence` took 44.7 s. In the final run they took 4.5 s and 28.8 s. Each is a single run. The mechanism is that Kafka holds a dead consumer's partitions until its session times out, which was 45 s and is now 10 s. The remaining 28.8 s in the sequence scenario comes from three restarts in a row, and I did not break it down further.

**Second full run.** A repeat of `make chaos` on the same stack also passed all 17 with zero loss, in [20261006T001211Z/](chaos/20261006T001211Z/). The two duplicate-forcing scenarios absorbed 1,404 and 1,964 redundant writes. `net-collector-resets` let 36% of its fault-window lines through, against 0% in the first run.

**Noise.** `net-collector-latency` let 49% of the fault-window lines through in the baseline and 0% in the final run, so that scenario varies a lot. It has no stall assertion for that reason.

## Phase 3: rate limiting and the search cache

Single runs on the same MacBook, the `make up` stack. Raw output is in [phase3/](phase3/).

**Collector limit.** `COLLECTOR_RATE_LIMIT=5000` (burst 5,000) on the collector, then `scripts/e2e.sh 100000 0`. The raw output is [e2e-20261006T024504Z.txt](phase3/e2e-20261006T024504Z.txt). All 100,000 lines were indexed with zero loss in 19.04 s, which is 5,253 lines/s. The expected time is (100,000 - 5,000 burst) / 5,000 = 19 s, so the limit held to within the measurement. The collector's [metrics](phase3/collector-throttle-metrics.txt) show 18.8 s spent throttled, 70 limited decisions, and 0 publish errors. The unlimited run of the same command, in [e2e-20261006T024409Z.txt](phase3/e2e-20261006T024409Z.txt), was 20,000 lines at 10,599 lines/s. That run is a smaller replay than the Phase 1 runs, so do not compare it with them.

**Query API.** [query-cache-and-ratelimit.txt](phase3/query-cache-and-ratelimit.txt) shows the same search returning `X-Cache: MISS` and then `HIT`, a different filter missing, and 300 back-to-back requests at a limit of 50/s with a burst of 100. 237 returned 200 and 63 returned 429, with `Retry-After: 1`. An unauthenticated request returned 401 without touching the bucket. I did not measure cached against uncached latency. That belongs to Phase 4.
