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
