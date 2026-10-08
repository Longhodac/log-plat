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
| **net-collector-ack-loss** (first version) | 6% | 8.5 | **1,407** |
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

## Phase 4: load benchmarks

All numbers are from one MacBook (Apple Silicon, 10 CPUs), with Docker Desktop's VM given 10 CPUs and 8 GB. The load generators, Kafka, OpenSearch, Redis and the services all share that machine, and other desktop apps were running (load average 4 to 6 when runs started). Treat the absolute numbers as this laptop's. The ratios and the limiters are the findings. The input is Loghub HDFS_v1, about 140 bytes a line.

"Zero loss" below means the checker found every expected ID in OpenSearch, with none missing and none extra. All 36 capacity runs, 30 sweep runs and 6 linger runs had zero loss, and error counters stayed at 0.

### Ingest throughput

**What was compared.** The indexer sends a poll's documents to OpenSearch in `INDEXER_BULK_WORKERS` parallel `_bulk` requests, and the index has `INDEX_SHARDS` shards. Four variants replayed 1,000,000 lines at full speed. The first campaign ran five interleaved rounds of all four (A B C D, A B C D, and so on), and a second campaign added eight interleaved rounds of B and C because their ranges overlapped. Each run used a fresh index and a fresh indexer, preceded by an unmeasured 20,000-line warmup.

**Commands.** `scripts/bench-capacity.sh 5 1000000`, then `VARIANTS="B-w4-s1:4:1 C-w4-s3:4:3" scripts/bench-capacity.sh 8 1000000`. Summaries come from `scripts/summarize_bench.py` and `scripts/compare_variants.py`. The raw files are in [capacity-20261006T031406Z](phase4/capacity-20261006T031406Z/) and [capacity-20261006T032844Z](phase4/capacity-20261006T032844Z/). Each run has a `.txt` (command, host, raw output), `.json` (the checker's report), `.stats` (`docker stats` samples) and `.metrics` (counters).

| Variant | Bulk workers | Shards | Runs | Median lines/s | Range | Against A |
|---|---|---|---|---|---|---|
| A (the Phase 1 behavior) | 1 | 1 | 5 | 39,895 | 39,240 to 43,579 | |
| B (the new default) | 4 | 1 | 13 | 69,706 | 61,713 to 77,622 | 1.75x, p = 0.0002 |
| C | 4 | 3 | 13 | 82,988 | 66,872 to 94,859 | 2.08x |
| D | 1 | 3 | 5 | 63,299 | 53,862 to 67,861 | 1.59x, p = 0.008 |

C against B is 1.19x (p = 0.001), and C against D is 1.31x (p = 0.0005). The p-values are two-sided Mann-Whitney tests on the per-run rates ([capacity-comparison.txt](phase4/capacity-comparison.txt)). With five runs per side, the first campaign could not separate B from C, so the second campaign added runs. Both parallel bulk requests and extra shards help, and they stack.

**The default.** Compose now sets `INDEXER_BULK_WORKERS=4` and leaves `INDEX_SHARDS=1`, which is variant B. A default-settings replay of 1,000,000 lines (one run) gave 76,138 lines/s with zero loss ([e2e-20261006T033656Z.txt](phase4/e2e-20261006T033656Z.txt)). I did not make 3 shards the default because I did not measure what extra shards do to query latency.

**Why not double again.** The limiter at full speed is the indexer's own cycle. [saturation-phases.txt](phase4/limiter-profile/saturation-phases.txt) times each phase of 501 poll cycles while 2,000,000 lines were indexed on the default settings. The indexer spent 33.4 s in "process" (encoding plus the parallel bulk writes, 46 ms per request), 0.3 s committing, and the remaining wait was mostly idle time before and after the load. So the indexer was busy writing for essentially the whole run, and it does not start the next poll until the current one is written and committed. In variant A, OpenSearch used a mean of 104% of one core with the indexer at 23%. In C, OpenSearch used a mean of 320% (peak 560%) of the 1,000% available. Neither side is saturated, which fits a serial cycle with a fixed amount of parallelism, but I did not test that explanation by pipelining polls. The next step would be to fetch the next poll while the current one writes, while still committing offsets in order.

An earlier 3,000,000-line run of C ([limiter-profile/](phase4/limiter-profile/)) shows the same shape from outside. The collector had all 3,000,000 entries in Kafka about 10 s after the run began, while indexing continued to about 54 s. So the agent and collector are not the limit at these rates.

### End-to-end latency at steady load

**Definition.** Latency is `indexed_at` minus `observed_at` on each document, computed exactly over every document of a run. `observed_at` is when the agent read the line. `indexed_at` is when the indexer began the bulk write for the poll that held it. It includes the agent's batching and spool, the collector, Kafka, and the wait to be polled. It does not include the bulk write itself (about 40 ms at the median) or OpenSearch's refresh interval. **A line becomes searchable only after the next refresh, which is up to 5 s by default** (`INDEX_REFRESH_INTERVAL`), and I did not measure that.

**Command.** `scripts/bench-sweep.sh 5`. Each run replays about 20 s of load at a fixed rate on a fresh indexer and index, after a warmup and a wait for OpenSearch to go quiet. Rates are visited in a rotated order each round. The raw files are in [sweep-20261006T051553Z](phase4/sweep-20261006T051553Z/).

| Offered lines/s | Runs | Achieved lines/s | p50 ms | p99 ms | Median of the max, ms |
|---|---|---|---|---|---|
| 5,000 | 5 | 5,087 (4,977 to 5,424) | 117 (117 to 119) | 233 (227 to 239) | 247 |
| 10,000 | 5 | 10,206 (10,016 to 10,825) | 20 (18 to 34) | 154 (131 to 263) | 230 |
| 20,000 | 5 | 20,660 (19,761 to 21,920) | 21 (19 to 22) | 333 (239 to 609) | 402 |
| 40,000 | 5 | 40,252 (39,196 to 44,104) | 34 (24 to 45) | 450 (265 to 1,034) | 589 |
| 60,000 | 5 | 59,429 (42,908 to 62,099) | 637 (122 to 3,174) | 1,590 (878 to 8,951) | 1,696 |
| full speed | 5 | 62,096 (48,218 to 68,306) | 6,659 (5,723 to 9,137) | 12,319 (11,831 to 17,964) | 12,378 |

Cells are the median over five runs, with the range in parentheses. Latency is flat and low up to 40,000 lines/s. The knee is between 40,000 and 60,000, where offered load reaches capacity and lines start to queue. At full speed the p50 is queue depth, not pipeline delay.

The full-speed row (62,096) is lower than variant B above (69,706). The sweep writes the file while the agent reads it, and the capacity runs read a file that was already complete. I did not investigate the gap further, so do not compare the two tables directly.

**Why the p50 is 117 ms at 5,000 lines/s but about 20 ms at 10,000 and 20,000.** At low rates a batch of 1,000 lines takes 200 ms to fill, so the agent's 200 ms linger timer, not the batch size, decides when it sends. I tested that by changing only `AGENT_BATCH_LINGER`. Three interleaved runs each at 5,000 lines/s ([linger/](phase4/linger/)) gave a p50 of 119, 118 and 119 ms with the 200 ms linger, and 66, 66 and 65 ms with 50 ms. The p99 fell from about 240 ms to about 117 ms. All six runs had zero loss. The linger is a latency and batch-size trade-off, and 200 ms stays the default.

### Query latency

**Setup.** 1,000,000 documents in one shard, with two query APIs over the same OpenSearch, one with the Redis cache and one without (`compose.bench.yaml`), and no rate limit on either. Each run had 2 s of warmup and 10 s measured. Five rounds alternated which API ran first. `scripts/gen_query_targets.py` makes the query lists: a single repeated query, 300,000 distinct queries, and a 30-page walk with cursors. The distinct mix is service and level filters, block-ID and word searches, and time windows of 1 to 60 minutes. About 19% of those return no rows. Each worker takes a different query, and every run starts at a random point in the list. Commands are `scripts/gen_query_targets.py` then `scripts/bench-query.sh <targets> results/phase4/query/run-<time> 5`. The raw files and the target-file hashes are in [run-20261006T040352Z](phase4/query/run-20261006T040352Z/). The 300,000-query file is not committed. It is regenerated from a fixed seed.

| Queries | Workers | Cache | Requests/s | p50 ms | p99 ms | Cache hits |
|---|---|---|---|---|---|---|
| repeated | 1 | off | 110 | 8.72 | 12.0 | |
| repeated | 1 | on | 2,368 | 0.37 | 1.6 | 100% |
| repeated | 16 | off | 611 | 24.5 | 51.1 | |
| repeated | 16 | on | 6,873 | 2.15 | 5.2 | 100% |
| 30-page walk | 1 | off | 12 | 81.7 | 115.8 | |
| 30-page walk | 1 | on | 1,546 | 0.53 | 2.0 | 100% |
| 30-page walk | 16 | off | 62 | 254.7 | 446.0 | |
| 30-page walk | 16 | on | 3,801 | 3.40 | 8.7 | 99% |
| distinct | 1 | off | 276 | 2.04 | 26.7 | |
| distinct | 1 | on | 266 | 2.17 | 27.9 | 1% |
| distinct | 16 | off | 1,708 | 6.83 | 45.1 | |
| distinct | 16 | on | 1,556 | 7.81 | 51.0 | 15% |

Each cell is the median of five runs. The ranges are in the `.json` files, and `scripts/summarize_query.py` prints them. All 12 configurations returned only HTTP 200, with no transport errors.

**What it shows.** The cache is a large win for repeated searches (about 22x at one worker and 11x at sixteen, on this data). It does nothing for queries that do not repeat. On the distinct mix it is slightly slower, because every miss still pays for a Redis lookup and write. At one worker the p50 is 0.13 ms higher (2.17 against 2.04). At 16 workers the throughput ranges overlap (1,152 to 1,817 against 1,481 to 1,775), so I cannot separate that cost from noise. The 30-page walk is slow without the cache because each page of `level=info` has to sort over most of the 1,000,000 documents. Per-query cost tracks the number of matching documents.

**Limiters.** [limiter-probe.txt](phase4/query/limiter-probe.txt) samples CPU during single 12 s runs. For distinct uncached queries at 16 workers, OpenSearch used about 6 of the 10 cores, the API about 1.5 and the load generator about 15%, so OpenSearch is the limit. For cached repeats at 16 workers, OpenSearch was idle and the API process was the busiest at about 2.7 cores, so the hit path is bound by the API's own work (decoding the cached page and encoding the response). I did not profile that, and Docker Desktop's port forwarding on the loopback path probably adds to it. For the uncached 30-page walk at one worker, OpenSearch used about 1.1 cores, one query at a time.

### Measurement mistakes found and fixed

These are kept here because three of them produced convincing but wrong numbers.

1. **Workers asked the same questions.** My load generator stepped through the query list one entry at a time per worker, so 16 workers requested nearly the same query at once. The cache coalesced them (71% "hits" on a distinct mix, and 4x throughput) and OpenSearch's own caches probably helped the uncached side too. I found it because Redis showed 25,224 lookups but only 1,602 writes. The tool now strides workers apart, with a test that fails on the old behavior. All query runs above were redone.
2. **A growing bind-mounted file arrives late and in bursts on Docker Desktop for Mac.** While the host appended to a file, a container kept seeing the size 3,487,204 for about 8 s, then the final size ([bind-mount-visibility.txt](phase4/bind-mount-visibility.txt)). Paced-load latency read as queueing in Kafka (about 2 s at 20,000 lines/s) when the real cause was the agent receiving the file in bursts. Paced runs now write the log inside Docker onto a named volume the agent shares. The full-speed capacity runs are not affected, because their file is complete before the agent reads it.
3. **A run inherits the previous run's OpenSearch background work.** Five-thousand-line-per-second runs split into about 70 ms and about 720 ms with identical settings, depending on what ran before (those runs were discarded and not kept). Each run now uses a fresh indexer and index and waits for OpenSearch merges and write queues to be idle for three seconds.
4. **A reused label made the agent start a new epoch.** The agent saw a different file at a path it had already read, so it correctly changed epoch, and the checker's expected IDs no longer matched. File names are now unique per run.

The capacity runs did not use the settle step (mistake 3). Their runs are interleaved and each starts on a fresh index, so any carry-over should affect all variants alike, but I did not test that.

### What these numbers do not show

One machine, one dataset, and short runs of 10 to 70 seconds. The client and server share CPUs. I did not measure searchable latency (the refresh interval), query latency while ingest is running, or the effect of 3 shards on queries. Kafka is one broker with one partition carrying all the load, because every record is keyed by service.

### Phase 2 chaos suite rerun after Phase 4

I changed the indexer's default to 4 parallel bulk writers, so I ran the full chaos suite again on that default. The raw output of the final run is [20261006T060443Z/run.txt](chaos/20261006T060443Z/run.txt). All 17 scenarios passed with zero loss, and the two scenarios built to force duplicate deliveries absorbed 2,803 (collector acks lost) and 1,389 (indexer killed after the write, before the commit) redundant writes.

The first rerun ([20261006T054551Z](chaos/20261006T054551Z/run.txt)) failed one check and showed a flaw in my test, not in the pipeline. `net-collector-ack-loss` is meant to force duplicates, and it produced none in that run, so its own check failed. Zero loss held. I reran it 14 times with the old fault and it produced duplicates in 5 of 14 ([repeat-ack-loss-before-fix](chaos/repeat-ack-loss-before-fix/)). The first six runs used the original fault, a TCP reset after a fixed 700 ms. The next eight added a 300 ms delay on the acks and managed only 2 of 8. A reset only creates duplicates if some entries have reached Kafka while their ack is still on the way, and a timer cannot target that. The scenario now watches the collector's published count and the agent's acked count, and cuts the connection when the first is ahead. It then produced duplicates in 8 of 8 reruns, with 1,404 to 11,213 redundant writes and zero loss each time ([repeat-ack-loss-after-fix](chaos/repeat-ack-loss-after-fix/)). The final run above uses the fixed scenario.

The stall check on hard outages and the duplicate check are unchanged. All of this is single runs on one machine, with the limits listed in the Phase 2 section above.

## Phase 5: live tail, error alerts, and the OpenTelemetry demo

Single runs on the same MacBook, with the `make bench-up` stack so paced load is written inside Docker (see the Phase 4 mistakes). Raw output is in [phase5/](phase5/). Every alert shown went to a local stub on the host, never to real Slack.

### Live tail

[live-tail.txt](phase5/live-tail.txt). A paced replay of 100,000 HDFS lines at 5,000 lines/s with three clients connected at once: a fast one (every event), a filtered one (`q=allocateBlock`), and a slow one that sleeps 5 ms per event.

| Client | Events received | Events dropped by the server |
|---|---|---|
| fast | 100,000 of 100,000 | 0 |
| filtered | 7,940 | 0 |
| slow (about 165 events/s) | 3,371 in 24 s | the remaining 83,496 |

The server's counters add up: 100,000 records consumed, 124,444 events delivered, 83,496 dropped, and every drop belongs to the slow client. The slow client's own report says 0 dropped, because the `dropped` notice travels behind the events still waiting in its socket and it stopped reading first. The server counter is the reliable one. From the agent reading a line to the tail service holding it, the delay was p50 112 ms and p99 274 ms (histogram quantiles, so approximate), and that includes the agent's 200 ms batch timer.

**A buffer that was too small.** With a 1,000-event buffer per client, even the fast client lost 2.6% to 3% of events (two runs, in this file's history only), because Kafka delivers records in bursts of several thousand. The default is now 5,000. That is the setting these numbers use.

### Error-spike alerting on synthetic errors

[alerting.txt](phase5/alerting.txt). Normal HDFS traffic at 2,000 lines/s plus a burst of 400 error lines at 20/s. The alerter sent one spike message 20 s after the burst began (94 errors in the window, usually about 0), nothing during the remaining burst, and one all-clear about 35 s after the errors stopped. It counted all 400 errors. These thresholds were chosen for a quick demo (window 20 s, baseline 2 m).

### The OpenTelemetry demo as a log source

The demo's core services (20 containers, its load generator running) shipped through a second agent that reads Docker's container logs. [otel-demo-logs.txt](phase5/otel-demo-logs.txt) shows what arrived: 27,990 documents from 18 containers in about 15 minutes, with the container name as `host` and the service as `otel-demo`. I checked the classification against the raw lines: the 221 "fatal" entries are PostgreSQL's own `FATAL: role "root" does not exist`, raised by the demo's health check, and the errors from the demo's own collector failing to scrape its database. I did not run the zero-loss checker on this data, because the demo's logs are not a file I can replay, so I make no loss claim for it. The collector reported 28,145 entries published and 0 publish errors, and the 155 more than were indexed when I queried were still in flight while the demo kept logging.

**A real fault, caught.** [otel-demo-fault.txt](phase5/otel-demo-fault.txt). I switched on the demo's `cartFailure` flag for 70 s with 250 simulated users. The alerter (window 30 s, baseline 3 m, minimum 15 errors, ratio 4) sent one spike message 32 s after the fault began (40 errors in the window, usually about 9, with real cart error text), and one all-clear about 32 s after it ended. A live tail filtered to the cart container streamed 120 errors during the fault. The thresholds were the same in an earlier attempt that did not alert.

**The earlier attempts, kept honest.** Two runs found nothing, and both are informative.

- With the `paymentFailure` flag and the demo's default load, the failure produced two warn lines in a minute and nothing else. The payment service logs the failure at warn level, and the load generator checked out only about twice. The checkout service wrote no stdout lines at all, so it must log through OpenTelemetry only. A reader of container logs cannot see logs a service sends by another route.
- With `cartFailure` at 40 simulated users, the fault added about 57 errors in 70 s. That is roughly 25 per 30-second window against a bar of about four times the demo's own background noise, so the alerter correctly stayed quiet.

I raised the load, not the sensitivity, for the run that alerted. The file in the repository is that run.

### What these runs do not show

One run of each. The alert latency (about 5 s on synthetic errors and about 32 s on the real fault) depends on the window and the evaluation interval (5 s), so it is mostly a setting, not a measurement. The tail was tested with at most three clients. The detector's memory and CPU at many services were not measured. The Slack integration was tested only against a local stub and an HTTP test server, not the real Slack API.
