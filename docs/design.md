# Design decisions

This document explains why the pipeline is built the way it is. Each section states the decision, the reason, and what it costs. Read it before you change delivery, idempotency, or offset handling. Those three properties depend on each other.

## The guarantee: at-least-once delivery into an idempotent sink

**Decision.** Every hop delivers at least once. OpenSearch deduplicates on write. The net effect is that each log line appears in OpenSearch exactly once.

**Why.** Exactly-once delivery across a file, a gRPC stream, Kafka, and OpenSearch needs a transaction that spans systems that do not share one. Kafka transactions stop at Kafka's edge, and OpenSearch has no transactions. An idempotent sink gets the same visible result with far less machinery. Every component may retry freely, and the sink absorbs the duplicates.

**Cost.** Duplicates still cross the network and get re-indexed during failures. That costs throughput, not correctness.

The rest of this document is a consequence of that choice. Each component must make sure of two things. It must never drop a line it has not handed on durably. And a retried line must arrive with the same identity as the original.

## Log IDs are derived, not generated

**Decision.** A line's ID is `sha256(agent_id, source path, epoch, byte offset)`, truncated to 128 bits and hex-encoded (`internal/logid`). The OpenSearch document `_id` is this ID.

**Why.** A random ID assigned when the agent reads a line breaks idempotency at the first hop. If the agent crashes after reading a line but before recording its position, it re-reads the line on restart and gives it a new random ID. OpenSearch then stores two documents. A derived ID makes every retry path converge:

| Retry path | Why the ID is the same |
|---|---|
| Agent re-reads a line after a crash | Same file, same offset |
| Agent resends a spooled batch after a stream failure | The spool stores the ID |
| Collector's Kafka producer retries | Same record bytes |
| Indexer reprocesses after a crash before commit | Same Kafka record |

The ID also makes the zero-loss checker independent of the pipeline. The checker reads the source file, frames it the way the agent does (`internal/lineio`), and computes every ID itself. It does not trust a ledger that the system under test wrote.

**The epoch.** An offset identifies a line only while the file keeps its content. When a file is truncated (its size drops below the read position) or replaced (its inode changes), the agent increments the file's epoch and restarts at offset 0. New content then gets new IDs and cannot overwrite old documents.

**Costs and limits.**

- `AGENT_ID` must be stable for a host. If you change it, the agent ships its files again as new documents.
- If a file is truncated and then grows past the old offset between two polls, the agent cannot detect the truncation. Every offset-based tailer, Filebeat included, has this limit. Rename-based rotation does not have it.
- Two identical lines at different offsets are different log events with different IDs. That is correct for logs.

## Agent: write-ahead spool on every batch

**Decision.** The agent appends and fsyncs every batch to an on-disk spool before it sends it (`internal/spool`). It does not spool only when the collector is down. The sender reads from the spool and advances an ack cursor only when the collector confirms the batch. The agent deletes segments only after every record in them is acknowledged.

**Why.** One code path covers normal operation, outages, and crashes. Buffering during an outage needs no special mode, because the spool just grows. A memory-first design would need two paths and would lose in-memory batches on a crash.

**Ordering rules that keep it safe.**

- *The registry may lag the spool but never lead it.* The registry records how far into each file the agent has spooled. The batcher updates it only after the spool append returns. A stale registry re-reads lines, and they produce the same IDs.
- *The ack cursor may lag acknowledgements but never lead them.* Acks can arrive out of order across batches. The cursor moves only over a contiguous run of acknowledged batches, so it never skips an unacked one. The cursor is persisted once a second. A stale cursor resends batches that downstream deduplicates.
- *A torn tail is not data.* A crash during an append leaves a partial record. On open, the spool truncates the newest segment to the last record whose CRC-32C checks out. The batcher never treated that record as durable, because its fsync had not returned, so the registry still points before it.

**Backpressure.** When the spool reaches `AGENT_SPOOL_MAX_BYTES`, appends fail, the batcher waits, and the tailers stop reading. The source file is then the buffer. The agent loses no data, though a file that is deleted while the agent is paused is lost.

**Reconnects.** The agent reconnects with exponential backoff and full jitter, a random delay in `[0, min(30s, 100ms × 2^attempt)]`. Jitter stops a fleet of agents that lost the collector together from reconnecting together. The attempt counter resets once a stream makes progress.

**Cost.** One fsync per batch. A batch holds up to 1,000 lines, so the fsync is amortized, and `make bench` reports its cost (`BenchmarkAppend`).

## Collector: ack only after Kafka acks

**Decision.** For each batch, the collector validates entries, publishes the valid ones to Kafka with `acks=all` and the idempotent producer, and waits for every record's acknowledgement. Only then does it send the batch ack.

**Why.** The ack is the agent's permission to delete. An early ack would let a Kafka failure lose data the agent had already discarded.

**Pipelining.** Waiting on Kafka serially per batch would cap a stream at one batch per Kafka round trip. The receive goroutine starts each batch's Kafka writes without waiting. The stream goroutine waits for the batches in arrival order and sends their acks. Acks stay in order, and up to `COLLECTOR_MAX_INFLIGHT` batches overlap their Kafka round trips.

**Two kinds of failure, two responses.**

- *Invalid entry* (bad ID, empty or oversized message, timestamp more than 24 hours in the future, unknown level). The entry is reported in the ack's `rejected` list and never retried. Retrying cannot fix it.
- *Kafka write failure.* The stream ends with gRPC `Unavailable`. The agent backs off, reconnects, and resends everything not yet acknowledged. Some records in the failed batch may already be in Kafka, and the sink absorbs those duplicates.

**Authentication.** Each stream carries an `x-api-key` header. The collector maps the key to a service name and stamps that name on every entry. Agents cannot claim another service's name. The collector keeps only SHA-256 digests of keys, so lookups do not reveal key prefixes through timing.

**Partitioning by service.** The Kafka record key is the service name, so the default murmur2 partitioner keeps each service's logs in order in one partition. The cost is a hot partition for a high-volume service. The local stack has one API key, so one partition carries all traffic. Phase 4 should measure this before anything changes it.

## Indexer: commit offsets only after the write is durable

**Decision.** The indexer polls up to `INDEXER_MAX_POLL_RECORDS` records and decodes them. It bulk-indexes the good ones and writes the bad ones to the dead-letter topic. It commits the poll's offsets only after every record is either indexed or dead-lettered (`internal/indexer`).

**Why.** A committed offset tells Kafka never to redeliver the record. Committing before the write succeeds would turn an indexer crash into data loss. Committing after the write turns the crash into a redelivery, and the redelivery overwrites the same `_id`.

**Details that keep it correct.**

- *Bulk results are checked per item.* A `_bulk` call can return HTTP 200 while individual items fail. A 429 or 5xx item is retried with backoff, and only the failed items are resent. Any other 4xx (a mapping conflict, a malformed document) fails the same way forever, so the indexer dead-letters it and moves on.
- *Dead letters are written before the commit,* with `acks=all`. They keep the original bytes plus headers that name the error, source topic, partition, and offset.
- *The `index` op, not `create`.* `create` returns 409 on a duplicate, and the indexer would have to treat that 409 as success. `index` overwrites in place, which is simpler and also repairs a document if its mapping changes.
- *The daily index is a function of the entry's own timestamp* (`logs-YYYY.MM.DD`). A redelivered entry lands in the same index as the original, so its `_id` collides as intended. An index chosen from the indexing time would spread duplicates across days.
- *`BlockRebalanceOnPoll`.* A rebalance cannot revoke partitions while a batch is in flight, so a batch is never committed by a consumer that no longer owns it. The cost is that a long OpenSearch outage stalls rebalances. Retries continue, without commits, until OpenSearch returns.
- *A failed commit is not an error.* The documents are already written, so the next owner reprocesses them and overwrites them in place.
- *Shutdown.* The batch in progress gets `SHUTDOWN_GRACE` to finish and commit. If it cannot, it stays uncommitted and is redelivered.

## Query API: keyset pagination with a total order

**Decision.** Results sort by `(timestamp desc, id desc)`. The cursor is the last result's sort key, and OpenSearch `search_after` resumes from it.

**Why.** Offset pagination (`from`/`size`) gets slower with depth and stops at 10,000 results. `search_after` costs the same at any depth. Many log lines share a timestamp, so the ID tiebreak gives a total order. Without it a page boundary could skip or repeat lines.

**Further choices.**

- The API fetches `limit + 1` hits, so it knows whether a next page exists without a second request.
- The cursor is opaque base64 and carries a fingerprint of the filters it was issued for. A cursor reused with different filters returns 400, not a wrong page.
- There is no point-in-time snapshot. A line indexed during pagination with a timestamp already paged past does not appear. That is acceptable for log search. PIT is the upgrade if it ever matters.
- Every error has the shape `{"error": {"code", "message", "field"}}`. Validation happens once in `query.Parse`, so the search code trusts its input.
- `/healthz` reports that the process is alive. `/readyz` checks that OpenSearch answers and is not red. Neither requires a key.

## Observability and lifecycle

Every service logs JSON through `log/slog` and serves Prometheus metrics. The agent, collector, and indexer serve them on an admin port (`:9100`). The query API serves them on its main port. The metrics cover throughput (lines read, entries published, records indexed), errors (rejections, publish errors, stream errors, bulk retries, dead letters, auth failures), latency histograms at each hop including end to end, and consumer lag per partition. The Grafana dashboard `Log Platform Pipeline` charts them.

Each service handles SIGTERM:

- The agent stops reading, spools what it has read, and keeps sending for up to `AGENT_DRAIN_TIMEOUT`.
- The collector calls `GracefulStop`, so in-flight batches finish publishing and acking, then flushes the producer.
- The indexer finishes and commits its current batch.
- The query API drains its HTTP connections.

## What the chaos tests show (Phase 2)

The chaos suite in `chaos/` runs against the real Compose stack. It writes a uniquely named 60,000-line log file at 3,000 lines/s, injects a fault four seconds in, holds it for several seconds, heals it, and then runs the zero-loss checker on that file. Faults are SIGKILLs (`docker compose kill`) and Toxiproxy network faults on the agent to collector, services to Kafka, and indexer to OpenSearch links. `make chaos-up` starts the stack with the Toxiproxy overlay (`compose.chaos.yaml`), and `make chaos` runs the suite.

**Each scenario checks that it did something.** A fault that never bit would pass trivially. For hard outages the test asserts that fewer than half of the lines written during the fault were indexed during it. Every hard outage in the final run indexed 0%.

**Redelivery is exercised on purpose, and counted.** Most faults never cause a duplicate, because a sender that never got an ack simply resends something nobody stored. A duplicate appears only when a write succeeded and its acknowledgement or offset commit did not. Two scenarios are built to hit that window and assert that OpenSearch absorbed redundant writes. The count is `index_total` on the `logs-*` primaries minus the lines written, and it is greater than zero only if the same ID was indexed more than once.

- *Collector acks lost.* A Toxiproxy `reset_peer` on the downstream direction drops the connection after 700 ms, so some batches reach Kafka and their acks never reach the agent. The agent resends them.
- *Indexer killed after the write, before the commit.* OpenSearch responses are delayed by three seconds, and the test kills the indexer when OpenSearch has performed a write that the indexer has not yet heard back about. Kafka redelivers those records to the restarted indexer. An earlier version killed after a fixed delay and missed the window two times in three, so the scenario now watches for the window instead.

**Two changes came out of it.**

- *Killing the indexer cost about 33 seconds of recovery.* The cause is the consumer group session timeout, which defaults to 45 s in the Kafka client. Kafka does not reassign a dead member's partitions until that timeout expires, so a restarted indexer sat idle. `INDEXER_SESSION_TIMEOUT` now defaults to 10 s. The trade-off is that an indexer whose heartbeats stop for 10 s loses its partitions. The client sends heartbeats from a background goroutine, so a slow OpenSearch should not stop them. The scenarios hold faults for 8 s, shorter than the timeout, so a stall longer than 10 s is untested.
- *A bulk request had no timeout.* A connection that goes silent, which the blackhole scenario simulates, would block the indexer until the connection died. `INDEXER_BULK_TIMEOUT` (default 30 s) abandons the request and retries it. This is safe because the retry writes the same IDs. A unit test covers it. The chaos blackhole scenario recovers either way, because its fault is removed after eight seconds, so it does not prove this change.

**What these tests do not cover.**

- Kafka has one broker with replication factor 1. "Kill Kafka" restarts the same broker with its data volume, so it shows the producers and consumers ride out a broker restart, not a replicated failover.
- The runs are short (about 20 s of writes). They do not cover slow leaks or behavior at the spool size limit.
- The agent's host is never lost, so the spool and registry on disk are always there when it restarts. A log file deleted while the agent is down is lost.
- Killing OpenSearch keeps its data volume. Losing OpenSearch's data is a backup question, not a delivery one.
- Each scenario ran once in the final run. Only the written-but-uncommitted kill was repeated (five times, all passing).

## Deliberately out of scope so far

- TLS between agent and collector, and on Kafka and OpenSearch. The local stack disables OpenSearch security.
- Kafka replication. The local broker has `replication.factor=1`, so `acks=all` means one replica.
- Redis caching and rate limiting (Phase 3). Redis runs in Compose but nothing uses it yet.
- Load benchmarks (Phase 4).
