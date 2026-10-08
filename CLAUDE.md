# CLAUDE.md

Guidance for AI agents (and humans) working in this repository.

## Project

A distributed log platform in Go. Data flows agent → collector (gRPC) → Kafka → indexer → OpenSearch → query API. See README.md for the architecture and docs/design.md for the reasoning behind it.

## Rules

- **Explain every major design decision in docs/design.md.** Cover delivery guarantees, idempotency, and offset handling especially. When a change alters one of these, update the matching section in the same change. Write each section as the decision, the reason, and the cost.
- **Never report a benchmark number without the command and the raw output that produced it.** Save results to the committed `results/` directory. Each file starts with the command, the commit, and the host. `scripts/e2e.sh` and `make bench` do this already. Name the limiter when you report a number, and say how many runs it is.
- **Keep later phases out of scope until the user asks for them.** Phases 1 (core pipeline), 2 (chaos tests), 3 (Redis), and 4 (load benchmarks) are done. Do not start any of these unprompted:
  - Phase 5: live tail, alerting, and the OpenTelemetry demo.
  - Phase 6: Helm, kind, and AWS on EC2.

## Invariants that must hold

- A log ID is a pure function of (agent ID, source path, epoch, offset), defined in `internal/logid`. Never assign IDs randomly or from the clock.
- The collector acks a batch only after Kafka acks every accepted record in it.
- The indexer commits offsets only after every record in the poll is indexed or dead-lettered.
- The agent's registry and spool cursor may lag reality but must never lead it.
- A chaos scenario must prove its fault took effect (hard outages stall indexing, redelivery scenarios absorb redundant writes). A scenario that can pass without disrupting anything is a bug.
- Redis is never a reason the pipeline stops. The limiter and the cache fail open, and the collector slows an over-quota service instead of rejecting it.
- Before trusting a benchmark number, confirm the work happened and name the limiter. Phase 4 produced four convincing wrong results (overlapping workers, a bind mount that delivers a growing file in bursts, carry-over between runs, a reused file name). Paced-load runs must write the log inside Docker (`VIA_VOLUME=1`), and each run needs a fresh indexer and a settle.
- When moving or deleting result files, name each file. A glob once swept up committed Phase 1 results.
- The agent (`internal/agent`) and the zero-loss checker (`internal/zeroloss`) frame lines through the same `internal/lineio.Framer`.

## Commands

```bash
make up                 # build images, start the stack, wait for health checks
make data               # download Loghub HDFS_v1 into data/loghub (gitignored)
make e2e LINES=100000   # replay through the stack and run the zero-loss checker
make test               # unit tests (-race) and integration tests (testcontainers; needs Docker)
make lint               # golangci-lint and buf lint
make bench              # Go microbenchmarks, saved to results/
make chaos-up           # start the stack with Toxiproxy between the services
make chaos              # kill services and break networks mid-stream, verify zero loss (about 9 minutes)
make chaos-down         # stop the chaos stack
make bench-up           # start the stack with the benchmark overlay (compose.bench.yaml)
make bench-capacity     # compare indexer and shard settings, interleaved rounds
make bench-sweep        # end-to-end latency at fixed offered loads
make proto              # regenerate gen/ after editing proto/
make ci                 # run .github/workflows/ci.yml locally with act
make down               # stop the stack and delete its volumes
```

## Layout

- `cmd/<service>`: one main package per binary. Each wires config from environment variables and calls into `internal/`.
- `internal/`: all logic. Each package has unit tests next to it.
- `chaos/`: fault-injection tests behind the `chaos` build tag. They drive the running Compose stack with `docker compose kill` and Toxiproxy. They are not part of CI.
- `integration/`: tests behind the `integration` build tag that run against real Kafka and OpenSearch containers.
- `proto/` holds the protobuf definitions. `gen/` holds the generated code, which is committed. CI fails if `gen/` is stale.
