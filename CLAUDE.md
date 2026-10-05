# CLAUDE.md

Guidance for AI agents (and humans) working in this repository.

## Project

A distributed log platform in Go. Data flows agent → collector (gRPC) → Kafka → indexer → OpenSearch → query API. See README.md for the architecture and docs/design.md for the reasoning behind it.

## Rules

- **Explain every major design decision in docs/design.md.** Cover delivery guarantees, idempotency, and offset handling especially. When a change alters one of these, update the matching section in the same change. Write each section as the decision, the reason, and the cost.
- **Never report a benchmark number without the command and the raw output that produced it.** Save results to the committed `results/` directory. Each file starts with the command, the commit, and the host. `scripts/e2e.sh` and `make bench` do this already. Name the limiter when you report a number, and say how many runs it is.
- **Keep later phases out of scope until the user asks for them.** Phase 1 (the core pipeline) is done. Do not start any of these unprompted:
  - Phase 2: chaos tests with Toxiproxy.
  - Phase 3: Redis caching and rate limiting.
  - Phase 4: load benchmarks.
  - Phase 5: live tail, alerting, and the OpenTelemetry demo.
  - Phase 6: Helm, kind, and AWS on EC2.

## Invariants that must hold

- A log ID is a pure function of (agent ID, source path, epoch, offset), defined in `internal/logid`. Never assign IDs randomly or from the clock.
- The collector acks a batch only after Kafka acks every accepted record in it.
- The indexer commits offsets only after every record in the poll is indexed or dead-lettered.
- The agent's registry and spool cursor may lag reality but must never lead it.
- The agent (`internal/agent`) and the zero-loss checker (`internal/zeroloss`) frame lines through the same `internal/lineio.Framer`.

## Commands

```bash
make up                 # build images, start the stack, wait for health checks
make data               # download Loghub HDFS_v1 into data/loghub (gitignored)
make e2e LINES=100000   # replay through the stack and run the zero-loss checker
make test               # unit tests (-race) and integration tests (testcontainers; needs Docker)
make lint               # golangci-lint and buf lint
make bench              # Go microbenchmarks, saved to results/
make proto              # regenerate gen/ after editing proto/
make ci                 # run .github/workflows/ci.yml locally with act
make down               # stop the stack and delete its volumes
```

## Layout

- `cmd/<service>`: one main package per binary. Each wires config from environment variables and calls into `internal/`.
- `internal/`: all logic. Each package has unit tests next to it.
- `integration/`: tests behind the `integration` build tag that run against real Kafka and OpenSearch containers.
- `proto/` holds the protobuf definitions. `gen/` holds the generated code, which is committed. CI fails if `gen/` is stale.
