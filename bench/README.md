# bench

The measuring equipment. `docs/06-performance.md` calls this milestone zero:
a benchmark a skeptic can reproduce is worth more than any number in a README.

## Running it

The harness needs a scratch Postgres. **It truncates every table at the start
of a run**, so never point it at anything you care about.

```bash
docker run -d --name huxio-bench -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=bench -p 5432:5432 postgres:16-alpine

export HUXIO_DATABASE_URL='postgres://postgres:postgres@localhost:5432/bench?sslmode=disable'
export HUXIO_ENCRYPTION_KEY="$(go run ./cmd/huxio keygen)"
export HUXIO_JWT_SECRET=bench

make bench            # throughput
make bench-isolation  # the product claim
make bench-all        # every short scenario
```

Each run writes `bench/results/<scenario>-<commit>.json` with the percentiles,
the commit and the machine. Results are not committed; a recorded baseline in
`bench/baselines/` is, and CI compares against it.

## Scenarios

| Scenario | What it applies | What it proves |
|---|---|---|
| `throughput` | One tenant, null sinks, fixed rate | The fixed cost per delivery: deliveries/sec, /sec/core, allocations each |
| `isolation` | 10 tenants, a fifth pointed at a 60s tarpit, 30s per phase | **The product.** Healthy-tenant p99 stays within 2x of the same run's tarpit-free baseline |
| `retry-storm` | Every endpoint failing, then recovering | Backoff spreads load, breakers open, and the backlog drains on recovery |
| `cold-start` | TLS sinks, handshake per delivery | The cold-connection cost, to compare against the warm throughput run |
| `soak` | Moderate rate for minutes | No growth in goroutines or queue depth — this is where the leak shows up |

The isolation scenario measures the healthy tenants **twice in one run**: once
alone, then again with the tarpitted tenants sending alongside. The pass
condition is that comparison, so nobody has to trust a number from another
machine.

`retry-storm` compresses the retry schedule by default, because the production
schedule's second delay is five minutes and no short run can wait it out. Pass
`--retry-schedule=5s,5m,30m,2h,5h,10h,10h` for a long run against the real
delays.

## Sinks

`bench/sinks` are the four endpoint behaviours that matter, each available over
HTTP and HTTPS:

- `null` — 200 immediately: the fixed-cost baseline.
- `tarpit` — 200 after 30s: head-of-line blocking and lane isolation.
- `flaky` — 500 at a configurable rate: retry storms and breakers.
- `slowloris` — 200 with the body trickled: read timeouts and connection leaks.

## Regression gate

```bash
make bench-baseline   # record the current numbers
go run ./cmd/huxio bench --scenario=throughput --baseline=bench/baselines/throughput.json
```

The gate fails when deliveries/sec drops, or ingest p99, delivery p99 or
allocations per delivery rise, by more than 15%. It is cheap to run and it is
the only thing that stops slow drift.
