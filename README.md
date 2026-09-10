# huxio

Self-hosted webhook delivery. One Go binary, Postgres, nothing else.

[![ci](https://github.com/plusiv/huxio/actions/workflows/ci.yml/badge.svg)](https://github.com/plusiv/huxio/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![go](https://img.shields.io/badge/go-1.26-00ADD8.svg)](go.mod)

You POST an event. huxio makes it durable, signs it, and takes responsibility
for delivering it to your customers' endpoints until it succeeds or is provably
exhausted. Retries, circuit breakers, a delivery log, replay, and a portal your
customers can use to debug their own integrations.

Signatures follow [Standard Webhooks](https://www.standardwebhooks.com/), so
every receiver verification library works unchanged, and the REST surface
mirrors the common hosted service closely enough that existing client SDKs work
against it.

## Why another one

Most webhook senders start as a loop in the application: a background job picks
up events, POSTs them, retries on failure. It works until one customer's
endpoint starts taking 30 seconds to respond, and then everybody's webhooks are
late, because that slow endpoint is holding workers that other tenants needed.

huxio is built around the opposite property:

> **One tenant's broken endpoints cannot slow down anyone else's deliveries.**

That is not a tuning parameter here. It is structural, and it is measured. Each
endpoint gets its own dedicated slice of concurrency, its own rate limiter, and
its own circuit breaker, so it physically cannot consume more than its share.
An endpoint that has been failing for hours gets moved to a quarantine pool and
stops touching healthy capacity at all. There is a benchmark scenario whose only
job is to prove this, and it fails the build if the claim stops holding.

## Quick start

With Docker, the whole stack:

```bash
git clone https://github.com/plusiv/huxio && cd huxio
make keygen-env      # writes .env with a fresh encryption key and JWT secret
make up              # Postgres, migrations, API, two workers, Prometheus, Grafana
```

API on `:8080`, Prometheus on `:9090`, Grafana on `:3000` (admin/admin).

Then create a tenant and mint a token:

```bash
ORG=$(docker compose run --rm api org create "Acme")
TOKEN=$(docker compose run --rm api jwt generate "$ORG")
```

Send your first webhook:

```bash
APP=$(curl -s localhost:8080/api/v1/app -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"name":"My customer"}' | jq -r .id)

curl -s localhost:8080/api/v1/app/$APP/endpoint -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://webhook.site/YOUR-ID","filterTypes":["invoice.paid"]}'

curl -s localhost:8080/api/v1/app/$APP/msg -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"eventType":"invoice.paid","payload":{"amount":4200}}'
```

The last call returns `202` as soon as the message is durable. Delivery happens
on a worker, which is the point.

> **Testing against a local receiver?** The SSRF guard blocks private
> addresses, and that includes Docker's own network, so a sibling container
> gets `destination address is not allowed`. That is the guard working. To
> allow it deliberately, put the compose subnet in `.env`:
>
> ```bash
> echo "HUXIO_ALLOW_SUBNETS=$(docker network inspect huxio_default \
>   --format '{{(index .IPAM.Config 0).Subnet}}')" >> .env
> docker compose up -d worker
> ```

<details>
<summary>Without Docker</summary>

```bash
docker run -d --name huxio-db -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=huxio -p 5432:5432 postgres:16-alpine

export HUXIO_DATABASE_URL='postgres://postgres:postgres@localhost:5432/huxio?sslmode=disable'
export HUXIO_ENCRYPTION_KEY="$(go run ./cmd/huxio keygen)"
export HUXIO_JWT_SECRET='change-me'

go run ./cmd/huxio migrate up
go run ./cmd/huxio serve --role=all
```

</details>

## What you get

**Delivery you can reason about.** Eight attempts over 27 hours
(`5s, 5m, 30m, 2h, 5h, 10h, 10h`), each with ±10% jitter so a shared outage
does not produce a synchronised thundering herd on recovery. A `Retry-After`
from the endpoint is honoured but clamped, so a hostile or careless value
cannot pin a queue slot for a week. Timeouts and 429s get a floor, because
hammering an overloaded endpoint on the original schedule is how you keep it
overloaded.

**Signing that receivers already understand.** HMAC-SHA256 by default, ed25519
if you want asymmetric verification. Secret rotation keeps the old secret
signing alongside the new one for an overlap window, so receivers roll over
without a coordinated deploy. Headers can be emitted in either the Standard
Webhooks naming or the `huxio-` prefixed naming, which is one config value rather
than a receiver rewrite.

**A delivery log, and the ability to act on it.** Every attempt is recorded with
its status, response code, truncated body, and duration. Resend one delivery,
recover everything an endpoint missed, or replay by filter. Replay only touches
deliveries whose *most recent* attempt failed, so a message that failed once and
then succeeded on retry is not sent twice.

**A portal for your customers.** Server-rendered, embedded in the binary, no
separate frontend to deploy. Your backend mints a short-lived scoped token; the
customer manages their own endpoints, reads their own delivery log, reveals and
rotates their own secret, sends a test event, and watches deliveries arrive on a
live tail. A portal session takes its application from the session token, and no
portal URL contains an application id at all, so there is no request a session
can make that names somebody else's data.

**Protection against your own customers' URLs.** An endpoint URL is user input
pointed at your network. huxio resolves it, checks the address against
loopback, private and link-local ranges, and then dials **that IP by literal**,
so nothing can be re-resolved between the check and the connect. Redirects are
not followed. Response bodies are read through a limit reader and drained.

**Metrics that can prove the isolation claim.** Prometheus histograms broken
down per tenant, because a global p99 hides exactly the failure this system
exists to avoid. Grafana dashboards and alert rules ship in `deploy/`.

## How it works

```
                  POST /msg                        one row, one transaction
  your backend ──────────────► API node ──────────────► Postgres
                                                          │  message + queue row
                                                          │
                              worker ◄───────────────────┘  claim by partition
                                │
                    ┌───────────┴───────────┐
                    ▼           ▼           ▼
                  lane        lane        lane      per endpoint: concurrency,
               (endpoint)  (endpoint)  (endpoint)   rate limit, circuit breaker
                    │           │           │
                    ▼           ▼           ▼
                 customer endpoints (signed POST)
```

The API never makes an outbound HTTP request. Ingest writes the message and its
queue row in a single transaction and returns `202`. That is what keeps ingest
latency independent of how badly anyone's endpoints are behaving, and it removes
the "accepted but never delivered" class of bug entirely, since there is no
second system to fall out of sync with.

Postgres is the queue. The work table is split into 256 fixed partitions;
workers take leases on partitions and claim only from the ones they own, using
`FOR UPDATE SKIP LOCKED`. Because each partition has exactly one owner, two
workers never examine the same rows, and all the per-endpoint state — rate
limiter, breaker, in-flight count — can live in one worker's memory instead of a
shared cache. A retry is a re-enqueue with a future `visible_at`, never a sleep.

Attempt records are written in batches with `COPY`, which is the single largest
throughput win in the design. Configuration is read from an immutable in-memory
snapshot refreshed by `LISTEN/NOTIFY`, so the delivery path never queries the
config tables.

## Deployment

One binary, three roles:

```bash
huxio serve --role=api       # HTTP only
huxio serve --role=worker    # delivery engine and maintenance loops
huxio serve --role=all       # both, good for small installs
```

Scale API nodes for ingest, workers for delivery. Workers discover each other
through Postgres and split partitions by fair share, so adding one rebalances
automatically and removing one hands its partitions over without dropping work.
Every role serves `/metrics` and health, including worker-only processes.

`--pools` lets a worker serve the `default` pool, the `quarantine` pool, or
both, so you can dedicate a small number of processes to endpoints that have
been failing for hours and leave the rest untouched.

Graceful shutdown on `SIGTERM`: stop claiming, release leases so peers take
over immediately, finish requests already on the wire, flush buffered attempt
records, deregister.

## API

Full OpenAPI document: `make openapi`. The shape:

| | |
| --- | --- |
| `POST /api/v1/app/:app_id/msg` | Send an event |
| `GET  /api/v1/app/:app_id/msg` | List messages |
| `POST /api/v1/app/:app_id/endpoint` | Create an endpoint |
| `GET  /api/v1/app/:app_id/endpoint/:id/secret` | Read the signing secret |
| `POST /api/v1/app/:app_id/endpoint/:id/secret/rotate` | Rotate with overlap |
| `GET  /api/v1/app/:app_id/attempt/msg/:msg_id` | Delivery log for a message |
| `GET  /api/v1/app/:app_id/attempt/stream` | Live tail (server-sent events) |
| `POST /api/v1/app/:app_id/msg/:msg_id/endpoint/:id/resend` | Resend one |
| `POST /api/v1/app/:app_id/endpoint/:id/recover` | Recover a window |
| `POST /api/v1/app/:app_id/replay` | Replay by filter |
| `POST /api/v1/app/:app_id/portal-access` | Mint a portal token |
| `GET  /api/v1/admin/queue` | Queue depth and lag per pool |

Listing is cursor-paginated. There is deliberately no total count: a `COUNT(*)`
next to a paginated query is the first thing to get slow.

`POST /msg` accepts an `Idempotency-Key` header. A duplicate replays the stored
response rather than creating a second message.

## Configuration

Environment only. Two variables are required and have no defaults:

| | |
| --- | --- |
| `HUXIO_DATABASE_URL` | Postgres DSN |
| `HUXIO_ENCRYPTION_KEY` | Seals signing secrets at rest. `huxio keygen`. Comma-separated for rotation, newest first |
| `HUXIO_JWT_SECRET` | Signs API tokens |
| `HUXIO_ROLE` | `all`, `api` or `worker` |
| `HUXIO_POOLS` | Worker pools this process serves |
| `HUXIO_COMPAT_HEADERS` | `standard` or `huxio` |
| `HUXIO_WORKER_REQUEST_TIMEOUT` | Per-delivery timeout, default `30s` |
| `HUXIO_ALLOW_SUBNETS` | Extra CIDRs the SSRF guard permits |

See `.env.example` for the full list. The knob count is deliberately small and
the defaults are the ones worth running.

## Performance

Measured on a laptop, single process, local sinks. Reproduce with `make bench`,
which starts its own throwaway Postgres for the purpose.

| | |
| --- | --- |
| Ingest | ~1000 msg/s, p99 4.7ms |
| Delivery | ~2000/s with one worker against a null sink |
| Isolation | healthy-tenant p99 78ms vs 60ms baseline, with a fifth of tenants tarpitted |

The isolation number is the one that matters: a 1.3× tail impact against a 2×
budget, while 20% of traffic points at endpoints that accept the connection and
never answer. Scenarios: `throughput`, `isolation`, `retry-storm`,
`cold-start`, `soak`. CI runs throughput and isolation on every merge to main
and fails the build on a regression beyond 15%.

## Development

```bash
make test              # unit tests, race detector, fast
make test-integration  # integration tests against a real Postgres (needs Docker)
make lint              # golangci-lint
make cover             # coverage, currently ~80%
make help              # everything else
```

283 tests. The integration suite spins up one Postgres container and gives each
test its own database inside it, so tests run in parallel without interfering.

The codebase is hexagonal (ports and adapters), and two import rules are load
bearing: `internal/domain` imports no web framework and no database driver, and
`internal/application/dispatch` imports no pgx types. The consequence is that
the retry policy, the signer and the entire delivery engine are testable with no
Postgres and no network.

```
cmd/huxio/              CLI: serve, migrate, jwt, keygen, org, openapi, bench
internal/domain/        entities, repository ports, signing, retry policy
internal/application/   use cases, delivery engine, config snapshot, maintenance
internal/adapters/       HTTP (Echo) and worker inbound; Postgres and HTTP outbound
internal/infrastructure/ logging, metrics, ids, secrets, payload codec
bench/                  sinks and benchmark scenarios
tests/integrations/     black-box tests
```

Contributions welcome. Run `make fmt vet lint test-all` before opening a PR, and
say *why* in the description; the diff already says what.

## Not built, on purpose

No inbound webhook receiving, no event transformations, no non-HTTP
destinations, no multi-region replication, no billing. Each of those is a
reasonable product and none of them is this one.

## License

[MIT](LICENSE)
