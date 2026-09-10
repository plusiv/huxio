-- +goose Up
-- +goose StatementBegin

-- ─────────────────────────────────────────────────────────────────────────────
-- Configuration tables: small, read constantly, cached in memory by every
-- process rather than queried on the delivery path.
-- Small, rarely written, loaded into process memory in full. Nothing on the
-- delivery path queries these.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE organization (
    id          text        PRIMARY KEY,
    name        text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    deleted_at  timestamptz
);

COMMENT ON TABLE organization IS
  'A tenant of the service. Every other row belongs to exactly one organization; it is the boundary for authentication, quotas and metrics.';

CREATE TABLE application (
    id          text        PRIMARY KEY,
    org_id      text        NOT NULL REFERENCES organization(id),
    uid         text,
    name        text        NOT NULL,
    rate_limit  integer,
    metadata    jsonb       NOT NULL DEFAULT '{}',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    deleted_at  timestamptz
);

CREATE UNIQUE INDEX application_uid_idx
    ON application (org_id, uid) WHERE deleted_at IS NULL;
CREATE INDEX application_org_idx
    ON application (org_id, id) WHERE deleted_at IS NULL;

COMMENT ON TABLE application IS
  'One of a tenant''s own customers. Endpoints hang off an application, so "every destination for this customer" is one lookup.';
COMMENT ON COLUMN application.uid IS
  'Tenant-assigned identifier. Accepted in place of id in API paths so tenants can address applications by their own key.';
COMMENT ON COLUMN application.rate_limit IS
  'Optional cap on messages accepted per second for this application. Null means the organization default.';

CREATE TABLE event_type (
    org_id      text        NOT NULL REFERENCES organization(id),
    name        text        NOT NULL,
    description text        NOT NULL DEFAULT '',
    schemas     jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    deleted_at  timestamptz,
    PRIMARY KEY (org_id, name)
);

COMMENT ON TABLE event_type IS
  'A named kind of event a tenant can send, such as invoice.paid. Endpoints subscribe by name, and the optional JSON schema drives sample payloads and generated receiver docs.';
COMMENT ON COLUMN event_type.deleted_at IS
  'Set when a tenant retires an event type. The row stays so historical messages referencing it still render.';

CREATE TABLE endpoint (
    id                text        PRIMARY KEY,
    app_id            text        NOT NULL REFERENCES application(id),
    org_id            text        NOT NULL,
    uid               text,
    url               text        NOT NULL,
    description       text        NOT NULL DEFAULT '',
    version           integer     NOT NULL DEFAULT 1,
    rate_limit        integer,
    event_types       text[],
    channels          text[],
    headers           jsonb,
    secret_enc        bytea       NOT NULL,
    secret_type       text        NOT NULL DEFAULT 'hmac256',
    old_secrets_enc   jsonb,
    pool              text        NOT NULL DEFAULT 'default',
    first_failure_at  timestamptz,
    disabled_at       timestamptz,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    deleted_at        timestamptz
);

CREATE INDEX endpoint_app_idx
    ON endpoint (app_id) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX endpoint_uid_idx
    ON endpoint (app_id, uid) WHERE deleted_at IS NULL;
CREATE INDEX endpoint_org_idx
    ON endpoint (org_id, id) WHERE deleted_at IS NULL;

COMMENT ON TABLE endpoint IS
  'A destination URL that receives signed HTTP POSTs. Owns its signing secret, its subscription filters, and its health state.';
COMMENT ON COLUMN endpoint.event_types IS
  'Event type names this endpoint wants. Null means all of them. Matched in memory against the config snapshot, never with a database query.';
COMMENT ON COLUMN endpoint.channels IS
  'Optional fan-out filter. A message is delivered here only if it carries at least one matching channel. Null means no channel filtering.';
COMMENT ON COLUMN endpoint.secret_enc IS
  'Signing secret, encrypted at rest. Decrypted once when the config snapshot is built, never per delivery.';
COMMENT ON COLUMN endpoint.old_secrets_enc IS
  'Previous secrets kept during a rotation window, as [{secret, expires_at}]. Every unexpired key produces a signature so receivers can roll over without downtime.';
COMMENT ON COLUMN endpoint.pool IS
  'Which worker pool handles this endpoint. Moving a failing endpoint to the quarantine pool stops it consuming healthy capacity.';
COMMENT ON COLUMN endpoint.first_failure_at IS
  'Start of the current unbroken run of failures. Cleared on any success. Drives auto-disable after the configured failure window.';
COMMENT ON COLUMN endpoint.disabled_at IS
  'Set when the endpoint was switched off, automatically after sustained failure or manually by its owner. Distinct from deleted_at: a disabled endpoint can be re-enabled.';

-- ─────────────────────────────────────────────────────────────────────────────
-- Message tables: time-partitioned by day. Retention detaches and drops whole
-- partitions rather than deleting rows.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE message (
    id            text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    org_id        text        NOT NULL,
    app_id        text        NOT NULL,
    event_type    text        NOT NULL,
    uid           text,
    channels      text[],
    payload       bytea       NOT NULL,
    payload_size  integer     NOT NULL,
    expires_at    timestamptz NOT NULL,
    deleted_at    timestamptz,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX message_app_idx
    ON message (app_id, created_at DESC) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX message_uid_idx
    ON message (app_id, uid, created_at)
    WHERE uid IS NOT NULL AND deleted_at IS NULL;

COMMENT ON TABLE message IS
  'One event a tenant asked us to deliver, with its raw payload. Written once, read a handful of times, then aged out by dropping the whole day partition.';
COMMENT ON COLUMN message.payload IS
  'The tenant''s JSON, zstd-compressed, stored as opaque bytes. Never parsed into a map; validated once at ingest and written to the socket as-is.';
COMMENT ON COLUMN message.payload_size IS
  'Uncompressed size in bytes, kept for metrics and quota reporting since the stored bytes are compressed.';
COMMENT ON COLUMN message.expires_at IS
  'When this message becomes eligible for removal. Determines which retention partition it belongs to.';

CREATE TABLE delivery_attempt (
    id                   text        NOT NULL,
    created_at           timestamptz NOT NULL DEFAULT now(),
    org_id               text        NOT NULL,
    app_id               text        NOT NULL,
    msg_id               text        NOT NULL,
    endpoint_id          text        NOT NULL,
    url                  text        NOT NULL,
    status               smallint    NOT NULL,
    response_status_code smallint    NOT NULL,
    response_body        text        NOT NULL,
    response_duration_ms integer     NOT NULL,
    attempt_number       smallint    NOT NULL,
    trigger_type         smallint    NOT NULL,
    next_attempt_at      timestamptz,
    deleted_at           timestamptz,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE INDEX attempt_msg_idx
    ON delivery_attempt (msg_id, created_at DESC);
CREATE INDEX attempt_endpoint_idx
    ON delivery_attempt (endpoint_id, created_at DESC);
CREATE INDEX attempt_failed_idx
    ON delivery_attempt (endpoint_id, created_at DESC) WHERE status = 2;
CREATE INDEX attempt_app_idx
    ON delivery_attempt (app_id, created_at DESC);

COMMENT ON TABLE delivery_attempt IS
  'One HTTP request we made to one endpoint for one message, and what came back. Append-only, highest-volume table in the system, written in batches.';
COMMENT ON COLUMN delivery_attempt.status IS
  '0 succeeded, 1 pending retry, 2 failed permanently.';
COMMENT ON COLUMN delivery_attempt.response_status_code IS
  'HTTP status returned. Zero means we never got a response: DNS failure, refused connection, timeout.';
COMMENT ON COLUMN delivery_attempt.response_body IS
  'First few kilobytes of the response, truncated. Shown in the portal so endpoint owners can debug their own failures.';
COMMENT ON COLUMN delivery_attempt.trigger_type IS
  '0 scheduled, 1 manual resend, 2 bulk replay. Manual and bulk attempts are not retried on failure.';

-- ─────────────────────────────────────────────────────────────────────────────
-- Queue table. Postgres is the queue: one fewer moving part than a broker,
-- and it lets a message and its queue row commit in the same transaction.
-- Rows are physically deleted as work completes, so the table drains rather
-- than accumulating.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE delivery_task (
    id             bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    partition_key  smallint    NOT NULL,
    pool           text        NOT NULL DEFAULT 'default',
    kind           smallint    NOT NULL,
    org_id         text        NOT NULL,
    app_id         text        NOT NULL,
    msg_id         text        NOT NULL,
    msg_created_at timestamptz NOT NULL,
    endpoint_id    text,
    attempt        smallint    NOT NULL DEFAULT 0,
    trigger_type   smallint    NOT NULL DEFAULT 0,
    visible_at     timestamptz NOT NULL DEFAULT now(),
    locked_by      text,
    locked_until   timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now(),
    deleted_at     timestamptz
) WITH (fillfactor = 70);

CREATE INDEX task_claim_idx
    ON delivery_task (pool, partition_key, visible_at, id)
    WHERE locked_until IS NULL AND deleted_at IS NULL;

CREATE INDEX task_stuck_idx
    ON delivery_task (locked_until)
    WHERE locked_until IS NOT NULL;

-- Bloat is how a Postgres-backed queue dies: every claim UPDATEs a row, and
-- default autovacuum thresholds scale with table size, so a hot queue table
-- never reaches them. These settings are not optional.
ALTER TABLE delivery_task SET (
    autovacuum_vacuum_scale_factor  = 0.01,
    autovacuum_vacuum_cost_delay    = 0,
    autovacuum_analyze_scale_factor = 0.02
);

COMMENT ON TABLE delivery_task IS
  'The work queue. One row per unit of pending work. Unlike every other table here, rows are physically deleted as work completes, because the queue must drain rather than accumulate.';
COMMENT ON COLUMN delivery_task.partition_key IS
  'crc32 of the routing id modulo 256. Decides which worker handles this task, which is what lets rate limits and circuit breakers live in worker memory.';
COMMENT ON COLUMN delivery_task.kind IS
  '0 fanout (expand one message into one task per matching endpoint), 1 deliver (send to one endpoint).';
COMMENT ON COLUMN delivery_task.visible_at IS
  'Earliest time this task may be claimed. Retries set it into the future; this is how backoff works without any process sleeping.';
COMMENT ON COLUMN delivery_task.locked_until IS
  'While set, another worker holds this task. If the holder dies, the lock expires and the maintenance loop returns the task to the queue.';
COMMENT ON COLUMN delivery_task.deleted_at IS
  'Present for schema uniformity only. Completed tasks are removed with DELETE so the table stays small; a soft-deleted queue would defeat the point.';

-- ─────────────────────────────────────────────────────────────────────────────
-- Coordination tables. Workers agree on who owns what through Postgres, so
-- there is no ZooKeeper or etcd to run alongside it.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE partition_lease (
    partition_key smallint    NOT NULL,
    pool          text        NOT NULL,
    owner_id      text,
    heartbeat_at  timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now(),
    deleted_at    timestamptz,
    PRIMARY KEY (partition_key, pool)
);

CREATE INDEX partition_lease_owner_idx ON partition_lease (pool, owner_id);

COMMENT ON TABLE partition_lease IS
  'Which worker currently owns which queue partition, per pool. Ownership is what allows per-endpoint state to live in one worker''s memory instead of a shared cache.';
COMMENT ON COLUMN partition_lease.heartbeat_at IS
  'Last time the owner said it was alive. A lease older than the TTL is free for anyone to claim.';

CREATE TABLE named_lease (
    name          text        PRIMARY KEY,
    owner_id      text        NOT NULL,
    heartbeat_at  timestamptz NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    deleted_at    timestamptz
);

COMMENT ON TABLE named_lease IS
  'Singleton locks for jobs that must run on exactly one process across the whole cluster, such as creating and dropping time partitions.';

CREATE TABLE worker_registry (
    id            text        PRIMARY KEY,
    pools         text[]      NOT NULL,
    version       text        NOT NULL,
    heartbeat_at  timestamptz NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    deleted_at    timestamptz
);

CREATE INDEX worker_registry_heartbeat_idx ON worker_registry (heartbeat_at);

COMMENT ON TABLE worker_registry IS
  'Currently live workers and the pools they serve, so each can work out its fair share of partitions without any external coordination service.';

-- ─────────────────────────────────────────────────────────────────────────────
-- Idempotency: a retried POST /msg must not create a second message.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE idempotency_key (
    key          text        PRIMARY KEY,
    status       smallint    NOT NULL,
    response     bytea,
    status_code  smallint,
    expires_at   timestamptz NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    deleted_at   timestamptz
);

CREATE INDEX idempotency_expiry_idx ON idempotency_key (expires_at);

COMMENT ON TABLE idempotency_key IS
  'Cached responses for POSTs carrying an Idempotency-Key, so a client that retries after a timeout gets the original answer instead of creating a second message.';
COMMENT ON COLUMN idempotency_key.key IS
  'blake2b of organization, method, path and the client-supplied key. Hashed so a client key can never collide across tenants.';
COMMENT ON COLUMN idempotency_key.status IS
  '0 in progress, 1 complete. An in-progress row is a short-lived lock that makes two concurrent duplicates safe.';

-- +goose StatementEnd

-- +goose StatementBegin
-- Bootstrap the daily partitions a fresh database needs before the maintenance
-- loop takes over: yesterday through seven days ahead.
DO $$
DECLARE
    day  date;
    tbl  text;
BEGIN
    FOREACH tbl IN ARRAY ARRAY['message', 'delivery_attempt'] LOOP
        FOR day IN
            SELECT generate_series(
                (now() AT TIME ZONE 'UTC')::date - 1,
                (now() AT TIME ZONE 'UTC')::date + 7,
                interval '1 day'
            )::date
        LOOP
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                tbl || '_' || to_char(day, 'YYYYMMDD'),
                tbl,
                day::timestamptz,
                (day + 1)::timestamptz
            );
        END LOOP;
    END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS idempotency_key;
DROP TABLE IF EXISTS worker_registry;
DROP TABLE IF EXISTS named_lease;
DROP TABLE IF EXISTS partition_lease;
DROP TABLE IF EXISTS delivery_task;
DROP TABLE IF EXISTS delivery_attempt;
DROP TABLE IF EXISTS message;
DROP TABLE IF EXISTS endpoint;
DROP TABLE IF EXISTS event_type;
DROP TABLE IF EXISTS application;
DROP TABLE IF EXISTS organization;
-- +goose StatementEnd
