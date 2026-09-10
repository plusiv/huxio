package postgres

import (
	"context"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/rotisserie/eris"
)

// LeaseRepo is the pgx implementation of repositories.LeaseRepository. The
// lease table is deliberately debuggable with a plain SELECT, which is a large
// part of why fixed partitions beat a consistent-hashing ring.
type LeaseRepo struct {
	store      *Store
	partitions int
}

// NewLeaseRepo builds the repository for a fixed partition count.
func NewLeaseRepo(store *Store, partitions int) *LeaseRepo {
	return &LeaseRepo{store: store, partitions: partitions}
}

// RegisterWorker upserts this process's registry row and heartbeat.
func (r *LeaseRepo) RegisterWorker(ctx context.Context, worker entities.Worker) error {
	const query = `
		INSERT INTO worker_registry (id, pools, version, heartbeat_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (id) DO UPDATE
		SET pools = EXCLUDED.pools, version = EXCLUDED.version, heartbeat_at = now(), deleted_at = NULL`
	if _, err := r.store.Querier(ctx).Exec(ctx, query, worker.ID, worker.Pools, worker.Version); err != nil {
		return eris.Wrap(err, "register worker")
	}
	return nil
}

// DeregisterWorker removes this process's registry row on shutdown.
func (r *LeaseRepo) DeregisterWorker(ctx context.Context, id string) error {
	if _, err := r.store.Querier(ctx).Exec(ctx, `DELETE FROM worker_registry WHERE id = $1`, id); err != nil {
		return eris.Wrap(err, "deregister worker")
	}
	return nil
}

// CountLiveWorkers counts workers serving a pool with a fresh heartbeat. It is
// the denominator of ceil(partitions / active_workers).
func (r *LeaseRepo) CountLiveWorkers(ctx context.Context, pool string, ttl time.Duration) (int, error) {
	const query = `
		SELECT count(*)
		FROM   worker_registry
		WHERE  $1 = ANY(pools)
		  AND  heartbeat_at > now() - ($2 * interval '1 second')
		  AND  deleted_at IS NULL`
	var count int
	if err := r.store.Querier(ctx).QueryRow(ctx, query, pool, ttl.Seconds()).Scan(&count); err != nil {
		return 0, eris.Wrap(err, "count live workers")
	}
	return count, nil
}

// ReapDeadWorkers deletes registry rows whose heartbeat has expired.
func (r *LeaseRepo) ReapDeadWorkers(ctx context.Context, ttl time.Duration) (int64, error) {
	const query = `DELETE FROM worker_registry WHERE heartbeat_at < now() - ($1 * interval '1 second')`
	tag, err := r.store.Querier(ctx).Exec(ctx, query, ttl.Seconds())
	if err != nil {
		return 0, eris.Wrap(err, "reap dead workers")
	}
	return tag.RowsAffected(), nil
}

// ClaimPartitions takes ownership of up to limit free or expired leases,
// preferring the partitions this worker already holds so ownership stays
// stable across ticks and rebalances stay rare.
func (r *LeaseRepo) ClaimPartitions(
	ctx context.Context,
	pool, ownerID string,
	ttl time.Duration,
	limit int,
) ([]int16, error) {
	if limit <= 0 {
		return nil, nil
	}
	if err := r.ensurePartitionRows(ctx, pool); err != nil {
		return nil, err
	}

	const query = `
		WITH claimable AS (
		    SELECT partition_key
		    FROM   partition_lease
		    WHERE  pool = $1
		      AND  deleted_at IS NULL
		      AND (
		            owner_id = $2
		         OR owner_id IS NULL
		         OR owner_id = ''
		         OR heartbeat_at IS NULL
		         OR heartbeat_at < now() - ($3 * interval '1 second')
		      )
		    ORDER BY (owner_id = $2) DESC, partition_key
		    LIMIT $4
		    FOR UPDATE SKIP LOCKED
		)
		UPDATE partition_lease l
		SET    owner_id = $2, heartbeat_at = now()
		FROM   claimable c
		WHERE  l.pool = $1 AND l.partition_key = c.partition_key
		RETURNING l.partition_key`

	rows, err := r.store.Querier(ctx).Query(ctx, query, pool, ownerID, ttl.Seconds(), limit)
	if err != nil {
		return nil, eris.Wrap(err, "claim partitions")
	}
	defer rows.Close()

	return scanPartitionKeys(rows)
}

// HeartbeatPartitions refreshes ownership and returns the subset still owned,
// so a worker that lost a lease mid-flight learns about it.
func (r *LeaseRepo) HeartbeatPartitions(ctx context.Context, pool, ownerID string, partitions []int16) ([]int16, error) {
	if len(partitions) == 0 {
		return nil, nil
	}
	const query = `
		UPDATE partition_lease
		SET    heartbeat_at = now()
		WHERE  pool = $1 AND owner_id = $2 AND partition_key = ANY($3::smallint[])
		RETURNING partition_key`

	rows, err := r.store.Querier(ctx).Query(ctx, query, pool, ownerID, partitions)
	if err != nil {
		return nil, eris.Wrap(err, "heartbeat partitions")
	}
	defer rows.Close()

	return scanPartitionKeys(rows)
}

// ReleasePartitions gives up ownership immediately. A nil list releases every
// lease held in the pool.
func (r *LeaseRepo) ReleasePartitions(ctx context.Context, pool, ownerID string, partitions []int16) error {
	const query = `
		UPDATE partition_lease
		SET    owner_id = NULL, heartbeat_at = NULL
		WHERE  pool = $1 AND owner_id = $2
		  AND  ($3::smallint[] IS NULL OR partition_key = ANY($3::smallint[]))`
	var arg any
	if len(partitions) > 0 {
		arg = partitions
	}
	if _, err := r.store.Querier(ctx).Exec(ctx, query, pool, ownerID, arg); err != nil {
		return eris.Wrap(err, "release partitions")
	}
	return nil
}

// ListPartitionLeases returns the lease table for the admin endpoint.
func (r *LeaseRepo) ListPartitionLeases(ctx context.Context, pool string) ([]entities.PartitionLease, error) {
	const query = `
		SELECT partition_key, pool, owner_id, heartbeat_at, created_at
		FROM   partition_lease
		WHERE  ($1::text = '' OR pool = $1)
		  AND  deleted_at IS NULL
		ORDER  BY pool, partition_key`

	rows, err := r.store.Querier(ctx).Query(ctx, query, pool)
	if err != nil {
		return nil, eris.Wrap(err, "list partition leases")
	}
	defer rows.Close()

	leases := make([]entities.PartitionLease, 0, r.partitions)
	for rows.Next() {
		var lease entities.PartitionLease
		if err := rows.Scan(&lease.PartitionKey, &lease.Pool, &lease.OwnerID, &lease.HeartbeatAt, &lease.CreatedAt); err != nil {
			return nil, eris.Wrap(err, "scan partition lease")
		}
		leases = append(leases, lease)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "iterate partition leases")
	}
	return leases, nil
}

// AcquireNamedLease takes or renews a singleton lock, reporting false when
// another live owner holds it.
func (r *LeaseRepo) AcquireNamedLease(ctx context.Context, name, ownerID string, ttl time.Duration) (bool, error) {
	const query = `
		INSERT INTO named_lease (name, owner_id, heartbeat_at)
		VALUES ($1, $2, now())
		ON CONFLICT (name) DO UPDATE
		SET owner_id = $2, heartbeat_at = now(), deleted_at = NULL
		WHERE named_lease.owner_id = $2
		   OR named_lease.heartbeat_at < now() - ($3 * interval '1 second')`
	tag, err := r.store.Querier(ctx).Exec(ctx, query, name, ownerID, ttl.Seconds())
	if err != nil {
		return false, eris.Wrap(err, "acquire named lease")
	}
	return tag.RowsAffected() > 0, nil
}

// ReleaseNamedLease gives up a singleton lock.
func (r *LeaseRepo) ReleaseNamedLease(ctx context.Context, name, ownerID string) error {
	const query = `DELETE FROM named_lease WHERE name = $1 AND owner_id = $2`
	if _, err := r.store.Querier(ctx).Exec(ctx, query, name, ownerID); err != nil {
		return eris.Wrap(err, "release named lease")
	}
	return nil
}

// ensurePartitionRows materialises the fixed partition set for a pool. It is
// idempotent, so any worker can call it on every claim tick.
func (r *LeaseRepo) ensurePartitionRows(ctx context.Context, pool string) error {
	const query = `
		INSERT INTO partition_lease (partition_key, pool)
		SELECT generate_series(0, $1::int - 1)::smallint, $2
		ON CONFLICT (partition_key, pool) DO NOTHING`
	if _, err := r.store.Querier(ctx).Exec(ctx, query, r.partitions, pool); err != nil {
		return eris.Wrap(err, "ensure partition lease rows")
	}
	return nil
}

func scanPartitionKeys(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) ([]int16, error) {
	keys := make([]int16, 0, 32)
	for rows.Next() {
		var key int16
		if err := rows.Scan(&key); err != nil {
			return nil, eris.Wrap(err, "scan partition key")
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "iterate partition keys")
	}
	return keys, nil
}
