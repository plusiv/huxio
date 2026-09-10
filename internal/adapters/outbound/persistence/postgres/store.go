// Package postgres implements the repository ports on top of pgx/v5. pgx is
// used directly, without database/sql and without an ORM, because CopyFrom and
// SendBatch are the two APIs the delivery design depends on and neither
// survives the database/sql abstraction.
package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/rotisserie/eris"
)

// Querier is the subset of pgx shared by a pool and a transaction, so every
// repository method works identically inside and outside a unit of work.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	CopyFrom(ctx context.Context, table pgx.Identifier, columns []string, src pgx.CopyFromSource) (int64, error)
	SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults
}

// txKey is the context key under which an open transaction is carried.
type txKey struct{}

// Store owns the connection pool and hands out the right Querier for the
// current context.
type Store struct {
	pool *pgxpool.Pool
}

// PoolConfig carries the role-sized connection budget. Workers block on
// customer HTTP rather than on Postgres, so their pools stay tiny.
type PoolConfig struct {
	DSN                string
	MaxConns           int32
	MinConns           int32
	MaxConnLifetime    time.Duration
	MaxConnIdleTime    time.Duration
	HealthCheckPeriod  time.Duration
	SlowQueryThreshold time.Duration
}

// NewPool opens and verifies a pgx connection pool.
func NewPool(ctx context.Context, cfg PoolConfig) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, eris.Wrap(err, "parse database dsn")
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = cfg.MinConns
	}
	poolCfg.MaxConnLifetime = orDuration(cfg.MaxConnLifetime, time.Hour)
	poolCfg.MaxConnIdleTime = orDuration(cfg.MaxConnIdleTime, 30*time.Minute)
	poolCfg.HealthCheckPeriod = orDuration(cfg.HealthCheckPeriod, time.Minute)
	poolCfg.ConnConfig.Tracer = newQueryTracer(orDuration(cfg.SlowQueryThreshold, 200*time.Millisecond))

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, eris.Wrap(err, "create connection pool")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, eris.Wrap(err, "ping database")
	}
	return pool, nil
}

// NewStore wraps a pool.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Pool exposes the underlying pool for components that need connection-level
// access, such as the LISTEN/NOTIFY listener.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Querier returns the transaction carried by ctx, or the pool when there is
// none.
func (s *Store) Querier(ctx context.Context) Querier {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok && tx != nil {
		return tx
	}
	return s.pool
}

// WithinTransaction runs fn inside one transaction. Nested calls join the
// outer transaction rather than opening a second one.
func (s *Store) WithinTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok && tx != nil {
		return fn(ctx)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return eris.Wrap(err, "begin transaction")
	}

	txCtx := context.WithValue(ctx, txKey{}, tx)
	if err := fn(txCtx); err != nil {
		// Roll back on a fresh context: ctx may already be cancelled, and a
		// rollback that never reaches the server leaks the connection.
		rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if rbErr := tx.Rollback(rollbackCtx); rbErr != nil && !eris.Is(rbErr, pgx.ErrTxClosed) {
			logger.FromContext(ctx).Error().Err(rbErr).Msg("rollback failed")
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return eris.Wrap(err, "commit transaction")
	}
	return nil
}

// Healthy reports whether the pool can serve queries.
func (s *Store) Healthy(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return eris.Wrap(err, "database unhealthy")
	}
	return nil
}

func orDuration(v, fallback time.Duration) time.Duration {
	if v <= 0 {
		return fallback
	}
	return v
}
