// Package integration_test exercises the adapters against a real PostgreSQL
// instance. One container is started for the whole suite and each test gets
// its own database inside it, which keeps isolation without paying for a
// container per test.
package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/plusiv/huxio/internal/adapters/outbound/persistence/postgres"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/plusiv/huxio/migrations"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

var (
	sharedContainer *tcpostgres.PostgresContainer
	adminDSN        string
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	logger.Init("error")

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("huxio"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Printf("start postgres container: %v\n", err)
		os.Exit(1)
	}
	sharedContainer = container

	adminDSN, err = container.ConnectionString(ctx, "sslmode=disable", "dbname=postgres")
	if err != nil {
		fmt.Printf("build admin dsn: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()
	_ = container.Terminate(ctx)
	os.Exit(code)
}

// testEnv is one isolated database with every repository wired to it.
type testEnv struct {
	DSN   string
	Pool  *pgxpool.Pool
	Store *postgres.Store

	Organizations *postgres.OrganizationRepo
	Applications  *postgres.ApplicationRepo
	EventTypes    *postgres.EventTypeRepo
	Endpoints     *postgres.EndpointRepo
	Messages      *postgres.MessageRepo
	Attempts      *postgres.AttemptRepo
	Queue         *postgres.QueueRepo
	Leases        *postgres.LeaseRepo
	Idempotency   *postgres.IdempotencyRepo
	Maintenance   *postgres.MaintenanceRepo
	Snapshots     *postgres.SnapshotRepo
}

// newTestEnv creates a migrated database for one test and tears it down after.
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()
	dbName := "test_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]

	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE %q`, dbName)); err != nil {
		_ = admin.Close()
		t.Fatalf("create test database: %v", err)
	}
	_ = admin.Close()

	dsn, err := sharedContainer.ConnectionString(ctx, "sslmode=disable", "dbname="+dbName)
	if err != nil {
		t.Fatalf("build test dsn: %v", err)
	}

	migrationDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open migration connection: %v", err)
	}
	if err := migrations.Up(ctx, migrationDB); err != nil {
		_ = migrationDB.Close()
		t.Fatalf("run migrations: %v", err)
	}
	_ = migrationDB.Close()

	pool, err := postgres.NewPool(ctx, postgres.PoolConfig{DSN: dsn, MaxConns: 8, MinConns: 1})
	if err != nil {
		t.Fatalf("open pgx pool: %v", err)
	}

	store := postgres.NewStore(pool)
	env := &testEnv{
		DSN:           dsn,
		Pool:          pool,
		Store:         store,
		Organizations: postgres.NewOrganizationRepo(store),
		Applications:  postgres.NewApplicationRepo(store),
		EventTypes:    postgres.NewEventTypeRepo(store),
		Endpoints:     postgres.NewEndpointRepo(store),
		Messages:      postgres.NewMessageRepo(store),
		Attempts:      postgres.NewAttemptRepo(store),
		Queue:         postgres.NewQueueRepo(store),
		Leases:        postgres.NewLeaseRepo(store, 256),
		Idempotency:   postgres.NewIdempotencyRepo(store),
		Maintenance:   postgres.NewMaintenanceRepo(store),
		Snapshots:     postgres.NewSnapshotRepo(store),
	}

	t.Cleanup(func() {
		pool.Close()

		dropCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		dropAdmin, err := sql.Open("pgx", adminDSN)
		if err != nil {
			return
		}
		defer dropAdmin.Close()
		_, _ = dropAdmin.ExecContext(dropCtx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, dbName))
	})

	return env
}

// waitFor polls cond until it holds or the timeout elapses.
func waitFor(t *testing.T, cond func() bool, timeout time.Duration, message string) {
	t.Helper()

	deadline := time.After(timeout)
	for !cond() {
		select {
		case <-deadline:
			t.Fatal(message)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
