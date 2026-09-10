package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/adapters/outbound/persistence/postgres"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
)

func TestMaintenanceEnsuresAndDropsDailyPartitions(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	for _, table := range []string{postgres.TableMessage, postgres.TableDeliveryAttempt} {
		t.Run(table, func(t *testing.T) {
			created, err := env.Maintenance.EnsureDailyPartitions(ctx, table, 7)
			if err != nil {
				t.Fatalf("EnsureDailyPartitions: %v", err)
			}
			if len(created) != 8 {
				t.Fatalf("ensured %d partitions, want today plus seven days", len(created))
			}
			// Idempotent: running twice must not fail.
			if _, err := env.Maintenance.EnsureDailyPartitions(ctx, table, 7); err != nil {
				t.Fatalf("second EnsureDailyPartitions: %v", err)
			}

			existing, err := env.Maintenance.ListPartitions(ctx, table)
			if err != nil {
				t.Fatalf("ListPartitions: %v", err)
			}
			if len(existing) < 8 {
				t.Fatalf("ListPartitions returned %d partitions", len(existing))
			}
			for _, spec := range existing {
				if spec.Day.IsZero() || spec.Name == "" {
					t.Errorf("partition spec incomplete: %+v", spec)
				}
			}

			// Retention drops whole partitions rather than deleting rows.
			cutoff := time.Now().UTC().AddDate(0, 0, 1)
			dropped, err := env.Maintenance.DropPartitionsBefore(ctx, table, cutoff)
			if err != nil {
				t.Fatalf("DropPartitionsBefore: %v", err)
			}
			if len(dropped) == 0 {
				t.Fatal("expected at least yesterday and today to be dropped")
			}
			for _, spec := range dropped {
				if !spec.Day.Before(cutoff.Truncate(24 * time.Hour)) {
					t.Errorf("dropped partition %s is not older than the cutoff", spec.Name)
				}
			}

			after, err := env.Maintenance.ListPartitions(ctx, table)
			if err != nil {
				t.Fatalf("ListPartitions after drop: %v", err)
			}
			if len(after) != len(existing)-len(dropped) {
				t.Errorf("partition count = %d, want %d", len(after), len(existing)-len(dropped))
			}
		})
	}
}

func TestMaintenanceRejectsUnknownTables(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	for _, table := range []string{"delivery_task", "organization", `message"; DROP TABLE message; --`} {
		if _, err := env.Maintenance.EnsureDailyPartitions(ctx, table, 1); err == nil {
			t.Errorf("EnsureDailyPartitions(%q) must be rejected", table)
		}
		if _, err := env.Maintenance.DropPartitionsBefore(ctx, table, time.Now()); err == nil {
			t.Errorf("DropPartitionsBefore(%q) must be rejected", table)
		}
		if _, err := env.Maintenance.ListPartitions(ctx, table); err == nil {
			t.Errorf("ListPartitions(%q) must be rejected", table)
		}
	}
}

func TestMaintenanceReportsHealthAndBloat(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	if err := env.Maintenance.Healthy(ctx); err != nil {
		t.Fatalf("Healthy: %v", err)
	}

	org := env.seedOrg(t, "Bloat")
	app := env.seedApp(t, org.ID, "customer", nil)
	ep := env.seedEndpoint(t, app, "https://example.test/hook")
	msg := env.seedMessage(t, app, "invoice.paid")

	// Churn the queue so the statistics collector has something to report.
	for range 20 {
		env.enqueueDeliver(t, msg, ep, time.Now().UTC())
	}
	if _, err := env.Pool.Exec(ctx, `DELETE FROM delivery_task`); err != nil {
		t.Fatalf("drain queue: %v", err)
	}
	if _, err := env.Pool.Exec(ctx, `ANALYZE delivery_task`); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	ratio, err := env.Maintenance.DeadTupleRatio(ctx, "delivery_task")
	if err != nil {
		t.Fatalf("DeadTupleRatio: %v", err)
	}
	if ratio < 0 || ratio > 1 {
		t.Errorf("DeadTupleRatio = %v, want a fraction", ratio)
	}
}

func TestQueueTableIsNotPartitioned(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	// The queue drains rather than accumulates, so it is a plain table with an
	// aggressive autovacuum configuration instead of a partitioned one.
	var relkind string
	if err := env.Pool.QueryRow(ctx,
		`SELECT relkind FROM pg_class WHERE relname = 'delivery_task'`).Scan(&relkind); err != nil {
		t.Fatalf("read relkind: %v", err)
	}
	if relkind != "r" {
		t.Errorf("delivery_task relkind = %q, want an ordinary table", relkind)
	}

	var options []string
	if err := env.Pool.QueryRow(ctx,
		`SELECT reloptions FROM pg_class WHERE relname = 'delivery_task'`).Scan(&options); err != nil {
		t.Fatalf("read reloptions: %v", err)
	}
	wanted := map[string]bool{
		"fillfactor=70":                       false,
		"autovacuum_vacuum_scale_factor=0.01": false,
		"autovacuum_vacuum_cost_delay=0":      false,
	}
	for _, opt := range options {
		if _, ok := wanted[opt]; ok {
			wanted[opt] = true
		}
	}
	for opt, found := range wanted {
		if !found {
			t.Errorf("delivery_task is missing %q; bloat is the main failure mode of a Postgres queue", opt)
		}
	}
}

func TestPartitionKeyRoutingMatchesStoredRows(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	ctx := context.Background()

	org := env.seedOrg(t, "Routing")
	app := env.seedApp(t, org.ID, "customer", nil)
	ep := env.seedEndpoint(t, app, "https://example.test/hook")
	msg := env.seedMessage(t, app, "invoice.paid")

	// Fan-out routes on application id; delivery routes on endpoint id.
	err := env.Queue.Enqueue(ctx, []repositories.EnqueueTask{{
		PartitionKey: entities.PartitionKeyFor(app.ID),
		Pool:         "default",
		Kind:         entities.TaskFanout,
		OrgID:        org.ID,
		AppID:        app.ID,
		MsgID:        msg.ID,
		MsgCreatedAt: msg.CreatedAt,
	}})
	if err != nil {
		t.Fatalf("Enqueue(fanout): %v", err)
	}
	env.enqueueDeliver(t, msg, ep, time.Now().UTC())

	rows, err := env.Pool.Query(ctx, `SELECT kind, partition_key FROM delivery_task ORDER BY kind`)
	if err != nil {
		t.Fatalf("select tasks: %v", err)
	}
	defer rows.Close()

	found := map[entities.TaskKind]int16{}
	for rows.Next() {
		var (
			kind int16
			key  int16
		)
		if err := rows.Scan(&kind, &key); err != nil {
			t.Fatalf("scan: %v", err)
		}
		found[entities.TaskKind(kind)] = key
	}
	if got, want := found[entities.TaskFanout], entities.PartitionKeyFor(app.ID); got != want {
		t.Errorf("fanout partition = %d, want crc32(app_id) %% 256 = %d", got, want)
	}
	if got, want := found[entities.TaskDeliver], entities.PartitionKeyFor(ep.ID); got != want {
		t.Errorf("deliver partition = %d, want crc32(endpoint_id) %% 256 = %d", got, want)
	}
}
