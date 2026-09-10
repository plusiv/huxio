package postgres

import (
	"context"
	"time"

	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/rotisserie/eris"
)

// TableMessage and TableDeliveryAttempt re-export the port's names so callers
// already holding the adapter do not need both imports.
const (
	TableMessage         = repositories.TableMessage
	TableDeliveryAttempt = repositories.TableDeliveryAttempt
)

// partitionedTables is the allowlist every table name is checked against
// before being interpolated into DDL.
var partitionedTables = map[string]struct{}{
	TableMessage:         {},
	TableDeliveryAttempt: {},
}

// MaintenanceRepo implements repositories.MaintenanceRepository. Partition
// lifecycle lives here rather than in pg_partman: roughly eighty lines of SQL
// is worth less than the one-binary install story.
type MaintenanceRepo struct {
	store *Store
}

// NewMaintenanceRepo builds the repository.
func NewMaintenanceRepo(store *Store) *MaintenanceRepo { return &MaintenanceRepo{store: store} }

// EnsureDailyPartitions creates the daily partitions for [today, today+ahead]
// and reports every partition it created.
func (r *MaintenanceRepo) EnsureDailyPartitions(
	ctx context.Context,
	table string,
	ahead int,
) ([]repositories.PartitionSpec, error) {
	if err := checkPartitionedTable(table); err != nil {
		return nil, err
	}
	if ahead < 0 {
		ahead = 0
	}

	created := make([]repositories.PartitionSpec, 0, ahead+1)
	today := time.Now().UTC().Truncate(24 * time.Hour)

	for i := 0; i <= ahead; i++ {
		day := today.AddDate(0, 0, i)
		spec := repositories.PartitionSpec{
			Table: table,
			Name:  partitionName(table, day),
			Day:   day,
		}

		// CREATE TABLE ... PARTITION OF cannot take bind parameters, so the
		// identifiers are built from the checked table name and a formatted
		// date, never from caller-supplied text.
		stmt := `CREATE TABLE IF NOT EXISTS ` + quoteIdent(spec.Name) +
			` PARTITION OF ` + quoteIdent(table) +
			` FOR VALUES FROM ('` + day.Format(time.RFC3339) + `') TO ('` + day.AddDate(0, 0, 1).Format(time.RFC3339) + `')`

		if _, err := r.store.Querier(ctx).Exec(ctx, stmt); err != nil {
			return created, eris.Wrapf(err, "create partition %s", spec.Name)
		}
		created = append(created, spec)
	}
	return created, nil
}

// DropPartitionsBefore drops whole partitions older than the cutoff. Dropping
// a partition is instant and produces no dead tuples, which is the entire
// reason for partitioning.
func (r *MaintenanceRepo) DropPartitionsBefore(
	ctx context.Context,
	table string,
	cutoff time.Time,
) ([]repositories.PartitionSpec, error) {
	if err := checkPartitionedTable(table); err != nil {
		return nil, err
	}

	existing, err := r.ListPartitions(ctx, table)
	if err != nil {
		return nil, err
	}

	cutoffDay := cutoff.UTC().Truncate(24 * time.Hour)
	dropped := make([]repositories.PartitionSpec, 0, len(existing))
	for _, spec := range existing {
		if !spec.Day.Before(cutoffDay) {
			continue
		}
		if _, err := r.store.Querier(ctx).Exec(ctx, `DROP TABLE IF EXISTS `+quoteIdent(spec.Name)); err != nil {
			return dropped, eris.Wrapf(err, "drop partition %s", spec.Name)
		}
		dropped = append(dropped, spec)
	}
	return dropped, nil
}

// ListPartitions reports the existing partitions of a partitioned table.
func (r *MaintenanceRepo) ListPartitions(ctx context.Context, table string) ([]repositories.PartitionSpec, error) {
	if err := checkPartitionedTable(table); err != nil {
		return nil, err
	}

	const query = `
		SELECT child.relname
		FROM   pg_inherits
		JOIN   pg_class parent ON parent.oid = pg_inherits.inhparent
		JOIN   pg_class child  ON child.oid  = pg_inherits.inhrelid
		WHERE  parent.relname = $1
		ORDER  BY child.relname`

	rows, err := r.store.Querier(ctx).Query(ctx, query, table)
	if err != nil {
		return nil, eris.Wrap(err, "list partitions")
	}
	defer rows.Close()

	specs := make([]repositories.PartitionSpec, 0, 128)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, eris.Wrap(err, "scan partition name")
		}
		day, err := partitionDay(table, name)
		if err != nil {
			// A partition we did not name: leave it alone rather than guess.
			continue
		}
		specs = append(specs, repositories.PartitionSpec{Table: table, Name: name, Day: day})
	}
	if err := rows.Err(); err != nil {
		return nil, eris.Wrap(err, "iterate partitions")
	}
	return specs, nil
}

// DeadTupleRatio reports dead tuples over live tuples for a table. Bloat on
// delivery_task is the main failure mode of a Postgres-backed queue, so this
// feeds an alert.
func (r *MaintenanceRepo) DeadTupleRatio(ctx context.Context, table string) (float64, error) {
	const query = `
		SELECT CASE WHEN n_live_tup + n_dead_tup = 0 THEN 0
		            ELSE n_dead_tup::float8 / (n_live_tup + n_dead_tup)
		       END
		FROM   pg_stat_user_tables
		WHERE  relname = $1`

	var ratio float64
	if err := r.store.Querier(ctx).QueryRow(ctx, query, table).Scan(&ratio); err != nil {
		return 0, eris.Wrap(err, "read dead tuple ratio")
	}
	return ratio, nil
}

// Healthy reports whether the pool can serve queries.
func (r *MaintenanceRepo) Healthy(ctx context.Context) error { return r.store.Healthy(ctx) }

func checkPartitionedTable(table string) error {
	if _, ok := partitionedTables[table]; !ok {
		return eris.Errorf("postgres: %q is not a partitioned table", table)
	}
	return nil
}

func partitionName(table string, day time.Time) string {
	return table + "_" + day.UTC().Format("20060102")
}

func partitionDay(table, name string) (time.Time, error) {
	prefix := table + "_"
	if len(name) != len(prefix)+8 || name[:len(prefix)] != prefix {
		return time.Time{}, eris.Errorf("postgres: %q is not a daily partition of %s", name, table)
	}
	return time.Parse("20060102", name[len(prefix):])
}

// quoteIdent double-quotes an identifier for safe interpolation into DDL.
func quoteIdent(name string) string {
	quoted := make([]byte, 0, len(name)+2)
	quoted = append(quoted, '"')
	for i := range len(name) {
		if name[i] == '"' {
			quoted = append(quoted, '"')
		}
		quoted = append(quoted, name[i])
	}
	return string(append(quoted, '"'))
}
