package migrations

import (
	"context"
	"database/sql"
	"io/fs"

	"github.com/pressly/goose/v3"
	"github.com/rotisserie/eris"
)

const (
	schemaDir          = "schema"
	schemaVersionTable = "goose_db_version"
)

// MigrationResult describes one applied or pending migration.
type MigrationResult struct {
	Version int64
	Source  string
	Applied bool
}

// Up applies every pending schema migration and reports what it applied.
func Up(ctx context.Context, db *sql.DB) error {
	provider, err := newProvider(db)
	if err != nil {
		return err
	}
	if _, err := provider.Up(ctx); err != nil {
		return eris.Wrap(err, "apply migrations")
	}
	return nil
}

// Down rolls back the most recently applied migration.
func Down(ctx context.Context, db *sql.DB) error {
	provider, err := newProvider(db)
	if err != nil {
		return err
	}
	if _, err := provider.Down(ctx); err != nil {
		return eris.Wrap(err, "roll back migration")
	}
	return nil
}

// Status reports every known migration and whether it has been applied.
func Status(ctx context.Context, db *sql.DB) ([]MigrationResult, error) {
	provider, err := newProvider(db)
	if err != nil {
		return nil, err
	}
	statuses, err := provider.Status(ctx)
	if err != nil {
		return nil, eris.Wrap(err, "read migration status")
	}
	out := make([]MigrationResult, 0, len(statuses))
	for _, s := range statuses {
		out = append(out, MigrationResult{
			Version: s.Source.Version,
			Source:  s.Source.Path,
			Applied: s.State == goose.StateApplied,
		})
	}
	return out, nil
}

// Version reports the current schema version.
func Version(ctx context.Context, db *sql.DB) (int64, error) {
	provider, err := newProvider(db)
	if err != nil {
		return 0, err
	}
	version, err := provider.GetDBVersion(ctx)
	if err != nil {
		return 0, eris.Wrap(err, "read schema version")
	}
	return version, nil
}

// newProvider builds a goose provider over the embedded schema directory.
// The provider API is used rather than goose's package-level helpers because
// those mutate process-global state, which races as soon as two databases are
// migrated concurrently (as the integration suite does).
func newProvider(db *sql.DB) (*goose.Provider, error) {
	sub, err := fs.Sub(Migrations, schemaDir)
	if err != nil {
		return nil, eris.Wrap(err, "open embedded migrations")
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, sub, goose.WithTableName(schemaVersionTable))
	if err != nil {
		return nil, eris.Wrap(err, "build migration provider")
	}
	return provider, nil
}
