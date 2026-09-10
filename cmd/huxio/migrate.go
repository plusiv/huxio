package main

import (
	"context"
	"database/sql"

	// Registers the pgx stdlib driver, which goose needs for database/sql.
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/plusiv/huxio/configs"
	"github.com/plusiv/huxio/migrations"
	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
)

func newMigrateCmd() *cobra.Command {
	migrate := &cobra.Command{
		Use:   "migrate",
		Short: "Apply, roll back or inspect schema migrations",
	}

	migrate.AddCommand(
		&cobra.Command{
			Use:   "up",
			Short: "Apply every pending migration",
			RunE: func(cmd *cobra.Command, _ []string) error {
				return withMigrationDB(cmd, migrations.Up)
			},
		},
		&cobra.Command{
			Use:   "down",
			Short: "Roll back the most recent migration",
			RunE: func(cmd *cobra.Command, _ []string) error {
				return withMigrationDB(cmd, migrations.Down)
			},
		},
		&cobra.Command{
			Use:   "status",
			Short: "Show applied and pending migrations",
			RunE: func(cmd *cobra.Command, _ []string) error {
				return withMigrationDB(cmd, func(ctx context.Context, db *sql.DB) error {
					statuses, err := migrations.Status(ctx, db)
					if err != nil {
						return err
					}
					for _, s := range statuses {
						state := "pending"
						if s.Applied {
							state = "applied"
						}
						cmd.Printf("%-8s %d %s\n", state, s.Version, s.Source)
					}
					return nil
				})
			},
		},
	)
	return migrate
}

func withMigrationDB(cmd *cobra.Command, fn func(ctx context.Context, db *sql.DB) error) error {
	cfg, err := configs.Load()
	if err != nil {
		return err
	}
	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return eris.Wrap(err, "open database")
	}
	defer db.Close()

	return fn(cmd.Context(), db)
}
