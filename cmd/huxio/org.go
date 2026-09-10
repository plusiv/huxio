package main

import (
	"context"
	"time"

	"github.com/plusiv/huxio/configs"
	"github.com/plusiv/huxio/internal/adapters/outbound/persistence/postgres"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/spf13/cobra"
)

// newOrgCmd manages tenants. There is deliberately no HTTP surface for this:
// creating a tenant is an operator action, and every API token is already
// scoped to one, so an endpoint for it would have nothing to authenticate
// against.
func newOrgCmd() *cobra.Command {
	orgCmd := &cobra.Command{
		Use:   "org",
		Short: "Manage tenants",
	}

	create := &cobra.Command{
		Use:   "create [name]",
		Short: "Create a tenant and print its id",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withStore(cmd, func(ctx context.Context, store *postgres.Store) error {
				org, err := orgUseCase(store).CreateOrganization(ctx,
					usecases.CreateOrganizationInput{Name: args[0]})
				if err != nil {
					return err
				}
				// The id goes to stdout so it can be captured; the hint does not.
				cmd.Println(org.ID)
				cmd.PrintErrf("mint a token with: huxio jwt generate %s\n", org.ID)
				return nil
			})
		},
	}

	list := &cobra.Command{
		Use:   "list",
		Short: "List tenants",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withStore(cmd, func(ctx context.Context, store *postgres.Store) error {
				result, err := orgUseCase(store).ListOrganizations(ctx,
					usecases.ListOrganizationsInput{Limit: repositories.MaxPageSize})
				if err != nil {
					return err
				}
				for _, org := range result.Items {
					cmd.Printf("%s\t%s\t%s\n", org.ID, org.CreatedAt.Format(time.RFC3339), org.Name)
				}
				return nil
			})
		},
	}

	orgCmd.AddCommand(create, list)
	return orgCmd
}

// orgUseCase wires the tenant use case over a one-shot store.
func orgUseCase(store *postgres.Store) *usecases.OrganizationUseCase {
	return usecases.NewOrganizationUseCase(
		postgres.NewOrganizationRepo(store),
		postgres.NewSnapshotRepo(store),
	)
}

// withStore opens a short-lived pgx pool for a one-shot command.
func withStore(cmd *cobra.Command, fn func(ctx context.Context, store *postgres.Store) error) error {
	cfg, err := configs.Load()
	if err != nil {
		return err
	}

	ctx := cmd.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	pool, err := postgres.NewPool(ctx, postgres.PoolConfig{DSN: cfg.DatabaseURL, MaxConns: 2, MinConns: 1})
	if err != nil {
		return err
	}
	defer pool.Close()

	return fn(ctx, postgres.NewStore(pool))
}
