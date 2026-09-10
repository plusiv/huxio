package main

import (
	"time"

	"github.com/plusiv/huxio/configs"
	"github.com/plusiv/huxio/internal/infrastructure/auth"
	"github.com/spf13/cobra"
)

func newJWTCmd() *cobra.Command {
	jwtCmd := &cobra.Command{
		Use:   "jwt",
		Short: "Mint API tokens",
	}

	var ttl time.Duration
	generate := &cobra.Command{
		Use:   "generate [org_id]",
		Short: "Mint an organization token with full access to that tenant",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := configs.Load()
			if err != nil {
				return err
			}
			manager, err := auth.NewManager(auth.Config{
				Algorithm:     cfg.JWTAlgorithm,
				Secret:        cfg.JWTSecret,
				PrivateKeyPEM: cfg.JWTPrivateKeyPEM,
				PublicKeyPEM:  cfg.JWTPublicKeyPEM,
				Issuer:        cfg.JWTIssuer,
			})
			if err != nil {
				return err
			}
			token, expiry, err := manager.IssueOrgToken(args[0], ttl)
			if err != nil {
				return err
			}
			cmd.Println(token)
			cmd.PrintErrf("expires %s\n", expiry.Format(time.RFC3339))
			return nil
		},
	}
	generate.Flags().DurationVar(&ttl, "ttl", 365*24*time.Hour, "token lifetime")

	jwtCmd.AddCommand(generate)
	return jwtCmd
}
