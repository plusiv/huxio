package main

import (
	"os"

	"github.com/spf13/cobra"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "huxio",
		Short:         "Self-hosted webhook delivery service",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	// Cobra writes command output to stderr by default, which makes `KEY=$(huxio
	// keygen)` silently produce an empty value. Values that are meant to be
	// captured go to stdout; commentary goes to stderr.
	root.SetOut(os.Stdout)
	root.SetErr(os.Stderr)

	root.AddCommand(
		newVersionCmd(),
		newKeygenCmd(),
		newMigrateCmd(),
		newServeCmd(),
		newJWTCmd(),
		newOrgCmd(),
		newOpenAPICmd(),
		newBenchCmd(),
	)
	return root
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the build version",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cmd.Println(version)
			return nil
		},
	}
}
