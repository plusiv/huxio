// Command huxio-dev provides migration controls, OpenAPI generation, and
// benchmarks outside the shipped binary. Benchmarks truncate tables, and
// migrate down can drop schema used by running processes.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	root := &cobra.Command{
		Use:           "huxio-dev",
		Short:         "Development and release tooling for huxio",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetOut(os.Stdout)
	root.SetErr(os.Stderr)

	root.AddCommand(
		newMigrateCmd(),
		newOpenAPICmd(),
		newBenchCmd(),
	)

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
