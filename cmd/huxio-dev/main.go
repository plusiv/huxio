// Command huxio-dev holds the commands that are not part of the product: the
// migration controls, the OpenAPI generator and the benchmark harness. They
// live outside the shipped binary because each of them can destroy a running
// deployment. The benchmark truncates every table it touches, and a hand-run
// `migrate down` drops a schema the service is serving against. The Makefile
// and CI are the only callers.
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
