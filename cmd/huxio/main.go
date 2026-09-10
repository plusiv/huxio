// Command huxio is the single binary for every role of the webhook delivery
// service: the HTTP API, the delivery workers, schema migrations, token
// generation and the benchmark harness.
package main

import (
	"fmt"
	"os"

	_ "go.uber.org/automaxprocs" // honour cgroup CPU limits
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
