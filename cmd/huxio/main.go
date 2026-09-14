// Command huxio is the single binary for every role of the webhook delivery
// service: the HTTP API, the delivery workers, token generation and the
// tenant bootstrap.
package main

// TZ in a deployment makes time.Local need the zone database, which the
// runtime image no longer installs.
import (
	"fmt"
	"os"
	_ "time/tzdata"

	_ "go.uber.org/automaxprocs" // honour cgroup CPU limits
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
