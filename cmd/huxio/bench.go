package main

import (
	"github.com/plusiv/huxio/bench/harness"
	"github.com/plusiv/huxio/configs"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
)

func newBenchCmd() *cobra.Command {
	var (
		scenario string
		baseline string
		cfg      harness.Config
	)

	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Run a benchmark scenario",
		Long: "Runs one of the benchmark scenarios against the configured\n" +
			"database, in this process, using the same components the server runs. Results\n" +
			"are written to bench/results as JSON.\n\n" +
			"The database is truncated at the start of a run: point it at a scratch\n" +
			"database, never at anything you care about.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			appConfig, err := configs.Load()
			if err != nil {
				return err
			}
			if cfg.DSN == "" {
				cfg.DSN = appConfig.DatabaseURL
			}

			log := logger.Init(appConfig.LogLevel)
			ctx := logger.Context(cmd.Context(), log)

			// Log what the run will use, not what was typed: the scenario fills in
			// every flag left at zero.
			cfg = harness.Defaults(scenario, cfg)

			log.Info().
				Str("scenario", scenario).
				Int("rate", cfg.Rate).
				Dur("duration", cfg.Duration).
				Int("tenants", cfg.Tenants).
				Msg("starting benchmark")

			result, err := harness.Run(ctx, scenario, cfg)
			if err != nil {
				return err
			}

			path, err := result.Write(cfg.ResultsDir)
			if err != nil {
				return err
			}

			// The summary goes to stdout so it can be piped; the detail is in the JSON.
			cmd.Printf("scenario:            %s\n", result.Scenario)
			cmd.Printf("ingest accepted:     %d (%.0f/s)\n", result.Ingest.Accepted, result.Ingest.RPS)
			cmd.Printf("ingest p50/p99 (ms): %.2f / %.2f\n", result.Ingest.Latency.P50, result.Ingest.Latency.P99)
			cmd.Printf("delivered:           %d of %d expected (%.0f/s, %.0f/s/core)\n",
				result.Delivery.Delivered, result.Delivery.Expected,
				result.Delivery.PerSecond, result.Delivery.PerSecondPerCore)
			cmd.Printf("delivery p50/p99(ms):%.2f / %.2f\n",
				result.Delivery.Latency.P50, result.Delivery.Latency.P99)
			cmd.Printf("alloc/delivery:      %.0f bytes\n", result.Delivery.AllocBytesPerDelivery)
			cmd.Printf("queue final/lag:     %d rows, %.2fs\n", result.Queue.FinalDepth, result.Queue.MaxLagSeconds)
			cmd.Printf("goroutines:          %d -> %d\n", result.Runtime.StartGoroutines, result.Runtime.EndGoroutines)
			for _, tenant := range result.Tenants {
				cmd.Printf("  tenant %-8s %6d delivered, p99 %.2fms\n", tenant.Role, tenant.Delivered, tenant.Latency.P99)
			}
			cmd.Printf("result:              %s\n", passLabel(result.Pass))
			if result.Reason != "" {
				cmd.Printf("reason:              %s\n", result.Reason)
			}
			cmd.PrintErrf("wrote %s\n", path)

			if baseline != "" {
				previous, err := harness.Load(baseline)
				if err != nil {
					return err
				}
				regressions := result.CompareTo(previous)
				for _, regression := range regressions {
					cmd.Printf("regression:          %s %.2f -> %.2f (%+.1f%%)\n",
						regression.Metric, regression.Baseline, regression.Current, regression.Change*100)
				}
				if len(regressions) > 0 {
					return eris.Errorf("%d headline numbers regressed beyond %.0f%% against %s",
						len(regressions), harness.RegressionBudget*100, baseline)
				}
				cmd.Printf("regression check:    no headline number moved beyond %.0f%%\n", harness.RegressionBudget*100)
			}

			if !result.Pass {
				return eris.Errorf("scenario %s did not pass: %s", result.Scenario, result.Reason)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&scenario, "scenario", harness.ScenarioThroughput,
		"throughput, isolation, retry-storm, cold-start or soak")
	cmd.Flags().IntVar(&cfg.Rate, "rate", 0, "target ingest rate per second")
	cmd.Flags().DurationVar(&cfg.Duration, "duration", 0, "how long to apply load")
	cmd.Flags().IntVar(&cfg.Tenants, "tenants", 0, "how many organizations send concurrently")
	cmd.Flags().IntVar(&cfg.EndpointsPerTenant, "endpoints", 0, "endpoints per tenant")
	cmd.Flags().IntVar(&cfg.PayloadBytes, "payload-bytes", 0, "approximate payload size")
	cmd.Flags().IntVar(&cfg.Workers, "workers", 0, "how many delivery engines to run")
	cmd.Flags().Float64Var(&cfg.TarpitFraction, "tarpit-fraction", 0, "share of tenants pointed at a tarpit")
	cmd.Flags().Float64Var(&cfg.FailureRate, "failure-rate", 0, "flaky sink failure probability")
	cmd.Flags().BoolVar(&cfg.TLS, "tls", false, "serve the sinks over HTTPS")
	cmd.Flags().StringVar(&cfg.DSN, "dsn", "", "database to run against (defaults to HUXIO_DATABASE_URL)")
	cmd.Flags().StringVar(&cfg.ResultsDir, "results", "", "directory for result JSON (default bench/results)")
	cmd.Flags().StringVar(&baseline, "baseline", "",
		"compare against a previous result file and fail on a regression beyond the budget")
	cmd.Flags().DurationVar(&cfg.DrainTimeout, "drain-timeout", 0, "how long to wait for the queue to drain")
	cmd.Flags().DurationSliceVar(&cfg.RetrySchedule, "retry-schedule", nil,
		"override the retry delays, e.g. 5s,5m,30m (retry-storm compresses them by default)")

	return cmd
}

func passLabel(pass bool) string {
	if pass {
		return "PASS"
	}
	return "FAIL"
}
