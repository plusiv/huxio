package configs_test

import (
	"testing"
	"time"

	"github.com/plusiv/huxio/configs"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("HUXIO_DATABASE_URL", "postgres://localhost:5432/huxio")

	cfg, err := configs.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Role != configs.RoleAll {
		t.Errorf("Role = %q, want %q", cfg.Role, configs.RoleAll)
	}
	if len(cfg.Pools) != 1 || cfg.Pools[0] != configs.DefaultPool {
		t.Errorf("Pools = %v, want [default]", cfg.Pools)
	}
	if !cfg.RunsAPI() || !cfg.RunsWorker() {
		t.Error("role=all must run both API and worker loops")
	}
	// Derived: lock TTL defaults to request timeout + 60s.
	if want := cfg.WorkerRequestTimeout + 60*time.Second; cfg.TaskLockTTL != want {
		t.Errorf("TaskLockTTL = %s, want %s", cfg.TaskLockTTL, want)
	}
	if cfg.MaxPayloadBytes != 1<<20 {
		t.Errorf("MaxPayloadBytes = %d, want 1MiB", cfg.MaxPayloadBytes)
	}
	if cfg.CompatHeaders != configs.CompatHeadersStandard {
		t.Errorf("CompatHeaders = %q", cfg.CompatHeaders)
	}
	if cfg.LaneWaitTimeout != 2*time.Second || cfg.LaneRequeueDelay != time.Second {
		t.Errorf("lane wait/requeue = %s/%s, want 2s/1s", cfg.LaneWaitTimeout, cfg.LaneRequeueDelay)
	}
	if cfg.AttemptWriterFlushInterval != 20*time.Millisecond {
		t.Errorf("AttemptWriterFlushInterval = %s, want 20ms", cfg.AttemptWriterFlushInterval)
	}
	if cfg.PayloadCacheMaxBytes != 64<<20 || cfg.PayloadCacheTTL != 30*time.Second {
		t.Errorf("payload cache = %d bytes / %s, want 64MiB / 30s", cfg.PayloadCacheMaxBytes, cfg.PayloadCacheTTL)
	}
}

func TestLoadBuildsDSNFromParts(t *testing.T) {
	t.Setenv("HUXIO_DATABASE_URL", "")
	t.Setenv("HUXIO_POSTGRES_HOST", "db.internal")
	t.Setenv("HUXIO_POSTGRES_PORT", "6432")
	t.Setenv("HUXIO_POSTGRES_USER", "huxio")
	t.Setenv("HUXIO_POSTGRES_PASSWORD", "hunter2")
	t.Setenv("HUXIO_POSTGRES_DB", "huxio_prod")
	t.Setenv("HUXIO_POSTGRES_SSL_MODE", "require")

	cfg, err := configs.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := "postgres://huxio:hunter2@db.internal:6432/huxio_prod?sslmode=require"
	if cfg.DatabaseURL != want {
		t.Errorf("DatabaseURL = %q, want %q", cfg.DatabaseURL, want)
	}
}

func TestLoadRoleSizedPools(t *testing.T) {
	t.Setenv("HUXIO_ROLE", "worker")
	t.Setenv("HUXIO_POOLS", "default,quarantine,default")

	cfg, err := configs.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RunsAPI() {
		t.Error("worker role must not serve the API")
	}
	if got := len(cfg.Pools); got != 2 {
		t.Errorf("Pools = %v, want deduplicated pair", cfg.Pools)
	}
	if cfg.DBMaxConns != 6 {
		t.Errorf("DBMaxConns = %d, want 6 for worker role", cfg.DBMaxConns)
	}
}

func TestLoadRejectsBadRoleAndCompat(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{"unknown role", map[string]string{"HUXIO_ROLE": "delivery"}},
		{"unknown compat headers", map[string]string{"HUXIO_COMPAT_HEADERS": "stripe"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if _, err := configs.Load(); err == nil {
				t.Error("expected Load to fail")
			}
		})
	}
}

func TestValidate(t *testing.T) {
	t.Parallel()

	base := func() *configs.Config {
		return &configs.Config{
			MaxPayloadBytes:         1 << 20,
			ClaimBatchSize:          100,
			WorkerMaxInflight:       2000,
			LaneInitialConcurrency:  8,
			LaneMaxConcurrency:      64,
			WorkerRequestTimeout:    30 * time.Second,
			TaskLockTTL:             90 * time.Second,
			LeaseHeartbeat:          5 * time.Second,
			LeaseTTL:                15 * time.Second,
			AttemptWriterBatchSize:  500,
			AttemptWriterBufferSize: 8192,
			RetentionMessagesDays:   90,
			RetentionAttemptsDays:   90,
			PartitionsAheadDays:     7,
		}
	}

	if err := base().Validate(); err != nil {
		t.Fatalf("baseline config must validate: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*configs.Config)
	}{
		{"lock ttl below request timeout", func(c *configs.Config) { c.TaskLockTTL = 10 * time.Second }},
		{"heartbeat above lease ttl", func(c *configs.Config) { c.LeaseHeartbeat = 20 * time.Second }},
		{"lane initial above max", func(c *configs.Config) { c.LaneInitialConcurrency = 128 }},
		{"buffer below batch", func(c *configs.Config) { c.AttemptWriterBufferSize = 10 }},
		{"zero payload cap", func(c *configs.Config) { c.MaxPayloadBytes = 0 }},
		{"zero claim batch", func(c *configs.Config) { c.ClaimBatchSize = 0 }},
		{"zero retention", func(c *configs.Config) { c.RetentionMessagesDays = 0 }},
		{"zero partitions ahead", func(c *configs.Config) { c.PartitionsAheadDays = 0 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := base()
			tc.mutate(cfg)
			if err := cfg.Validate(); err == nil {
				t.Error("expected Validate to fail")
			}
		})
	}
}
