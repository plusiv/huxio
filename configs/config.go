// Package configs loads the runtime configuration from the environment. The
// knob count stays deliberately small, every knob has an obvious unit, and
// the defaults are the ones we run in production: a setting nobody ever
// changes is a setting nobody has tested.
package configs

import (
	"fmt"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"github.com/rotisserie/eris"
)

// Role selects which loops a process runs.
type Role string

// The process roles. One binary; the role decides which loops it runs.
const (
	RoleAll    Role = "all"
	RoleAPI    Role = "api"
	RoleWorker Role = "worker"
)

// CompatHeaders selects the signature header naming scheme.
const (
	CompatHeadersStandard = "standard"
	CompatHeadersHuxio     = "huxio"
)

// QueuePartitions is the fixed partition count. It is a constant, not a knob:
// changing it invalidates every routed partition_key.
const QueuePartitions = 256

// DefaultPool is the pool every partition and endpoint belongs to until an
// operator moves it.
const DefaultPool = "default"

// QuarantinePool holds endpoints whose breaker has been open past the
// quarantine threshold.
const QuarantinePool = "quarantine"

// Config is the normalized runtime configuration consumed by application
// components after environment parsing and post-processing.
type Config struct {
	Role  Role
	Pools []string

	Port        string
	DatabaseURL string
	DBMaxConns  int32
	DBMinConns  int32
	LogLevel    string

	EncryptionKeys []string

	JWTSecret        string
	JWTPrivateKeyPEM string
	JWTPublicKeyPEM  string
	JWTAlgorithm     string
	JWTIssuer        string
	PortalTokenTTL   time.Duration
	PortalBaseURL    string

	MaxPayloadBytes     int64
	CompatHeaders       string
	CompressionMinBytes int
	IdempotencyTTL      time.Duration

	WorkerRequestTimeout time.Duration
	WorkerMaxInflight    int
	ClaimBatchSize       int
	TaskLockTTL          time.Duration
	QueuePollInterval    time.Duration

	LaneInitialConcurrency int
	LaneMaxConcurrency     int
	LaneIdleEviction       time.Duration

	BreakerFailureThreshold     int
	BreakerCooldown             time.Duration
	BreakerMaxCooldown          time.Duration
	QuarantineAfter             time.Duration
	EndpointFailureDisableAfter time.Duration

	AttemptWriterBufferSize    int
	AttemptWriterBatchSize     int
	AttemptWriterFlushInterval time.Duration
	AttemptBodyLimitBytes      int

	RetentionMessagesDays int
	RetentionAttemptsDays int
	PartitionsAheadDays   int

	AllowSubnets []string

	RateLimitOrgRPS int
	RateLimitAppRPS int

	ConfigRefreshInterval time.Duration
	LeaseTTL              time.Duration
	LeaseHeartbeat        time.Duration
	ShutdownTimeout       time.Duration

	CORSAllowOrigins []string
}

// envVars is the raw environment input model parsed by envconfig. It mirrors
// environment variable names (all prefixed HUXIO_) and keeps string forms
// before normalization into Config.
type envVars struct {
	Role  string   `default:"all"`
	Pools []string `default:"default"`

	Port     string `default:"8080"`
	LogLevel string `default:"info"                                     split_words:"true"`

	DatabaseURL string `envconfig:"HUXIO_DATABASE_URL"`

	PostgresHost     string `default:"localhost"        split_words:"true"`
	PostgresPort     string `default:"5432"             split_words:"true"`
	PostgresUser     string `default:"postgres"         split_words:"true"`
	PostgresPassword string `default:"postgres"         split_words:"true"`
	PostgresDB       string `default:"huxio"            split_words:"true"`
	PostgresSSLMode  string `default:"disable"          split_words:"true"`

	DBMaxConns int32 `default:"0" split_words:"true"`
	DBMinConns int32 `default:"2" split_words:"true"`

	EncryptionKey []string `split_words:"true"`

	JWTSecret        string        `envconfig:"HUXIO_JWT_SECRET"`
	JWTPrivateKeyPEM string        `envconfig:"HUXIO_JWT_PRIVATE_KEY"`
	JWTPublicKeyPEM  string        `envconfig:"HUXIO_JWT_PUBLIC_KEY"`
	JWTAlgorithm     string        `default:"HS256"                envconfig:"HUXIO_JWT_ALGORITHM"`
	JWTIssuer        string        `default:"huxio"                envconfig:"HUXIO_JWT_ISSUER"`
	PortalTokenTTL   time.Duration `default:"8h"                   envconfig:"HUXIO_PORTAL_TOKEN_TTL"`
	PortalBaseURL    string        `default:"http://localhost:8080" envconfig:"HUXIO_PORTAL_BASE_URL"`

	MaxPayloadBytes     int64         `default:"1048576" split_words:"true"`
	CompatHeaders       string        `default:"standard" split_words:"true"`
	CompressionMinBytes int           `default:"512"      split_words:"true"`
	IdempotencyTTL      time.Duration `default:"12h"      split_words:"true"`

	WorkerRequestTimeout time.Duration `default:"30s"  split_words:"true"`
	WorkerMaxInflight    int           `default:"2000" split_words:"true"`
	ClaimBatchSize       int           `default:"100"  split_words:"true"`
	TaskLockTTL          time.Duration `default:"0s"   split_words:"true"`
	QueuePollInterval    time.Duration `default:"250ms" split_words:"true"`

	LaneInitialConcurrency int           `default:"8"   split_words:"true"`
	LaneMaxConcurrency     int           `default:"64"  split_words:"true"`
	LaneIdleEviction       time.Duration `default:"10m" split_words:"true"`

	BreakerFailureThreshold     int           `default:"20"   split_words:"true"`
	BreakerCooldown             time.Duration `default:"30s"  split_words:"true"`
	BreakerMaxCooldown          time.Duration `default:"10m"  split_words:"true"`
	QuarantineAfter             time.Duration `default:"15m"  split_words:"true"`
	EndpointFailureDisableAfter time.Duration `default:"120m" split_words:"true"`

	AttemptWriterBufferSize    int           `default:"8192" split_words:"true"`
	AttemptWriterBatchSize     int           `default:"500"  split_words:"true"`
	AttemptWriterFlushInterval time.Duration `default:"5ms"  split_words:"true"`
	AttemptBodyLimitBytes      int           `default:"8192" split_words:"true"`

	RetentionMessagesDays int `default:"90" split_words:"true"`
	RetentionAttemptsDays int `default:"90" split_words:"true"`
	PartitionsAheadDays   int `default:"7"  split_words:"true"`

	AllowSubnets []string `split_words:"true"`

	RateLimitOrgRPS int `default:"2000" envconfig:"HUXIO_RATE_LIMIT_ORG_RPS"`
	RateLimitAppRPS int `default:"500"  envconfig:"HUXIO_RATE_LIMIT_APP_RPS"`

	ConfigRefreshInterval time.Duration `default:"60s" split_words:"true"`
	LeaseTTL              time.Duration `default:"15s" split_words:"true"`
	LeaseHeartbeat        time.Duration `default:"5s"  split_words:"true"`
	ShutdownTimeout       time.Duration `default:"60s" split_words:"true"`

	CORSAllowOrigins []string `default:"*" split_words:"true"`
}

// Load parses the environment into a Config, applying defaults and derived
// values. It returns an error rather than panicking so callers (including
// tests) decide how to fail.
func Load() (*Config, error) {
	// Local .env for developer workflows; process env always wins.
	_ = godotenv.Load(".env", "../.env")

	var env envVars
	if err := envconfig.Process("huxio", &env); err != nil {
		return nil, eris.Wrap(err, "process environment")
	}

	role := Role(strings.ToLower(strings.TrimSpace(env.Role)))
	switch role {
	case RoleAll, RoleAPI, RoleWorker:
	default:
		return nil, eris.Errorf("configs: unsupported HUXIO_ROLE=%q", env.Role)
	}

	compat := strings.ToLower(strings.TrimSpace(env.CompatHeaders))
	if compat != CompatHeadersStandard && compat != CompatHeadersHuxio {
		return nil, eris.Errorf("configs: unsupported HUXIO_COMPAT_HEADERS=%q", env.CompatHeaders)
	}

	dsn := strings.TrimSpace(env.DatabaseURL)
	if dsn == "" {
		dsn = fmt.Sprintf(
			"postgres://%s:%s@%s:%s/%s?sslmode=%s",
			env.PostgresUser, env.PostgresPassword,
			env.PostgresHost, env.PostgresPort,
			env.PostgresDB, env.PostgresSSLMode,
		)
	}

	pools := normalizePools(env.Pools)

	maxConns := env.DBMaxConns
	if maxConns <= 0 {
		// Workers spend their time blocked on customer HTTP, not on Postgres, so a
		// handful of connections saturates them. Sizing worker pools like API pools
		// just burns Postgres backends.
		if role == RoleWorker {
			maxConns = 6
		} else {
			maxConns = 12
		}
	}

	// The lock TTL must outlast the request timeout plus a buffer. Set it
	// shorter and a slow delivery's lock expires while the request is still
	// on the wire, so another worker claims the same task and the receiver
	// gets it twice.
	lockTTL := env.TaskLockTTL
	if lockTTL <= 0 {
		lockTTL = env.WorkerRequestTimeout + 60*time.Second
	}

	cfg := &Config{
		Role:        role,
		Pools:       pools,
		Port:        env.Port,
		DatabaseURL: dsn,
		DBMaxConns:  maxConns,
		DBMinConns:  env.DBMinConns,
		LogLevel:    env.LogLevel,

		EncryptionKeys: env.EncryptionKey,

		JWTSecret:        env.JWTSecret,
		JWTPrivateKeyPEM: env.JWTPrivateKeyPEM,
		JWTPublicKeyPEM:  env.JWTPublicKeyPEM,
		JWTAlgorithm:     strings.ToUpper(strings.TrimSpace(env.JWTAlgorithm)),
		JWTIssuer:        env.JWTIssuer,
		PortalTokenTTL:   env.PortalTokenTTL,
		PortalBaseURL:    strings.TrimRight(env.PortalBaseURL, "/"),

		MaxPayloadBytes:     env.MaxPayloadBytes,
		CompatHeaders:       compat,
		CompressionMinBytes: env.CompressionMinBytes,
		IdempotencyTTL:      env.IdempotencyTTL,

		WorkerRequestTimeout: env.WorkerRequestTimeout,
		WorkerMaxInflight:    env.WorkerMaxInflight,
		ClaimBatchSize:       env.ClaimBatchSize,
		TaskLockTTL:          lockTTL,
		QueuePollInterval:    env.QueuePollInterval,

		LaneInitialConcurrency: env.LaneInitialConcurrency,
		LaneMaxConcurrency:     env.LaneMaxConcurrency,
		LaneIdleEviction:       env.LaneIdleEviction,

		BreakerFailureThreshold:     env.BreakerFailureThreshold,
		BreakerCooldown:             env.BreakerCooldown,
		BreakerMaxCooldown:          env.BreakerMaxCooldown,
		QuarantineAfter:             env.QuarantineAfter,
		EndpointFailureDisableAfter: env.EndpointFailureDisableAfter,

		AttemptWriterBufferSize:    env.AttemptWriterBufferSize,
		AttemptWriterBatchSize:     env.AttemptWriterBatchSize,
		AttemptWriterFlushInterval: env.AttemptWriterFlushInterval,
		AttemptBodyLimitBytes:      env.AttemptBodyLimitBytes,

		RetentionMessagesDays: env.RetentionMessagesDays,
		RetentionAttemptsDays: env.RetentionAttemptsDays,
		PartitionsAheadDays:   env.PartitionsAheadDays,

		AllowSubnets: env.AllowSubnets,

		RateLimitOrgRPS: env.RateLimitOrgRPS,
		RateLimitAppRPS: env.RateLimitAppRPS,

		ConfigRefreshInterval: env.ConfigRefreshInterval,
		LeaseTTL:              env.LeaseTTL,
		LeaseHeartbeat:        env.LeaseHeartbeat,
		ShutdownTimeout:       env.ShutdownTimeout,

		CORSAllowOrigins: env.CORSAllowOrigins,
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Validate rejects configurations that would silently misbehave at runtime.
func (c *Config) Validate() error {
	if c.MaxPayloadBytes <= 0 {
		return eris.New("configs: HUXIO_MAX_PAYLOAD_BYTES must be positive")
	}
	if c.ClaimBatchSize <= 0 {
		return eris.New("configs: HUXIO_CLAIM_BATCH_SIZE must be positive")
	}
	if c.WorkerMaxInflight <= 0 {
		return eris.New("configs: HUXIO_WORKER_MAX_INFLIGHT must be positive")
	}
	if c.LaneInitialConcurrency < 1 || c.LaneInitialConcurrency > c.LaneMaxConcurrency {
		return eris.New("configs: lane initial concurrency must be within [1, lane max concurrency]")
	}
	// The claim lock must outlive the slowest possible delivery.
	if c.TaskLockTTL <= c.WorkerRequestTimeout {
		return eris.Errorf(
			"configs: HUXIO_TASK_LOCK_TTL (%s) must exceed HUXIO_WORKER_REQUEST_TIMEOUT (%s); "+
				"a shorter lock lets a slow delivery be re-claimed while still in flight",
			c.TaskLockTTL, c.WorkerRequestTimeout,
		)
	}
	if c.LeaseHeartbeat >= c.LeaseTTL {
		return eris.Errorf("configs: HUXIO_LEASE_HEARTBEAT (%s) must be shorter than HUXIO_LEASE_TTL (%s)", c.LeaseHeartbeat, c.LeaseTTL)
	}
	if c.AttemptWriterBatchSize <= 0 || c.AttemptWriterBufferSize < c.AttemptWriterBatchSize {
		return eris.New("configs: attempt writer buffer must be at least the batch size")
	}
	if c.RetentionMessagesDays <= 0 || c.RetentionAttemptsDays <= 0 {
		return eris.New("configs: retention windows must be positive")
	}
	if c.PartitionsAheadDays <= 0 {
		return eris.New("configs: HUXIO_PARTITIONS_AHEAD_DAYS must be positive")
	}
	return nil
}

// RunsAPI reports whether this process serves the HTTP API.
func (c *Config) RunsAPI() bool { return c.Role == RoleAPI || c.Role == RoleAll }

// RunsWorker reports whether this process runs delivery loops.
func (c *Config) RunsWorker() bool { return c.Role == RoleWorker || c.Role == RoleAll }

func normalizePools(raw []string) []string {
	seen := make(map[string]struct{}, len(raw))
	pools := make([]string, 0, len(raw))
	for _, p := range raw {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		pools = append(pools, p)
	}
	if len(pools) == 0 {
		pools = []string{DefaultPool}
	}
	return pools
}
