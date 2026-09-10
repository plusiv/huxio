package harness

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/plusiv/huxio/bench/sinks"
	inboundhttp "github.com/plusiv/huxio/internal/adapters/inbound/http"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/handlers"
	"github.com/plusiv/huxio/internal/adapters/outbound/deliveryhttp"
	"github.com/plusiv/huxio/internal/adapters/outbound/persistence/postgres"
	"github.com/plusiv/huxio/internal/application/config"
	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/retry"
	"github.com/plusiv/huxio/internal/infrastructure/auth"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/plusiv/huxio/internal/infrastructure/payload"
	"github.com/plusiv/huxio/internal/infrastructure/secrets"
	"github.com/plusiv/huxio/internal/infrastructure/telemetry"
	"github.com/plusiv/huxio/migrations"
	"github.com/rotisserie/eris"
	"golang.org/x/sync/errgroup"
)

// Config is one benchmark run.
type Config struct {
	// DSN is the database to run against. The harness creates its own schema
	// there and expects to be the only thing using it.
	DSN string
	// Rate is the target ingest rate in messages per second.
	Rate int
	// Duration is how long load is applied.
	Duration time.Duration
	// Tenants is how many organizations send concurrently.
	Tenants int
	// EndpointsPerTenant is how many endpoints each tenant's messages fan out
	// to.
	EndpointsPerTenant int
	// PayloadBytes pads the message payload to roughly this size.
	PayloadBytes int
	// Workers is how many delivery engines run.
	Workers int
	// Sink selects the endpoint behaviour for tenants under test.
	Sink sinks.Kind
	// TarpitFraction is the share of tenants whose endpoints are tarpitted.
	TarpitFraction float64
	// FailureRate is the flaky sink's failure probability.
	FailureRate float64
	// TLS serves the sinks over HTTPS.
	TLS bool
	// ResultsDir overrides where results are written.
	ResultsDir string
	// DrainTimeout bounds how long the harness waits for the queue to drain
	// after load stops.
	DrainTimeout time.Duration
	// RetrySchedule overrides the production retry delays. The retry-storm
	// scenario compresses them by default: the real schedule's second delay
	// is five minutes, which no short run can wait out. A long run can pass
	// the production schedule and watch the real thing.
	RetrySchedule []time.Duration
}

// tenant is one organization under load.
type tenant struct {
	orgID     string
	appID     string
	token     string
	role      string
	sink      *sinks.Sink
	endpoints int
}

// harness holds one run's wiring.
type harness struct {
	cfg Config

	db           *dbHandles
	server       *httptest.Server
	metrics      *telemetry.Metrics
	tenants      []*tenant
	tenantsByOrg map[string]*tenant

	// latencies records ingest-to-arrival per tenant, keyed by message id
	// until the delivery lands.
	runtime *runtimeComponents

	mu     sync.Mutex
	sentAt map[string]sendRecord
	// pending holds arrival times for message ids whose send has not been
	// recorded yet. The sink can see a delivery before the ingest response has
	// been read and recorded by the sender, and those are exactly the fastest
	// deliveries, so dropping them would bias the tail the wrong way.
	pending   map[string][]time.Time
	latencies map[string][]float64
	delivered map[string]int64
}

type sendRecord struct {
	orgID string
	at    time.Time
}

// Run executes a scenario end to end and returns its result.
func Run(ctx context.Context, scenario string, cfg Config) (*Result, error) {
	cfg = applyDefaults(scenario, cfg)

	switch scenario {
	case ScenarioThroughput:
		return runThroughput(ctx, cfg)
	case ScenarioIsolation:
		return runIsolation(ctx, cfg)
	case ScenarioRetryStorm:
		return runRetryStorm(ctx, cfg)
	case ScenarioColdStart:
		return runColdStart(ctx, cfg)
	case ScenarioSoak:
		return runSoak(ctx, cfg)
	default:
		return nil, eris.Errorf("harness: unknown scenario %q", scenario)
	}
}

// dbHandles keeps the database handles a run needs.
type dbHandles struct {
	store  *postgres.Store
	closer func()
}

// setup migrates the database and wires an API server plus workers, exactly
// as the binary does, so the harness measures the real thing.
func setup(ctx context.Context, cfg Config, tenants []tenantSpec) (*harness, error) {
	if strings.TrimSpace(cfg.DSN) == "" {
		return nil, eris.New("harness: a database DSN is required")
	}

	sqlDB, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, eris.Wrap(err, "open database")
	}
	if err := migrations.Up(ctx, sqlDB); err != nil {
		_ = sqlDB.Close()
		return nil, eris.Wrap(err, "run migrations")
	}
	logger.FromContext(ctx).Warn().
		Str("database", redactDSN(cfg.DSN)).
		Msg("truncating every table in this database before the run")
	if err := truncate(ctx, sqlDB); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	_ = sqlDB.Close()

	pgxPool, err := postgres.NewPool(ctx, postgres.PoolConfig{
		DSN:      cfg.DSN,
		MaxConns: int32(max(16, cfg.Workers*6)),
		MinConns: 4,
		// The harness is deliberately noisy about slow queries.
		SlowQueryThreshold: time.Second,
	})
	if err != nil {
		return nil, err
	}

	store := postgres.NewStore(pgxPool)
	h := &harness{
		cfg:          cfg,
		db:           &dbHandles{store: store, closer: pgxPool.Close},
		metrics:      telemetry.New(),
		tenantsByOrg: map[string]*tenant{},
		sentAt:       map[string]sendRecord{},
		pending:      map[string][]time.Time{},
		latencies:    map[string][]float64{},
		delivered:    map[string]int64{},
	}

	key, err := secrets.GenerateKey()
	if err != nil {
		return nil, err
	}
	sealer, err := secrets.NewSealer([]string{key})
	if err != nil {
		return nil, err
	}
	payloadCodec, err := payload.NewCodec(512)
	if err != nil {
		return nil, err
	}
	tokens, err := auth.NewManager(auth.Config{Algorithm: "HS256", Secret: "bench", Issuer: "huxio"})
	if err != nil {
		return nil, err
	}

	var (
		orgRepo         = postgres.NewOrganizationRepo(store)
		appRepo         = postgres.NewApplicationRepo(store)
		endpointRepo    = postgres.NewEndpointRepo(store)
		eventTypeRepo   = postgres.NewEventTypeRepo(store)
		messageRepo     = postgres.NewMessageRepo(store)
		queueRepo       = postgres.NewQueueRepo(store)
		attemptRepo     = postgres.NewAttemptRepo(store)
		idempotencyRepo = postgres.NewIdempotencyRepo(store)
		snapshotRepo    = postgres.NewSnapshotRepo(store)
		notifications   = postgres.NewListener(pgxPool)
	)

	configManager := config.NewManager(snapshotRepo, notifications, sealer, h.metrics.ConfigSnapshotAge, config.ManagerOptions{
		Channel:         postgres.ConfigNotifyChannel,
		RefreshInterval: 5 * time.Second,
		Debounce:        20 * time.Millisecond,
	})

	// Tenants, applications, sinks and endpoints.
	appUC := usecases.NewApplicationUseCase(appRepo, snapshotRepo)
	endpointUC := usecases.NewEndpointUseCase(endpointRepo, sealer, snapshotRepo)

	for _, spec := range tenants {
		created, err := h.createTenant(ctx, orgRepo, appUC, endpointUC, tokens, spec)
		if err != nil {
			return nil, err
		}
		h.tenants = append(h.tenants, created)
		h.tenantsByOrg[created.orgID] = created
	}

	if err := configManager.Load(ctx); err != nil {
		return nil, err
	}

	ingestUC := usecases.NewIngestUseCase(store, messageRepo, queueRepo, configManager, payloadCodec, usecases.IngestConfig{
		MaxPayloadBytes:      4 << 20,
		DefaultRetentionDays: 1,
		Pool:                 "default",
	}, usecases.WithApplicationFallback(appRepo, configManager))
	messageUC := usecases.NewMessageUseCase(messageRepo, configManager, appRepo, configManager)
	attemptUC := usecases.NewAttemptUseCase(attemptRepo, endpointRepo, messageRepo, queueRepo, configManager, appRepo, configManager, "default")

	router := inboundhttp.NewRouter(inboundhttp.RouterDeps{
		Config: inboundhttp.RouterConfig{
			MaxPayloadBytes:  4 << 20,
			CORSAllowOrigins: []string{"*"},
			Pools:            []string{"default"},
		},
		Metrics:            h.metrics,
		TokenParser:        tokens,
		IdempotencyRepo:    idempotencyRepo,
		HealthHandler:      handlers.NewHealthHandler(store, configManager, "bench"),
		ApplicationHandler: handlers.NewApplicationHandler(appUC),
		EndpointHandler:    handlers.NewEndpointHandler(endpointUC, appUC),
		EventTypeHandler:   handlers.NewEventTypeHandler(usecases.NewEventTypeUseCase(eventTypeRepo, snapshotRepo)),
		MessageHandler:     handlers.NewMessageHandler(ingestUC, messageUC, h.metrics),
		AttemptHandler:     handlers.NewAttemptHandler(attemptUC),
		PortalTokenHandler: handlers.NewPortalHandler(usecases.NewPortalUseCase(tokens, configManager, appRepo, configManager, "http://bench", time.Hour)),
		AdminHandler: handlers.NewAdminHandler(usecases.NewAdminUseCase(
			queueRepo, postgres.NewLeaseRepo(store, entities.QueuePartitions), endpointRepo,
			configManager, appRepo, configManager, snapshotRepo,
		), []string{"default"}),
		StreamHandler: handlers.NewStreamHandler(attemptUC),
	})
	h.server = httptest.NewServer(router)

	// Workers. The delivery client trusts the sinks' certificates when the
	// scenario runs over TLS.
	guard, err := deliveryhttp.NewGuard(deliveryhttp.GuardOptions{AllowPrivate: true})
	if err != nil {
		return nil, err
	}
	// Over TLS the sinks serve a self-signed certificate, so the delivery
	// client is given the same trust store the sinks' own clients use.
	var tlsConfig *tls.Config
	if cfg.TLS && len(h.tenants) > 0 {
		tlsConfig = h.tenants[0].sink.TLSConfig()
	}
	deliveryClient, err := deliveryhttp.New(deliveryhttp.Options{
		RequestTimeout:    30 * time.Second,
		ResponseBodyLimit: 8 << 10,
		Guard:             guard,
		DNSCache:          deliveryhttp.NewDNSCache(deliveryhttp.DNSCacheOptions{}),
		TLSClientConfig:   tlsConfig,
	})
	if err != nil {
		return nil, err
	}

	h.runtime = &runtimeComponents{
		configManager:   configManager,
		payloadCodec:    payloadCodec,
		deliveryClient:  deliveryClient,
		attemptRepo:     attemptRepo,
		queueRepo:       queueRepo,
		messageRepo:     messageRepo,
		endpointRepo:    endpointRepo,
		leaseRepo:       postgres.NewLeaseRepo(store, entities.QueuePartitions),
		maintenanceRepo: postgres.NewMaintenanceRepo(store),
	}
	return h, nil
}

// runtimeComponents are the pieces the workers are built from, kept so a
// scenario can start and stop them.
type runtimeComponents struct {
	configManager   *config.Manager
	payloadCodec    *payload.Codec
	deliveryClient  dispatch.DeliveryClient
	attemptRepo     *postgres.AttemptRepo
	queueRepo       *postgres.QueueRepo
	messageRepo     *postgres.MessageRepo
	endpointRepo    *postgres.EndpointRepo
	leaseRepo       *postgres.LeaseRepo
	maintenanceRepo *postgres.MaintenanceRepo
}

// tenantSpec describes a tenant to create.
type tenantSpec struct {
	role      string
	sink      sinks.Kind
	delay     time.Duration
	failure   float64
	endpoints int
}

// createTenant makes an organization, an application, a sink and the endpoints
// pointing at it.
func (h *harness) createTenant(
	orgRepo context.Context,
	orgs *postgres.OrganizationRepo,
	appUC *usecases.ApplicationUseCase,
	endpointUC *usecases.EndpointUseCase,
	tokens *auth.Manager,
	spec tenantSpec,
) (*tenant, error) {
	now := time.Now().UTC()
	org := &entities.Organization{
		ID:        ids.New(ids.PrefixOrganization),
		Name:      "bench " + spec.role,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := orgs.CreateOrganization(orgRepo, org); err != nil {
		return nil, err
	}

	app, err := appUC.CreateApplication(orgRepo, usecases.CreateApplicationInput{
		OrgID: org.ID, Name: "bench app",
	})
	if err != nil {
		return nil, err
	}

	sink := sinks.New(sinks.Options{
		Kind:        spec.sink,
		Delay:       spec.delay,
		FailureRate: spec.failure,
		TLS:         h.cfg.TLS,
		Observer:    h.observeDelivery,
	})

	endpointCount := max(1, spec.endpoints)
	for range endpointCount {
		if _, _, err := endpointUC.CreateEndpoint(orgRepo, usecases.CreateEndpointInput{
			OrgID: org.ID, AppID: app.ID, URL: sink.URL(),
		}); err != nil {
			sink.Close()
			return nil, err
		}
	}

	token, _, err := tokens.IssueOrgToken(org.ID, time.Hour)
	if err != nil {
		sink.Close()
		return nil, err
	}

	return &tenant{
		orgID: org.ID, appID: app.ID, token: token,
		role: spec.role, sink: sink, endpoints: endpointCount,
	}, nil
}

// observeDelivery is called by every sink as a request arrives.
func (h *harness) observeDelivery(msgID string, at time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()

	record, ok := h.sentAt[msgID]
	if !ok {
		// Either the delivery beat the ingest response back to the sender, or
		// this is a retry of something from before a reset. Park it; recordSend
		// settles the first case and resetLatencies discards the second.
		h.pending[msgID] = append(h.pending[msgID], at)
		return
	}
	h.credit(record, at)
}

// recordSend remembers when a message was accepted, so the sink can compute
// the ingest-to-delivery latency, and credits any arrival that got here first.
func (h *harness) recordSend(orgID, msgID string, at time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	record := sendRecord{orgID: orgID, at: at}
	h.sentAt[msgID] = record
	if early, ok := h.pending[msgID]; ok {
		delete(h.pending, msgID)
		for _, arrived := range early {
			h.credit(record, arrived)
		}
	}
}

// credit records one delivery against its tenant. The caller holds h.mu.
func (h *harness) credit(record sendRecord, at time.Time) {
	h.latencies[record.orgID] = append(h.latencies[record.orgID], float64(at.Sub(record.at).Microseconds())/1000)
	h.delivered[record.orgID]++
}

// close tears the run down.
func (h *harness) close() {
	if h.server != nil {
		h.server.Close()
	}
	for _, t := range h.tenants {
		t.sink.Close()
	}
	if h.runtime != nil && h.runtime.payloadCodec != nil {
		h.runtime.payloadCodec.Close()
	}
	if h.db != nil && h.db.closer != nil {
		h.db.closer()
	}
}

// startWorkers runs the delivery engines and the attempt writer, returning a
// stop function that drains and flushes in the right order.
func (h *harness) startWorkers(ctx context.Context) func() {
	writer := dispatch.NewAttemptWriter(h.runtime.attemptRepo, h.runtime.queueRepo, dispatch.AttemptWriterOptions{
		BatchSize:     500,
		BufferSize:    16384,
		FlushInterval: 5 * time.Millisecond,
	}, h.metrics.AttemptBatchSize, h.metrics.AttemptQueueDepth)

	engineCtx, stopEngines := context.WithCancel(ctx)
	writerCtx, stopWriter := context.WithCancel(context.WithoutCancel(ctx))

	group := &errgroup.Group{}
	group.Go(func() error { return writer.Run(writerCtx) })

	for range max(1, h.cfg.Workers) {
		lanes := dispatch.NewLaneManager(dispatch.LaneManagerOptions{
			Defaults: dispatch.LaneOptions{
				InitialConcurrency: 8,
				MaxConcurrency:     64,
				Breaker:            dispatch.BreakerOptions{FailureThreshold: 20, Cooldown: 5 * time.Second},
			},
		})

		var policy *retry.Policy
		if len(h.cfg.RetrySchedule) > 0 {
			policy = retry.NewPolicy(retry.WithSchedule(h.cfg.RetrySchedule))
		}

		engine, err := dispatch.NewEngine(dispatch.EngineDeps{
			TaskQueue:           h.runtime.queueRepo,
			TxManager:           h.db.store,
			MessageStore:        postgres.NewMessageStore(h.runtime.messageRepo),
			PayloadCodec:        h.runtime.payloadCodec,
			SnapshotProvider:    h.runtime.configManager,
			DeliveryClient:      h.runtime.deliveryClient,
			AttemptSink:         writer,
			Policy:              policy,
			PartitionSource:     dispatch.NewStaticPartitions(),
			SnapshotInvalidator: h.runtime.configManager,
			LaneManager:         lanes,
			EndpointState:       h.runtime.endpointRepo,
			Metrics:             engineMetrics(h.metrics),
		}, dispatch.EngineOptions{
			WorkerID:       ids.New(ids.PrefixWorker),
			Pool:           "default",
			ClaimBatchSize: 100,
			MaxInflight:    2000,
			PollInterval:   25 * time.Millisecond,
			RequestTimeout: 30 * time.Second,
			LockTTL:        90 * time.Second,
		})
		if err != nil {
			logger.FromContext(ctx).Error().Err(err).Msg("failed to build a benchmark engine")
			continue
		}
		group.Go(func() error { return engine.Run(engineCtx, nil) })
	}

	// Keep the config snapshot fresh for the duration of the run.
	configCtx, stopConfig := context.WithCancel(ctx)
	group.Go(func() error { return h.runtime.configManager.Run(configCtx) })

	return func() {
		stopEngines()
		stopConfig()
		stopWriter()
		_ = group.Wait()
	}
}

// send posts one message and records when it was accepted.
func (h *harness) send(ctx context.Context, t *tenant, payloadBytes int) (time.Duration, error) {
	body := fmt.Sprintf(`{"eventType":"bench.event","payload":{"pad":%q}}`, strings.Repeat("x", max(0, payloadBytes)))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		h.server.URL+"/api/v1/app/"+t.appID+"/msg", strings.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+t.token)

	started := time.Now()
	resp, err := h.server.Client().Do(req)
	if err != nil {
		return time.Since(started), err
	}
	defer resp.Body.Close()

	elapsed := time.Since(started)
	if resp.StatusCode != http.StatusAccepted {
		return elapsed, eris.Errorf("ingest returned %d", resp.StatusCode)
	}

	var accepted struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&accepted); err != nil {
		return elapsed, err
	}
	h.recordSend(t.orgID, accepted.ID, started)
	return elapsed, nil
}

// truncate clears the tables between runs so one run's backlog is not another
// run's throughput.
func truncate(ctx context.Context, db *sql.DB) error {
	const statement = `
		TRUNCATE delivery_task, delivery_attempt, message, endpoint, application,
		         event_type, organization, partition_lease, named_lease,
		         worker_registry, idempotency_key`
	if _, err := db.ExecContext(ctx, statement); err != nil {
		return eris.Wrap(err, "truncate benchmark tables")
	}
	return nil
}

// redactDSN reduces a DSN to host and database name so a log line can say
// what is about to be wiped without printing the password.
func redactDSN(dsn string) string {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "unparseable DSN"
	}
	return parsed.Host + parsed.Path
}

// engineMetrics adapts the collectors to the engine's ports.
func engineMetrics(metrics *telemetry.Metrics) dispatch.EngineMetrics {
	return dispatch.EngineMetrics{
		DeliveryDuration: func(org, outcome string, seconds float64) {
			metrics.DeliveryDuration.WithLabelValues(org, outcome).Observe(seconds)
		},
		InternalDuration: func(org string, seconds float64) {
			metrics.DeliveryInternal.WithLabelValues(org).Observe(seconds)
		},
		DeliveriesTotal: func(org, outcome, statusClass string) {
			metrics.DeliveriesTotal.WithLabelValues(org, outcome, statusClass).Inc()
		},
		ClaimBatchSize: metrics.TaskClaimBatch.Observe,
		InflightTotal:  metrics.LaneInflightTotal.Set,
	}
}

// memStats reads the allocation counters the report divides by deliveries.
func memStats() runtime.MemStats {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats
}
