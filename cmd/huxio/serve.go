package main

import (
	"cmp"
	"context"
	"errors"
	"net/http"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/plusiv/huxio/configs"
	inboundhttp "github.com/plusiv/huxio/internal/adapters/inbound/http"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/handlers"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/portal"
	"github.com/plusiv/huxio/internal/adapters/inbound/worker"
	"github.com/plusiv/huxio/internal/adapters/outbound/deliveryhttp"
	"github.com/plusiv/huxio/internal/adapters/outbound/persistence/postgres"
	"github.com/plusiv/huxio/internal/application/config"
	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/application/maintenance"
	"github.com/plusiv/huxio/internal/application/nodes"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/signing"
	"github.com/plusiv/huxio/internal/infrastructure/auth"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/plusiv/huxio/internal/infrastructure/payload"
	"github.com/plusiv/huxio/internal/infrastructure/secrets"
	"github.com/plusiv/huxio/internal/infrastructure/telemetry"
	"github.com/rotisserie/eris"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func newServeCmd() *cobra.Command {
	var (
		role  string
		pools []string
	)

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the API, the delivery workers, or both",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := configs.Load()
			if err != nil {
				return err
			}
			// Flags win over the environment, so a deployment can run the same
			// image with different roles.
			if cmd.Flags().Changed("role") {
				cfg.Role = configs.Role(role)
			}
			if cmd.Flags().Changed("pools") {
				cfg.Pools = pools
			}
			if err := cfg.Validate(); err != nil {
				return err
			}
			return serve(cmd.Context(), cfg)
		},
	}

	cmd.Flags().StringVar(&role, "role", string(configs.RoleAll), "process role: all, api or worker")
	cmd.Flags().StringSliceVar(&pools, "pools", []string{configs.DefaultPool}, "worker pools this process serves")
	return cmd
}

// serve builds the composition root and runs the selected roles until the
// process is signalled.
func serve(ctx context.Context, cfg *configs.Config) error {
	log := logger.Init(cfg.LogLevel)
	ctx = logger.Context(ctx, log)
	log.Info().
		Str("role", string(cfg.Role)).
		Strs("pools", cfg.Pools).
		Str("version", version).
		Msg("starting huxio")

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metrics := telemetry.New()

	sealer, err := secrets.NewSealer(cfg.EncryptionKeys)
	if err != nil {
		return eris.Wrap(err, "HUXIO_ENCRYPTION_KEY is required; generate one with: huxio keygen")
	}
	codec, err := payload.NewCodec(cfg.CompressionMinBytes)
	if err != nil {
		return err
	}
	defer codec.Close()

	tokens, err := auth.NewManager(auth.Config{
		Algorithm:     cfg.JWTAlgorithm,
		Secret:        cfg.JWTSecret,
		PrivateKeyPEM: cfg.JWTPrivateKeyPEM,
		PublicKeyPEM:  cfg.JWTPublicKeyPEM,
		Issuer:        cfg.JWTIssuer,
	})
	if err != nil {
		return err
	}

	pgxPool, err := postgres.NewPool(ctx, postgres.PoolConfig{
		DSN:      cfg.DatabaseURL,
		MaxConns: cfg.DBMaxConns,
		MinConns: cfg.DBMinConns,
	})
	if err != nil {
		return err
	}
	defer pgxPool.Close()

	store := postgres.NewStore(pgxPool)
	var (
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

	configManager := config.NewManager(snapshotRepo, notifications, sealer, metrics.ConfigSnapshotAge, config.ManagerOptions{
		Channel:         postgres.ConfigNotifyChannel,
		RefreshInterval: cfg.ConfigRefreshInterval,
	})
	if err := configManager.Load(ctx); err != nil {
		return err
	}

	ingestUC := usecases.NewIngestUseCase(store, messageRepo, queueRepo, configManager, codec, usecases.IngestConfig{
		MaxPayloadBytes:      cfg.MaxPayloadBytes,
		DefaultRetentionDays: cfg.RetentionMessagesDays,
		Pool:                 configs.DefaultPool,
	}, usecases.WithApplicationFallback(appRepo, configManager))
	messageUC := usecases.NewMessageUseCase(messageRepo, configManager, appRepo, configManager)
	appUC := usecases.NewApplicationUseCase(appRepo, snapshotRepo)
	attemptUC := usecases.NewAttemptUseCase(
		attemptRepo, endpointRepo, messageRepo, queueRepo,
		configManager, appRepo, configManager, configs.DefaultPool,
	)
	portalUC := usecases.NewPortalUseCase(tokens, configManager, appRepo, configManager, cfg.PortalBaseURL, cfg.PortalTokenTTL)
	// Operational webhooks ride the normal ingest path, so a worker needs the
	// ingest use case too.
	operationalUC := usecases.NewOperationalUseCase(ingestUC, appUC)
	adminUC := usecases.NewAdminUseCase(
		queueRepo, postgres.NewLeaseRepo(store, configs.QueuePartitions), endpointRepo,
		configManager, appRepo, configManager, snapshotRepo,
	)
	endpointUC := usecases.NewEndpointUseCase(endpointRepo, sealer, snapshotRepo)
	eventTypeUC := usecases.NewEventTypeUseCase(eventTypeRepo, snapshotRepo)

	group, groupCtx := errgroup.WithContext(ctx)

	// The snapshot refresher runs in every role: workers read config from it
	// on the delivery path.
	group.Go(func() error { return configManager.Run(groupCtx) })

	// Every role serves health and metrics; the API role serves the rest on
	// top of them.
	health := handlers.NewHealthHandler(store, configManager, version)

	var router http.Handler
	if cfg.RunsAPI() {
		// API nodes register among their peers so each can enforce its slice
		// of a cluster-wide rate limit locally.
		nodeRegistry, err := nodes.New(postgres.NewLeaseRepo(store, configs.QueuePartitions), nodes.Options{
			NodeID:    ids.New(ids.PrefixWorker),
			Pool:      nodes.PoolAPI,
			Version:   version,
			Heartbeat: cfg.LeaseHeartbeat,
			TTL:       cfg.LeaseTTL,
		})
		if err != nil {
			return err
		}
		group.Go(func() error { return nodeRegistry.Run(groupCtx) })

		portalHandler, err := portal.New(endpointUC, attemptUC, eventTypeUC, ingestUC,
			strings.HasPrefix(cfg.PortalBaseURL, "https://"))
		if err != nil {
			return err
		}

		router = inboundhttp.NewRouter(inboundhttp.RouterDeps{
			Config: inboundhttp.RouterConfig{
				MaxPayloadBytes:    cfg.MaxPayloadBytes,
				CORSAllowOrigins:   cfg.CORSAllowOrigins,
				RateLimitOrgRPS:    cfg.RateLimitOrgRPS,
				RateLimitAppRPS:    cfg.RateLimitAppRPS,
				Pools:              cfg.Pools,
				PortalSecureCookie: strings.HasPrefix(cfg.PortalBaseURL, "https://"),
			},
			PortalUI:           portalHandler,
			Metrics:            metrics,
			TokenParser:        tokens,
			IdempotencyRepo:    idempotencyRepo,
			NodeCounter:        nodeRegistry,
			HealthHandler:      health,
			ApplicationHandler: handlers.NewApplicationHandler(appUC),
			EndpointHandler:    handlers.NewEndpointHandler(endpointUC, appUC),
			EventTypeHandler:   handlers.NewEventTypeHandler(eventTypeUC),
			MessageHandler:     handlers.NewMessageHandler(ingestUC, messageUC, metrics),
			AttemptHandler:     handlers.NewAttemptHandler(attemptUC),
			PortalTokenHandler: handlers.NewPortalHandler(portalUC),
			AdminHandler:       handlers.NewAdminHandler(adminUC, cfg.Pools),
			StreamHandler:      handlers.NewStreamHandler(attemptUC),
		})
	} else {
		router = inboundhttp.NewTelemetryRouter(metrics, health)
	}

	{
		server := &http.Server{
			Addr:              ":" + cfg.Port,
			Handler:           router,
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       120 * time.Second,
		}

		group.Go(func() error {
			log.Info().Str("addr", server.Addr).Str("role", string(cfg.Role)).Msg("http listening")
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return eris.Wrap(err, "http server")
			}
			return nil
		})

		group.Go(func() error {
			<-groupCtx.Done()

			// Fail readiness first and only then stop accepting. The load
			// balancer needs a health check cycle to notice; tearing the
			// listener down before it does means requests it is still
			// sending arrive at a closed socket.
			health.BeginShutdown()

			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if err := server.Shutdown(shutdownCtx); err != nil {
				log.Warn().Err(err).Msg("http shutdown timed out")
			}
			log.Info().Msg("http drained")
			return nil
		})
	}

	if cfg.RunsWorker() {
		workerID := ids.New(ids.PrefixWorker)

		guard, err := deliveryhttp.NewGuard(deliveryhttp.GuardOptions{AllowSubnets: cfg.AllowSubnets})
		if err != nil {
			return err
		}
		dnsCache := deliveryhttp.NewDNSCache(deliveryhttp.DNSCacheOptions{
			OnHit:  metrics.DNSCacheHits.Inc,
			OnMiss: metrics.DNSCacheMisses.Inc,
		})
		client, err := deliveryhttp.New(deliveryhttp.Options{
			RequestTimeout:    cfg.WorkerRequestTimeout,
			ResponseBodyLimit: cfg.AttemptBodyLimitBytes,
			Guard:             guard,
			DNSCache:          dnsCache,
		})
		if err != nil {
			return err
		}
		defer client.CloseIdleConnections()

		attemptWriter := dispatch.NewAttemptWriter(
			attemptRepo,
			queueRepo,
			dispatch.AttemptWriterOptions{
				BufferSize:    cfg.AttemptWriterBufferSize,
				BatchSize:     cfg.AttemptWriterBatchSize,
				FlushInterval: cfg.AttemptWriterFlushInterval,
			},
			metrics.AttemptBatchSize,
			metrics.AttemptQueueDepth,
		)

		leaseRepo := postgres.NewLeaseRepo(store, configs.QueuePartitions)

		pools := make([]worker.Pool, 0, len(cfg.Pools))
		laneManagers := make([]*dispatch.LaneManager, 0, len(cfg.Pools))

		for _, pool := range cfg.Pools {
			// Each pool gets its own lease manager, so a worker serving both default
			// and quarantine claims a fair share of each.
			leaseManager, err := dispatch.NewLeaseManager(leaseRepo, metrics.PartitionLeasesOwned, dispatch.LeaseManagerOptions{
				WorkerID:   workerID,
				Pool:       pool,
				Partitions: configs.QueuePartitions,
				TTL:        cfg.LeaseTTL,
				Heartbeat:  cfg.LeaseHeartbeat,
			})
			if err != nil {
				return err
			}

			lanes := dispatch.NewLaneManager(dispatch.LaneManagerOptions{
				Defaults: dispatch.LaneOptions{
					InitialConcurrency: cfg.LaneInitialConcurrency,
					MaxConcurrency:     cfg.LaneMaxConcurrency,
					Breaker: dispatch.BreakerOptions{
						FailureThreshold: cfg.BreakerFailureThreshold,
						Cooldown:         cfg.BreakerCooldown,
						MaxCooldown:      cfg.BreakerMaxCooldown,
					},
				},
				IdleEviction: cfg.LaneIdleEviction,
			})
			laneManagers = append(laneManagers, lanes)

			engine, err := dispatch.NewEngine(dispatch.EngineDeps{
				TaskQueue:           queueRepo,
				TxManager:           store,
				MessageStore:        postgres.NewMessageStore(messageRepo),
				PayloadCodec:        codec,
				SnapshotProvider:    configManager,
				DeliveryClient:      client,
				AttemptSink:         attemptWriter,
				PartitionSource:     leaseManager.Partitions(),
				SnapshotInvalidator: configManager,
				LaneManager:         lanes,
				EndpointState:       endpointRepo,
				Operational:         operationalUC,
				Metrics:             engineMetrics(metrics),
			}, dispatch.EngineOptions{
				WorkerID:          workerID,
				Pool:              pool,
				ClaimBatchSize:    cfg.ClaimBatchSize,
				MaxInflight:       cfg.WorkerMaxInflight,
				LockTTL:           cfg.TaskLockTTL,
				PollInterval:      cfg.QueuePollInterval,
				LaneWaitTimeout:   cfg.LaneWaitTimeout,
				LaneRequeueDelay:  cfg.LaneRequeueDelay,
				RequestTimeout:    cfg.WorkerRequestTimeout,
				ResponseBodyLimit: cfg.AttemptBodyLimitBytes,
				CompatHeaders:     signing.HeaderScheme(cfg.CompatHeaders),
				QuarantinePool:    configs.QuarantinePool,
				QuarantineAfter:   cfg.QuarantineAfter,
				DisableAfter:      cfg.EndpointFailureDisableAfter,
			})
			if err != nil {
				return err
			}

			pools = append(pools, worker.Pool{Name: pool, Engine: engine, LeaseManager: leaseManager})
		}

		maintenanceLoop, err := maintenance.New(maintenance.Deps{
			LeaseRepo:       leaseRepo,
			QueueRepo:       queueRepo,
			PartitionRepo:   postgres.NewMaintenanceRepo(store),
			IdempotencyRepo: idempotencyRepo,
			Metrics: maintenance.Metrics{
				QueueLag: func(pool string, seconds float64) {
					metrics.QueueLagSeconds.WithLabelValues(pool).Set(seconds)
				},
				QueueDepth: func(pool, state string, count float64) {
					metrics.QueueDepth.WithLabelValues(pool, state).Set(count)
				},
			},
			Hooks: maintenance.Hooks{
				// Lane eviction is process-local, so it runs on every worker rather than
				// only on the maintenance leader.
				OnTick: func(_ context.Context) {
					reportLaneMetrics(metrics, laneManagers, cfg.QuarantineAfter)
				},
			},
		}, maintenance.Options{
			OwnerID:              workerID,
			Pools:                cfg.Pools,
			LeaseTTL:             cfg.LeaseTTL,
			PartitionsAhead:      cfg.PartitionsAheadDays,
			MessageRetentionDays: cfg.RetentionMessagesDays,
			AttemptRetentionDays: cfg.RetentionAttemptsDays,
		})
		if err != nil {
			return err
		}

		runner, err := worker.New(worker.Deps{
			Pools:           pools,
			AttemptWriter:   attemptWriter,
			RegistryRepo:    leaseRepo,
			Notifications:   notifications,
			MaintenanceLoop: maintenanceLoop,
		}, worker.Options{
			WorkerID:          workerID,
			Pools:             cfg.Pools,
			Version:           version,
			TaskChannel:       postgres.TaskNotifyChannel,
			HeartbeatInterval: cfg.LeaseHeartbeat,
			LeaseTTL:          cfg.LeaseTTL,
		})
		if err != nil {
			return err
		}

		group.Go(func() error { return runner.Run(groupCtx) })
	}

	if err := group.Wait(); err != nil {
		return err
	}
	log.Info().Msg("shutdown complete")
	return nil
}

// laneMetricSampleSize bounds the per-endpoint gauge. Labelling every endpoint
// takes down Prometheus before it takes down the service, so only the busiest
// lanes are exported.
const laneMetricSampleSize = 50

// reportLaneMetrics evicts idle lanes and publishes the lane and breaker
// metrics for one tick. It runs on every worker, because lanes are per
// process.
func reportLaneMetrics(metrics *telemetry.Metrics, managers []*dispatch.LaneManager, quarantineAfter time.Duration) {
	breakers := map[dispatch.BreakerState]int{
		dispatch.BreakerClosed:   0,
		dispatch.BreakerOpen:     0,
		dispatch.BreakerHalfOpen: 0,
	}
	quarantined := 0
	stats := make([]dispatch.LaneStats, 0, 256)

	for _, lanes := range managers {
		lanes.EvictIdle()
		for state, count := range lanes.BreakerCounts() {
			breakers[state] += count
		}
		for _, stat := range lanes.Stats() {
			if quarantineAfter > 0 && stat.OpenFor >= quarantineAfter {
				quarantined++
			}
			stats = append(stats, stat)
		}
	}

	for state, count := range breakers {
		metrics.BreakerState.WithLabelValues(state.String()).Set(float64(count))
	}
	metrics.EndpointsQuarantined.Set(float64(quarantined))
	metrics.LaneInflightTotal.Set(float64(totalInFlight(stats)))

	// Busiest first, and only the top slice is labelled by endpoint. The vec
	// is reset each tick so an evicted lane's series does not linger.
	slices.SortFunc(stats, func(a, b dispatch.LaneStats) int {
		if c := cmp.Compare(b.InFlight, a.InFlight); c != 0 {
			return c
		}
		return cmp.Compare(b.Concurrency, a.Concurrency)
	})
	metrics.LaneConcurrency.Reset()
	for _, stat := range stats[:min(len(stats), laneMetricSampleSize)] {
		metrics.LaneConcurrency.WithLabelValues(stat.EndpointID).Set(float64(stat.Concurrency))
	}
}

func totalInFlight(stats []dispatch.LaneStats) int {
	total := 0
	for _, stat := range stats {
		total += stat.InFlight
	}
	return total
}

// engineMetrics adapts the Prometheus collectors to the engine's metric ports,
// keeping the metrics library out of the application layer.
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
