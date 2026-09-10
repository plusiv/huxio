// Package telemetry owns the Prometheus collectors for the whole binary. The
// per-tenant histograms carry their weight: without a per-org breakdown,
// "one noisy tenant does not slow the others down" is not something you can
// show from the outside.
package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Namespace prefixes every metric this binary exports.
const Namespace = "huxio"

// Outcome labels used on delivery metrics.
const (
	OutcomeSuccess = "success"
	OutcomeFailure = "failure"
	OutcomeError   = "error"
	OutcomeSkipped = "skipped"
)

// Metrics is the registered collector set. One instance is built at the
// composition root and passed to the components that record into it.
type Metrics struct {
	registry *prometheus.Registry

	IngestDuration       *prometheus.HistogramVec
	DeliveryDuration     *prometheus.HistogramVec
	DeliveryInternal     *prometheus.HistogramVec
	DeliveriesTotal      *prometheus.CounterVec
	QueueLagSeconds      *prometheus.GaugeVec
	QueueDepth           *prometheus.GaugeVec
	TaskClaimBatch       prometheus.Histogram
	LaneConcurrency      *prometheus.GaugeVec
	LaneInflightTotal    prometheus.Gauge
	BreakerState         *prometheus.GaugeVec
	EndpointsQuarantined prometheus.Gauge
	AttemptBatchSize     prometheus.Histogram
	AttemptQueueDepth    prometheus.Gauge
	HTTPConnReusedRatio  prometheus.Gauge
	DNSCacheHits         prometheus.Counter
	DNSCacheMisses       prometheus.Counter
	PartitionLeasesOwned prometheus.Gauge
	ConfigSnapshotAge    prometheus.Gauge
	APIRequestDuration   *prometheus.HistogramVec
}

// New builds and registers the collector set on a private registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		registry: reg,

		IngestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "ingest_duration_seconds",
			Help:      "Duration of accepted message ingest requests, per organization.",
			Buckets:   []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
		}, []string{"org"}),

		DeliveryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "delivery_duration_seconds",
			Help:      "Wall-clock duration of one outbound delivery attempt, including endpoint time.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"org", "outcome"}),

		DeliveryInternal: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "delivery_internal_seconds",
			Help:      "Time from task claim to request bytes written, excluding endpoint response time.",
			Buckets:   []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.05, 0.1},
		}, []string{"org"}),

		DeliveriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "deliveries_total",
			Help:      "Delivery attempts by organization, outcome and HTTP status class.",
		}, []string{"org", "outcome", "status_class"}),

		QueueLagSeconds: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "queue_lag_seconds",
			Help:      "Age of the oldest ready task per pool.",
		}, []string{"pool"}),

		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "queue_depth",
			Help:      "Number of queue rows per pool and state (ready, delayed, locked).",
		}, []string{"pool", "state"}),

		TaskClaimBatch: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "task_claim_batch_size",
			Help:      "Rows returned by one claim query.",
			Buckets:   []float64{0, 1, 5, 10, 25, 50, 100, 200},
		}),

		LaneConcurrency: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "lane_concurrency",
			Help:      "Current AIMD concurrency per sampled endpoint lane (top-N by volume only).",
		}, []string{"endpoint"}),

		LaneInflightTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "lane_inflight_total",
			Help:      "Deliveries in flight across every lane on this worker.",
		}),

		BreakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "breaker_state",
			Help:      "Number of endpoint circuit breakers in each state.",
		}, []string{"state"}),

		EndpointsQuarantined: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "endpoints_quarantined",
			Help:      "Endpoints currently assigned to the quarantine pool.",
		}),

		AttemptBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "attempt_writer_batch_size",
			Help:      "Attempt records per COPY flush.",
			Buckets:   []float64{1, 10, 50, 100, 250, 500, 1000},
		}),

		AttemptQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "attempt_writer_queue_depth",
			Help:      "Attempt records buffered but not yet flushed. Sustained non-zero means Postgres is the bottleneck.",
		}),

		HTTPConnReusedRatio: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "http_conn_reused_ratio",
			Help:      "Fraction of outbound requests that reused a warm connection.",
		}),

		DNSCacheHits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "dns_cache_hits_total",
			Help:      "Resolver lookups served from the in-process DNS cache.",
		}),

		DNSCacheMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "dns_cache_misses_total",
			Help:      "Resolver lookups that reached the system resolver.",
		}),

		PartitionLeasesOwned: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "partition_leases_owned",
			Help:      "Queue partitions currently leased by this worker.",
		}),

		ConfigSnapshotAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "config_snapshot_age_seconds",
			Help:      "Age of the in-memory config snapshot. Above 300 means LISTEN died.",
		}),

		APIRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "api_request_duration_seconds",
			Help:      "Duration of API requests by route and status class.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		}, []string{"method", "route", "status_class"}),
	}

	reg.MustRegister(
		m.IngestDuration,
		m.DeliveryDuration,
		m.DeliveryInternal,
		m.DeliveriesTotal,
		m.QueueLagSeconds,
		m.QueueDepth,
		m.TaskClaimBatch,
		m.LaneConcurrency,
		m.LaneInflightTotal,
		m.BreakerState,
		m.EndpointsQuarantined,
		m.AttemptBatchSize,
		m.AttemptQueueDepth,
		m.HTTPConnReusedRatio,
		m.DNSCacheHits,
		m.DNSCacheMisses,
		m.PartitionLeasesOwned,
		m.ConfigSnapshotAge,
		m.APIRequestDuration,
	)

	return m
}

// Registry exposes the private registry so the HTTP adapter can serve /metrics.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// StatusClass maps an HTTP status code onto the low-cardinality class label
// used by the delivery and API metrics. Zero means no response was received.
func StatusClass(code int) string {
	switch {
	case code == 0:
		return "none"
	case code < 200:
		return "1xx"
	case code < 300:
		return "2xx"
	case code < 400:
		return "3xx"
	case code < 500:
		return "4xx"
	default:
		return "5xx"
	}
}
