package integration_test

import (
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/plusiv/huxio/configs"
	"github.com/plusiv/huxio/internal/adapters/outbound/deliveryhttp"
	"github.com/plusiv/huxio/internal/adapters/outbound/persistence/postgres"
	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/domain/retry"
	"github.com/plusiv/huxio/internal/domain/signing"
	"github.com/plusiv/huxio/internal/infrastructure/payload"
)

// receivedRequest is one webhook a test sink received.
type receivedRequest struct {
	Headers http.Header
	Body    []byte
}

// sink is a local receiver that records requests and answers with a scripted
// status.
type sink struct {
	server *httptest.Server

	mu       sync.Mutex
	received []receivedRequest
	status   int
	delay    time.Duration
	headers  map[string]string
	signal   chan struct{}
}

func newSink(t *testing.T, status int) *sink {
	t.Helper()

	s := &sink{status: status, signal: make(chan struct{}, 64)}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		s.mu.Lock()
		s.received = append(s.received, receivedRequest{Headers: r.Header.Clone(), Body: body})
		status, delay, headers := s.status, s.delay, s.headers
		s.mu.Unlock()

		select {
		case s.signal <- struct{}{}:
		default:
		}

		if delay > 0 {
			time.Sleep(delay)
		}
		for name, value := range headers {
			w.Header().Set(name, value)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte("received"))
	}))
	t.Cleanup(s.server.Close)
	return s
}

func (s *sink) setStatus(status int, headers map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status, s.headers = status, headers
}

func (s *sink) requests() []receivedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]receivedRequest(nil), s.received...)
}

// waitFor blocks until n requests have arrived.
func (s *sink) waitFor(n int, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		s.mu.Lock()
		count := len(s.received)
		s.mu.Unlock()
		if count >= n {
			return true
		}
		select {
		case <-s.signal:
		case <-deadline:
			return false
		}
	}
}

// workerEnv is a real delivery engine over the real repositories.
type workerEnv struct {
	*apiEnv

	engine *dispatch.Engine
	writer *dispatch.AttemptWriter
	lanes  *dispatch.LaneManager
	codec  *payload.Codec
	client dispatch.DeliveryClient
	opts   workerOptions
}

type workerOptions struct {
	requestTimeout time.Duration
	batchSize      int
	flushInterval  time.Duration
	policy         *retry.Policy

	// Isolation settings. Zero values keep lanes and health transitions off,
	// which is the phase-3 behaviour the earlier tests assert.
	failingNotifyAttempt int16
	laneConcurrency      int
	breakerThreshold     int
	breakerCooldown      time.Duration
	quarantineAfter      time.Duration
	disableAfter         time.Duration
	laneWaitTimeout      time.Duration
	laneRequeueDelay     time.Duration
	pool                 string
}

func newWorkerEnv(t *testing.T, api *apiEnv, opts workerOptions) *workerEnv {
	t.Helper()

	if opts.requestTimeout <= 0 {
		opts.requestTimeout = 5 * time.Second
	}
	if opts.batchSize <= 0 {
		opts.batchSize = 1
	}
	if opts.flushInterval <= 0 {
		opts.flushInterval = 5 * time.Millisecond
	}

	codec, err := payload.NewCodec(512)
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}
	t.Cleanup(codec.Close)

	// The sinks run on loopback, which the SSRF guard blocks by default; the
	// tests opt in explicitly rather than weakening the guard.
	guard, err := deliveryhttp.NewGuard(deliveryhttp.GuardOptions{AllowPrivate: true})
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}
	client, err := deliveryhttp.New(deliveryhttp.Options{
		RequestTimeout: opts.requestTimeout,
		Guard:          guard,
		DNSCache:       deliveryhttp.NewDNSCache(deliveryhttp.DNSCacheOptions{}),
	})
	if err != nil {
		t.Fatalf("deliveryhttp.New: %v", err)
	}
	t.Cleanup(client.CloseIdleConnections)

	var lanes *dispatch.LaneManager
	if opts.laneConcurrency > 0 || opts.breakerThreshold > 0 {
		if opts.laneConcurrency <= 0 {
			opts.laneConcurrency = 8
		}
		if opts.breakerThreshold <= 0 {
			opts.breakerThreshold = 20
		}
		if opts.breakerCooldown <= 0 {
			opts.breakerCooldown = 30 * time.Second
		}
		lanes = dispatch.NewLaneManager(dispatch.LaneManagerOptions{
			Defaults: dispatch.LaneOptions{
				InitialConcurrency: opts.laneConcurrency,
				MaxConcurrency:     opts.laneConcurrency,
				Breaker: dispatch.BreakerOptions{
					FailureThreshold: opts.breakerThreshold,
					Cooldown:         opts.breakerCooldown,
				},
			},
		})
	}

	writer := dispatch.NewAttemptWriter(api.Attempts, api.Queue, dispatch.AttemptWriterOptions{
		BatchSize:     opts.batchSize,
		BufferSize:    1024,
		FlushInterval: opts.flushInterval,
	}, api.Metrics.AttemptBatchSize, api.Metrics.AttemptQueueDepth)

	engine, err := dispatch.NewEngine(dispatch.EngineDeps{
		TaskQueue:           api.Queue,
		TxManager:           api.Store,
		MessageStore:        postgres.NewMessageStore(api.Messages),
		PayloadCodec:        codec,
		SnapshotProvider:    api.Config,
		DeliveryClient:      client,
		AttemptSink:         writer,
		Policy:              opts.policy,
		PartitionSource:     dispatch.NewStaticPartitions(),
		SnapshotInvalidator: api.Config,
		LaneManager:         lanes,
		EndpointState:       api.Endpoints,
	}, dispatch.EngineOptions{
		WorkerID:         "wkr_integration",
		Pool:             cmp.Or(opts.pool, "default"),
		ClaimBatchSize:   50,
		MaxInflight:      16,
		PollInterval:     10 * time.Millisecond,
		RequestTimeout:   opts.requestTimeout,
		LockTTL:          opts.requestTimeout + time.Minute,
		LaneWaitTimeout:  cmp.Or(opts.laneWaitTimeout, 100*time.Millisecond),
		LaneRequeueDelay: cmp.Or(opts.laneRequeueDelay, 50*time.Millisecond),
		QuarantinePool:   configs.QuarantinePool,
		QuarantineAfter:  opts.quarantineAfter,
		DisableAfter:     opts.disableAfter,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	return &workerEnv{
		apiEnv: api, engine: engine, writer: writer, lanes: lanes,
		codec: codec, client: client, opts: opts,
	}
}

// start runs the engine and the writer, and returns a stop function that
// performs the ordered shutdown: drain deliveries, then flush attempts.
func (w *workerEnv) start(t *testing.T) func() {
	t.Helper()

	engineCtx, stopEngine := context.WithCancel(context.Background())
	writerCtx, stopWriter := context.WithCancel(context.Background())

	engineDone := make(chan error, 1)
	writerDone := make(chan error, 1)
	go func() { engineDone <- w.engine.Run(engineCtx, nil) }()
	go func() { writerDone <- w.writer.Run(writerCtx) }()

	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true

		stopEngine()
		select {
		case err := <-engineDone:
			if err != nil {
				t.Errorf("engine Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("engine did not drain")
		}

		stopWriter()
		select {
		case err := <-writerDone:
			if err != nil {
				t.Errorf("writer Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("attempt writer did not flush")
		}
	}
}

// ingest posts a message through the API and returns its id.
func (w *workerEnv) ingest(t *testing.T, appID, eventType string, body map[string]any) string {
	t.Helper()

	resp := w.do(t, request{
		Method: http.MethodPost,
		Path:   "/api/v1/app/" + appID + "/msg",
		Body:   map[string]any{"eventType": eventType, "payload": body},
	})
	if resp.Status != http.StatusAccepted {
		t.Fatalf("ingest: %d %s", resp.Status, resp.Body)
	}
	id, _ := resp.field(t, "id").(string)
	return id
}

// waitForAttempts polls until the expected number of attempt rows exist.
func (e *testEnv) waitForAttempts(t *testing.T, msgID string, want int, timeout time.Duration) []*entities.DeliveryAttempt {
	t.Helper()

	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for {
		result, err := e.Attempts.GetAttempts(ctx, repositories.AttemptFilters{
			MsgID: repositories.Eq(msgID),
		}, repositories.CursorPagination{Limit: 50})
		if err != nil {
			t.Fatalf("GetAttempts: %v", err)
		}
		if len(result.Items) >= want {
			return result.Items
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d attempt rows for %s, found %d", want, msgID, len(result.Items))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *testEnv) countTasks(t *testing.T) int {
	t.Helper()
	var count int
	if err := e.Pool.QueryRow(context.Background(), `SELECT count(*) FROM delivery_task`).Scan(&count); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	return count
}

func TestDeliveryEndToEnd(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusOK)

	appID := api.createApp(t, "Delivery", nil)
	endpointID, secret := api.createEndpoint(t, appID, map[string]any{
		"url":         receiver.server.URL,
		"filterTypes": []string{"invoice.paid"},
		"headers":     map[string]string{"X-Tenant": "acme"},
	})

	worker := newWorkerEnv(t, api, workerOptions{})
	stop := worker.start(t)
	defer stop()

	msgID := worker.ingest(t, appID, "invoice.paid", map[string]any{"amount": 100, "currency": "EUR"})

	if !receiver.waitFor(1, 10*time.Second) {
		t.Fatal("the endpoint never received the webhook")
	}

	got := receiver.requests()[0]

	// The payload arrives byte for byte as the sender wrote it.
	if !strings.Contains(string(got.Body), `"currency":"EUR"`) {
		t.Errorf("body = %s", got.Body)
	}
	// Custom endpoint headers are applied.
	if got.Headers.Get("X-Tenant") != "acme" {
		t.Errorf("custom header missing: %v", got.Headers)
	}
	if got.Headers.Get("User-Agent") == "" {
		t.Error("requests must identify themselves")
	}

	// The signature is exactly what an off-the-shelf receiver verifies.
	verifySignature(t, got, msgID, secret)

	// The attempt is recorded and the queue drains.
	attempts := api.waitForAttempts(t, msgID, 1, 10*time.Second)
	attempt := attempts[0]
	if attempt.Status != entities.AttemptSucceeded {
		t.Errorf("status = %v, want succeeded", attempt.Status)
	}
	if attempt.ResponseStatusCode != http.StatusOK {
		t.Errorf("response status = %d", attempt.ResponseStatusCode)
	}
	if attempt.ResponseBody != "received" {
		t.Errorf("response body = %q", attempt.ResponseBody)
	}
	if attempt.EndpointID != endpointID || attempt.AttemptNumber != 1 {
		t.Errorf("attempt = %+v", attempt)
	}
	if attempt.ResponseDurationMS < 0 {
		t.Errorf("duration = %d", attempt.ResponseDurationMS)
	}

	deadline := time.Now().Add(5 * time.Second)
	for api.countTasks(t) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d queue rows remain after a successful delivery", api.countTasks(t))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// verifySignature reproduces what a Standard Webhooks receiver library does.
func verifySignature(t *testing.T, got receivedRequest, msgID, secret string) {
	t.Helper()

	if got.Headers.Get(signing.HeaderStandardID) != msgID {
		t.Errorf("webhook-id = %q, want %q", got.Headers.Get(signing.HeaderStandardID), msgID)
	}

	timestamp := got.Headers.Get(signing.HeaderStandardTimestamp)
	unix, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		t.Fatalf("webhook-timestamp = %q, want unix seconds", timestamp)
	}
	if skew := time.Since(time.Unix(unix, 0)); skew > time.Minute || skew < -time.Minute {
		t.Errorf("webhook-timestamp is %s away from now", skew)
	}

	mac := hmac.New(sha256.New, []byte(strings.TrimPrefix(secret, "whsec_")))
	mac.Write([]byte(msgID + "." + timestamp + "."))
	mac.Write(got.Body)
	want := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))

	signatures := strings.Split(got.Headers.Get(signing.HeaderStandardSignature), " ")
	for _, signature := range signatures {
		if signature == want {
			return
		}
	}
	t.Errorf("no signature in %v matches the receiver's computation %q", signatures, want)
}

func TestDeliveryRetriesOnFailure(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusInternalServerError)

	appID := api.createApp(t, "Retries", nil)
	api.createEndpoint(t, appID, map[string]any{"url": receiver.server.URL})

	// A short, exact schedule so the test asserts real numbers quickly.
	policy := retry.NewPolicy(
		retry.WithSchedule([]time.Duration{200 * time.Millisecond, 10 * time.Minute}),
		retry.WithJitter(retry.NoJitter),
	)
	worker := newWorkerEnv(t, api, workerOptions{policy: policy})
	stop := worker.start(t)
	defer stop()

	msgID := worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})

	// The first attempt fails and is re-enqueued rather than retried in process,
	// so the delay survives a worker restart.
	attempts := api.waitForAttempts(t, msgID, 1, 10*time.Second)
	if attempts[0].Status != entities.AttemptPendingRetry {
		t.Fatalf("status = %v, want pending retry", attempts[0].Status)
	}
	if attempts[0].NextAttemptAt == nil {
		t.Error("a pending retry must record when the next attempt is due")
	}

	// The endpoint recovers, and the re-enqueued task succeeds on the retry.
	receiver.setStatus(http.StatusOK, nil)

	if !receiver.waitFor(2, 10*time.Second) {
		t.Fatal("the retry never happened")
	}
	final := api.waitForAttempts(t, msgID, 2, 10*time.Second)

	var succeeded bool
	for _, attempt := range final {
		if attempt.Status == entities.AttemptSucceeded {
			succeeded = true
			if attempt.AttemptNumber != 2 {
				t.Errorf("successful attempt number = %d, want 2", attempt.AttemptNumber)
			}
		}
	}
	if !succeeded {
		t.Error("expected the retry to succeed once the endpoint recovered")
	}
}

func TestDeliveryRespectsRetryAfter(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusTooManyRequests)
	receiver.setStatus(http.StatusTooManyRequests, map[string]string{"Retry-After": "600"})

	appID := api.createApp(t, "Backoff", nil)
	api.createEndpoint(t, appID, map[string]any{"url": receiver.server.URL})

	policy := retry.NewPolicy(
		retry.WithSchedule([]time.Duration{time.Minute, time.Hour}),
		retry.WithJitter(retry.NoJitter),
	)
	worker := newWorkerEnv(t, api, workerOptions{policy: policy})
	stop := worker.start(t)
	defer stop()

	msgID := worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})
	api.waitForAttempts(t, msgID, 1, 10*time.Second)

	// Retry-After of 600s is clamped into [60s, 120s], so the endpoint cannot
	// pin a queue slot for as long as it asked.
	//
	// The poll waits for visible_at to move into the future rather than reading
	// it once: the attempt row is written before the queue row is rescheduled, on
	// purpose, so an attempt can exist for a moment while the task still carries
	// its original visibility.
	var visibleIn time.Duration
	deadline := time.Now().Add(10 * time.Second)
	for {
		var seconds float64
		err := api.Pool.QueryRow(context.Background(),
			`SELECT EXTRACT(EPOCH FROM (visible_at - now())) FROM delivery_task WHERE msg_id = $1`, msgID,
		).Scan(&seconds)
		if err == nil && seconds > 0 {
			visibleIn = time.Duration(seconds * float64(time.Second))
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the task was never rescheduled into the future: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if visibleIn < 50*time.Second || visibleIn > 125*time.Second {
		t.Errorf("next attempt in %s, want it clamped into roughly [60s, 120s]", visibleIn)
	}
}

func TestDeliverySkipsDisabledEndpointWithoutSending(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusOK)

	appID := api.createApp(t, "Disabled", nil)
	endpointID, _ := api.createEndpoint(t, appID, map[string]any{"url": receiver.server.URL})

	// Disabling after ingest, before the worker starts: the queued task must
	// be dropped rather than delivered.
	if resp := api.do(t, request{
		Method: http.MethodPatch,
		Path:   "/api/v1/app/" + appID + "/endpoint/" + endpointID,
		Body:   map[string]any{"disabled": true},
	}); resp.Status != http.StatusOK {
		t.Fatalf("disable endpoint: %d %s", resp.Status, resp.Body)
	}

	worker := newWorkerEnv(t, api, workerOptions{})
	msgID := worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})
	api.reloadConfig(t)

	stop := worker.start(t)
	defer stop()

	deadline := time.Now().Add(5 * time.Second)
	for api.countTasks(t) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the queue never drained")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if len(receiver.requests()) != 0 {
		t.Error("a disabled endpoint must not receive anything")
	}
	result, err := api.Attempts.GetAttempts(context.Background(), repositories.AttemptFilters{
		MsgID: repositories.Eq(msgID),
	}, repositories.CursorPagination{Limit: 10})
	if err != nil {
		t.Fatalf("GetAttempts: %v", err)
	}
	if len(result.Items) != 0 {
		t.Errorf("a skipped delivery must not produce an attempt record, got %d", len(result.Items))
	}
}

func TestDeliveryFanoutReachesEveryEndpointOnce(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	appID := api.createApp(t, "Fanout", nil)

	sinks := make([]*sink, 3)
	for i := range sinks {
		sinks[i] = newSink(t, http.StatusOK)
		api.createEndpoint(t, appID, map[string]any{"url": sinks[i].server.URL})
	}
	// An endpoint subscribed to something else must not be reached.
	unrelated := newSink(t, http.StatusOK)
	api.createEndpoint(t, appID, map[string]any{
		"url":         unrelated.server.URL,
		"filterTypes": []string{"customer.created"},
	})

	worker := newWorkerEnv(t, api, workerOptions{})
	stop := worker.start(t)
	defer stop()

	msgID := worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})

	for i, s := range sinks {
		if !s.waitFor(1, 10*time.Second) {
			t.Fatalf("sink %d never received the webhook", i)
		}
	}
	api.waitForAttempts(t, msgID, len(sinks), 10*time.Second)

	for i, s := range sinks {
		if got := len(s.requests()); got != 1 {
			t.Errorf("sink %d received %d requests, want exactly one", i, got)
		}
	}
	if got := len(unrelated.requests()); got != 0 {
		t.Errorf("the unsubscribed endpoint received %d requests", got)
	}
}

func TestDeliveryShutdownFlushesBufferedAttempts(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusOK)

	appID := api.createApp(t, "Shutdown", nil)
	api.createEndpoint(t, appID, map[string]any{"url": receiver.server.URL})

	// A batch size and interval that would never flush on their own inside
	// the test: only the shutdown flush can produce the row.
	worker := newWorkerEnv(t, api, workerOptions{batchSize: 500, flushInterval: time.Hour})
	stop := worker.start(t)

	msgID := worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})
	if !receiver.waitFor(1, 10*time.Second) {
		t.Fatal("the endpoint never received the webhook")
	}

	// Nothing is recorded yet: the record is sitting in the writer's buffer.
	before, err := api.Attempts.GetAttempts(context.Background(), repositories.AttemptFilters{
		MsgID: repositories.Eq(msgID),
	}, repositories.CursorPagination{Limit: 10})
	if err != nil {
		t.Fatalf("GetAttempts: %v", err)
	}
	if len(before.Items) != 0 {
		t.Fatalf("found %d attempts before the flush; the test is not exercising the shutdown path", len(before.Items))
	}

	stop()

	after := api.waitForAttempts(t, msgID, 1, 5*time.Second)
	if after[0].Status != entities.AttemptSucceeded {
		t.Errorf("status = %v, want succeeded", after[0].Status)
	}
	// The queue row is removed as part of the same flush.
	if remaining := api.countTasks(t); remaining != 0 {
		t.Errorf("%d queue rows remain after the shutdown flush", remaining)
	}
}

// newBlockingSink is a receiver that holds every request until release is
// closed, signalling on held as each one arrives. It stands in for a
// tarpitting endpoint: one that accepts the connection and then never answers,
// which is what actually exhausts a delivery pool in production.
func newBlockingSink(t *testing.T, release <-chan struct{}, held chan<- struct{}) *httptest.Server {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		select {
		case held <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server
}

// engineOverrides adjusts one aspect of the harness's engine, for tests about
// partition ownership or operational webhooks.
type engineOverrides struct {
	partitions   []int16
	operational  dispatch.OperationalEmitter
	policy       *retry.Policy
	failingAt    int16
	disableAfter time.Duration
}

// rebuildEngine returns an engine sharing the harness's writer, client and
// lanes, with the supplied overrides applied.
func rebuildEngine(t *testing.T, api *apiEnv, worker *workerEnv, overrides engineOverrides) *dispatch.Engine {
	t.Helper()

	var partitions dispatch.PartitionSource = dispatch.NewStaticPartitions()
	if overrides.partitions != nil {
		source := dispatch.NewAtomicPartitions()
		source.Store(overrides.partitions)
		partitions = source
	}

	engine, err := dispatch.NewEngine(dispatch.EngineDeps{
		TaskQueue:           api.Queue,
		TxManager:           api.Store,
		MessageStore:        postgres.NewMessageStore(api.Messages),
		PayloadCodec:        worker.codec,
		SnapshotProvider:    api.Config,
		DeliveryClient:      worker.client,
		AttemptSink:         worker.writer,
		Policy:              overrides.policy,
		PartitionSource:     partitions,
		SnapshotInvalidator: api.Config,
		LaneManager:         worker.lanes,
		EndpointState:       api.Endpoints,
		Operational:         overrides.operational,
	}, dispatch.EngineOptions{
		WorkerID:             "wkr_rebuilt",
		Pool:                 cmp.Or(worker.opts.pool, "default"),
		ClaimBatchSize:       50,
		MaxInflight:          16,
		PollInterval:         10 * time.Millisecond,
		RequestTimeout:       cmp.Or(worker.opts.requestTimeout, 5*time.Second),
		LockTTL:              time.Minute,
		LaneWaitTimeout:      100 * time.Millisecond,
		LaneRequeueDelay:     50 * time.Millisecond,
		QuarantinePool:       configs.QuarantinePool,
		QuarantineAfter:      worker.opts.quarantineAfter,
		DisableAfter:         overrides.disableAfter,
		FailingNotifyAttempt: overrides.failingAt,
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return engine
}

// TestDeliveryAfterEndpointCreatedInTheSameSecond covers the window a KSUID's
// one-second timestamp resolution opens. An endpoint created a fraction of a
// second before a message is created has a real timestamp *later* than the
// message's truncated one, so a config snapshot built in between looks fresh
// while predating the endpoint. Judging freshness against the message rather
// than the queued task loses the delivery outright.
func TestDeliveryAfterEndpointCreatedInTheSameSecond(t *testing.T) {
	t.Parallel()

	api := newAPIEnv(t)
	receiver := newSink(t, http.StatusOK)

	appID := api.createApp(t, "Same second", nil)

	worker := newWorkerEnv(t, api, workerOptions{})
	stop := worker.start(t)
	defer stop()

	// Create the endpoint and send immediately, with no reload in between:
	// the worker's snapshot is the one from before the endpoint existed.
	api.createEndpointWithoutReload(t, appID, map[string]any{"url": receiver.server.URL})
	msgID := worker.ingest(t, appID, "invoice.paid", map[string]any{"a": 1})

	// The worker must wait for its snapshot rather than concluding that nothing
	// subscribes.
	if !receiver.waitFor(1, 20*time.Second) {
		t.Fatal("a message sent immediately after creating an endpoint was never delivered")
	}
	api.waitForAttempts(t, msgID, 1, 20*time.Second)

	deadline := time.Now().Add(10 * time.Second)
	for api.countTasks(t) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d queue rows remain", api.countTasks(t))
		}
		time.Sleep(50 * time.Millisecond)
	}
}
