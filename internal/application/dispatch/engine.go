package dispatch

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/plusiv/huxio/internal/application/config"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/domain/retry"
	"github.com/plusiv/huxio/internal/domain/signing"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
	"github.com/rs/zerolog"
)

// EngineOptions configures the worker loop.
type EngineOptions struct {
	// WorkerID identifies this process in the queue's lock column.
	WorkerID string
	// Pool is the worker pool this engine serves.
	Pool string
	// ClaimBatchSize is how many tasks one claim query locks. 50-200 keeps it
	// one round trip per batch rather than per task.
	ClaimBatchSize int
	// MaxInflight bounds deliveries in flight across the whole worker.
	MaxInflight int
	// LockTTL must exceed RequestTimeout plus a buffer, or a slow delivery is
	// re-claimed while still in flight and the endpoint sees a duplicate.
	LockTTL time.Duration
	// PollInterval is the fallback poll for delayed tasks becoming visible and
	// for notifications lost on reconnect.
	PollInterval time.Duration
	// RequestTimeout bounds one outbound delivery.
	RequestTimeout time.Duration
	// ResponseBodyLimit caps how much of a response body is recorded.
	ResponseBodyLimit int
	// CompatHeaders selects the signature header naming.
	CompatHeaders signing.HeaderScheme
	// StaleSnapshotDelay is how long a task is deferred when this worker's
	// config snapshot predates the work it describes.
	StaleSnapshotDelay time.Duration
	// LaneWaitTimeout is how long a delivery waits for a slot in its endpoint's
	// lane before the task is put back in the queue. Waiting a little absorbs a
	// burst without a round trip to Postgres. A waiting delivery gives up its
	// in-flight slot for the duration, so however many pile up behind a dead
	// endpoint, they cannot stop this worker claiming other endpoints' work.
	LaneWaitTimeout time.Duration
	// LaneRequeueDelay is how long a task deferred for lane saturation waits
	// before it is visible again.
	LaneRequeueDelay time.Duration
	// QuarantinePool receives endpoints whose breaker has been open past
	// QuarantineAfter.
	QuarantinePool string
	// QuarantineAfter is how long a breaker must stay open before its endpoint is
	// moved to the quarantine pool. Zero disables the transition.
	QuarantineAfter time.Duration
	// DisableAfter is how long an endpoint must fail continuously before it
	// is switched off. Zero disables auto-disable.
	DisableAfter time.Duration
	// FailingNotifyAttempt is the attempt number at which the
	// message.attempt.failing operational webhook fires. Zero disables it.
	FailingNotifyAttempt int16
}

// EngineMetrics is the metric set the engine reports through, as ports so the
// application layer never imports a metrics library.
type EngineMetrics struct {
	DeliveryDuration func(org, outcome string, seconds float64)
	InternalDuration func(org string, seconds float64)
	DeliveriesTotal  func(org, outcome, statusClass string)
	ClaimBatchSize   func(size float64)
	InflightTotal    func(count float64)
}

// Engine turns queued tasks into delivered webhooks.
type Engine struct {
	taskQueue        Queue
	txManager        TxManager
	messageStore     MessageStore
	payloadCodec     PayloadDecoder
	snapshotProvider SnapshotProvider
	deliveryClient   DeliveryClient
	attemptSink      AttemptSink
	signer           *signing.Signer
	policy           *retry.Policy
	partitionSource  PartitionSource
	invalidator      SnapshotInvalidator
	laneManager      *LaneManager
	// endpointState records the health transitions that outlive this worker.
	endpointState EndpointStateWriter
	operational   OperationalEmitter
	metrics       EngineMetrics
	opts          EngineOptions

	// inflight bounds concurrent deliveries. Claiming stops while it is full,
	// so backpressure ends with tasks staying in Postgres where they are
	// durable, rather than in memory where they are not.
	inflight chan struct{}
	wg       sync.WaitGroup
}

// EngineDeps is everything the engine needs.
type EngineDeps struct {
	TaskQueue        Queue
	TxManager        TxManager
	MessageStore     MessageStore
	PayloadCodec     PayloadDecoder
	SnapshotProvider SnapshotProvider
	DeliveryClient   DeliveryClient
	AttemptSink      AttemptSink
	Policy           *retry.Policy
	PartitionSource  PartitionSource
	// SnapshotInvalidator is optional: when set, a worker that finds its snapshot too
	// old to decide a task asks for a reload instead of waiting for the next
	// scheduled refresh.
	SnapshotInvalidator SnapshotInvalidator
	// LaneManager is optional. Without it every delivery runs under the global
	// in-flight bound alone, which is no per-endpoint bound at all; with it,
	// isolation becomes structural.
	LaneManager *LaneManager
	// EndpointState is optional: it records the health transitions that outlive a
	// worker (failure-run start, auto-disable, quarantine).
	EndpointState EndpointStateWriter
	// Operational is optional: when set, the engine publishes operational
	// webhooks about failing, recovered, exhausted and disabled endpoints.
	Operational OperationalEmitter
	Metrics     EngineMetrics
}

// NewEngine builds an engine.
func NewEngine(deps EngineDeps, opts EngineOptions) (*Engine, error) {
	switch {
	case deps.TaskQueue == nil, deps.TxManager == nil, deps.MessageStore == nil, deps.PayloadCodec == nil,
		deps.SnapshotProvider == nil, deps.DeliveryClient == nil, deps.AttemptSink == nil, deps.PartitionSource == nil:
		return nil, eris.New("dispatch: engine is missing a dependency")
	}
	if opts.WorkerID == "" {
		opts.WorkerID = ids.New(ids.PrefixWorker)
	}
	if opts.Pool == "" {
		opts.Pool = "default"
	}
	if opts.ClaimBatchSize <= 0 {
		opts.ClaimBatchSize = 100
	}
	if opts.MaxInflight <= 0 {
		opts.MaxInflight = 2000
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 250 * time.Millisecond
	}
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = 30 * time.Second
	}
	if opts.LockTTL <= opts.RequestTimeout {
		opts.LockTTL = opts.RequestTimeout + time.Minute
	}
	if opts.ResponseBodyLimit <= 0 {
		opts.ResponseBodyLimit = 8 << 10
	}
	if opts.StaleSnapshotDelay <= 0 {
		opts.StaleSnapshotDelay = 250 * time.Millisecond
	}
	if opts.LaneWaitTimeout <= 0 {
		opts.LaneWaitTimeout = 2 * time.Second
	}
	if opts.LaneRequeueDelay <= 0 {
		opts.LaneRequeueDelay = time.Second
	}
	if opts.QuarantinePool == "" {
		opts.QuarantinePool = "quarantine"
	}
	if opts.FailingNotifyAttempt == 0 {
		opts.FailingNotifyAttempt = 4
	}

	policy := deps.Policy
	if policy == nil {
		policy = retry.NewPolicy()
	}

	return &Engine{
		taskQueue:        deps.TaskQueue,
		txManager:        deps.TxManager,
		messageStore:     deps.MessageStore,
		payloadCodec:     deps.PayloadCodec,
		snapshotProvider: deps.SnapshotProvider,
		deliveryClient:   deps.DeliveryClient,
		attemptSink:      deps.AttemptSink,
		signer:           signing.NewSigner(opts.CompatHeaders),
		policy:           policy,
		partitionSource:  deps.PartitionSource,
		invalidator:      deps.SnapshotInvalidator,
		laneManager:      deps.LaneManager,
		endpointState:    deps.EndpointState,
		operational:      deps.Operational,
		metrics:          deps.Metrics,
		opts:             opts,
		inflight:         make(chan struct{}, opts.MaxInflight),
	}, nil
}

// WorkerID reports this engine's identity.
func (e *Engine) WorkerID() string { return e.opts.WorkerID }

// Run claims and dispatches tasks until ctx is cancelled. wakeups carries
// partition keys published by the queue; it may be nil, in which case only the
// fallback poll drives the loop.
func (e *Engine) Run(ctx context.Context, wakeups <-chan int16) error {
	// The runner already stamps worker_id, so only the pool is added here.
	log := logger.FromContext(ctx).With().Str("pool", e.opts.Pool).Logger()
	ctx = logger.Context(ctx, log)

	ticker := time.NewTicker(e.opts.PollInterval)
	defer ticker.Stop()

	log.Info().Int("max_inflight", e.opts.MaxInflight).Msg("delivery engine started")

	for {
		// Drain the queue until a claim comes back short, then wait. A short
		// batch is the signal that there is nothing ready right now.
		for {
			claimed, err := e.claimAndDispatch(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return e.drain(log)
				}
				log.Error().Err(err).Msg("claim failed")
				break
			}
			if claimed < e.opts.ClaimBatchSize {
				break
			}
		}

		// Wait for a reason to claim again: the poll tick, or a wakeup for a
		// partition this worker owns. Anything else keeps waiting, so it never
		// costs a claim query.
	wait:
		for {
			select {
			case <-ctx.Done():
				return e.drain(log)
			case <-ticker.C:
				break wait
			case partition, ok := <-wakeups:
				if !ok {
					wakeups = nil
					continue
				}
				// One claim covers every partition this worker owns, so a burst of
				// wakeups collapses into one query: a fan-out notifies for every
				// partition it expanded into, and a busy API notifies once per
				// message. Draining happens before the claim, not after, so a wakeup
				// that lands during the claim, for a row the claim's snapshot could
				// not see, is still there to trigger the next one.
				if e.drainWakeups(wakeups, e.owns(partition)) {
					break wait
				}
			}
		}
	}
}

// drainWakeups empties whatever is buffered on the wakeup channel without
// blocking, and reports whether any drained wakeup, or the one already taken
// (owned), is for a partition this worker owns.
func (e *Engine) drainWakeups(wakeups <-chan int16, owned bool) bool {
	for {
		select {
		case partition, ok := <-wakeups:
			if !ok {
				return owned
			}
			if e.owns(partition) {
				owned = true
			}
		default:
			return owned
		}
	}
}

// drain waits for in-flight deliveries to finish. The attempt writer is
// flushed by its own Run loop; the caller sequences the two.
func (e *Engine) drain(log zerolog.Logger) error {
	log.Info().Msg("waiting for in-flight deliveries")
	e.wg.Wait()
	log.Info().Msg("delivery engine drained")
	return nil
}

func (e *Engine) owns(partition int16) bool {
	return utils.Contains(e.partitionSource.Partitions(), partition)
}

// claimAndDispatch locks one batch and hands each task to a worker goroutine,
// returning how many tasks were claimed.
func (e *Engine) claimAndDispatch(ctx context.Context) (int, error) {
	partitions := e.partitionSource.Partitions()
	if len(partitions) == 0 {
		return 0, nil
	}

	// Never claim more than there is capacity to run: a claimed task holds a
	// lock, and a lock held by a task nobody is working on is pure lag.
	capacity := e.opts.MaxInflight - len(e.inflight)
	if capacity <= 0 {
		return 0, nil
	}
	limit := min(e.opts.ClaimBatchSize, capacity)

	tasks, err := e.taskQueue.Claim(ctx, repositories.ClaimRequest{
		Pool:       e.opts.Pool,
		Partitions: partitions,
		OwnerID:    e.opts.WorkerID,
		LockTTL:    e.opts.LockTTL,
		Limit:      limit,
	})
	if err != nil {
		return 0, err
	}
	if e.metrics.ClaimBatchSize != nil {
		e.metrics.ClaimBatchSize(float64(len(tasks)))
	}
	if len(tasks) == 0 {
		return 0, nil
	}

	for _, task := range tasks {
		slot := &inflightSlot{engine: e}
		if err := slot.acquire(ctx); err != nil {
			// Shutting down. Give back what has not been started so another worker
			// can pick it up immediately instead of waiting for the lock to expire.
			e.release(ctx, tasks)
			return len(tasks), err
		}

		e.wg.Add(1)

		go func(task entities.DeliveryTask, slot *inflightSlot) {
			defer func() {
				slot.release()
				e.wg.Done()
			}()

			// Detached from the loop's cancellation on purpose. On shutdown the claim
			// loop stops immediately, but a request already on the wire runs to
			// completion: abandoning it mid-flight means the receiver may have
			// processed it while we recorded nothing, so the retry would duplicate. The
			// timeout still bounds it.
			taskCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx),
				e.opts.RequestTimeout+30*time.Second,
			)
			defer cancel()

			e.handle(taskCtx, task, slot)
		}(task, slot)
	}

	return len(tasks), nil
}

// inflightSlot is one delivery's hold on the engine's in-flight budget. It is
// taken when the task is claimed and given back when the delivery is done,
// except while the delivery waits for a lane slot: a waiter has no request on
// the wire and holds no payload, so it should cost the worker nothing. Without
// that release, one endpoint that stopped answering fills the whole budget
// with waiters and the worker stops claiming everyone else's work.
type inflightSlot struct {
	engine *Engine
	held   bool
}

func (s *inflightSlot) acquire(ctx context.Context) error {
	if s.held {
		return nil
	}
	select {
	case s.engine.inflight <- struct{}{}:
		s.held = true
		s.engine.reportInflight()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *inflightSlot) release() {
	if !s.held {
		return
	}
	<-s.engine.inflight
	s.held = false
	s.engine.reportInflight()
}

func (e *Engine) reportInflight() {
	if e.metrics.InflightTotal != nil {
		e.metrics.InflightTotal(float64(len(e.inflight)))
	}
}

func (e *Engine) release(ctx context.Context, tasks []entities.DeliveryTask) {
	ids := make([]int64, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.ID)
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := e.taskQueue.Release(releaseCtx, ids); err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("failed to release claimed tasks")
	}
}

// handle dispatches one task by kind.
func (e *Engine) handle(ctx context.Context, task entities.DeliveryTask, slot *inflightSlot) {
	ctx = logger.With(ctx, map[string]any{
		"task_id": task.ID,
		"msg_id":  task.MsgID,
		"org_id":  task.OrgID,
	})

	switch task.Kind {
	case entities.TaskFanout:
		e.handleFanout(ctx, task)
	case entities.TaskDeliver:
		e.handleDeliver(ctx, task, slot)
	default:
		logger.FromContext(ctx).Error().Int("kind", int(task.Kind)).Msg("unknown task kind, dropping")
		e.complete(ctx, task.ID)
	}
}

// handleFanout expands one message into one deliver task per matching
// endpoint. Matching is pure in-memory set logic against the snapshot, and the
// expansion and the removal of the fan-out row commit together so a crash
// cannot half-expand.
func (e *Engine) handleFanout(ctx context.Context, task entities.DeliveryTask) {
	log := logger.FromContext(ctx)

	snapshot := e.snapshotProvider.Current()
	if snapshot == nil {
		log.Error().Msg("config snapshot is not loaded, leaving fan-out queued")
		return
	}

	msg, err := e.messageStore.LoadMessage(ctx, task.MsgID, task.MsgCreatedAt)
	if err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			// The message aged out from under the task; there is nothing left to
			// deliver.
			log.Warn().Msg("message no longer exists, dropping fan-out")
			e.complete(ctx, task.ID)
			return
		}
		log.Error().Err(err).Msg("failed to load message for fan-out")
		return
	}

	endpoints := snapshot.MatchingEndpoints(task.AppID, msg.EventType, msg.Channels)
	if len(endpoints) == 0 {
		// "Nothing matches" is only a safe conclusion when the snapshot is at least
		// as new as the work: any endpoint that existed when the fan-out was queued
		// is then in it. A snapshot that predates the task may simply not have seen
		// a just-created endpoint, and dropping the fan-out on that basis loses a
		// delivery we already acknowledged.
		//
		// The comparison is against the task's created_at, not the message's. A
		// message's created_at is the second-resolution timestamp inside its KSUID,
		// so it can sit up to a second in the past: a snapshot built inside that
		// window looks fresh while predating an endpoint created moments before the
		// message. The task row's created_at is real time from the database, which
		// closes the window.
		if snapshot.BuiltAt().Before(task.CreatedAt) {
			e.deferTask(ctx, task.ID, "config snapshot predates the fan-out task")
			return
		}
		e.complete(ctx, task.ID)
		return
	}

	tasks := make([]repositories.EnqueueTask, 0, len(endpoints))
	now := time.Now().UTC()
	for _, ep := range endpoints {
		tasks = append(tasks, repositories.EnqueueTask{
			PartitionKey: entities.PartitionKeyFor(ep.ID),
			Pool:         utils.Fallback(ep.Pool, e.opts.Pool),
			Kind:         entities.TaskDeliver,
			OrgID:        task.OrgID,
			AppID:        task.AppID,
			MsgID:        task.MsgID,
			MsgCreatedAt: task.MsgCreatedAt,
			EndpointID:   utils.Ptr(ep.ID),
			TriggerType:  task.TriggerType,
			VisibleAt:    now,
		})
	}

	err = e.txManager.WithinTransaction(ctx, func(txCtx context.Context) error {
		if err := e.taskQueue.Enqueue(txCtx, tasks); err != nil {
			return err
		}
		return e.taskQueue.Complete(txCtx, task.ID)
	})
	if err != nil {
		log.Error().Err(err).Int("endpoints", len(tasks)).Msg("fan-out failed")
		return
	}

	// One wakeup for every partition the expansion landed in, after the commit
	// and never inside it: the queue's Notify explains why. Best effort, the
	// owners also poll.
	if err := e.taskQueue.Notify(ctx, repositories.DistinctPartitions(tasks)); err != nil {
		log.Warn().Err(err).Msg("fan-out wakeup failed")
	}
	log.Debug().Int("endpoints", len(tasks)).Msg("fanned out")
}

// handleDeliver signs and sends one message to one endpoint, records the
// attempt, and decides whether to retry.
func (e *Engine) handleDeliver(ctx context.Context, task entities.DeliveryTask, slot *inflightSlot) {
	log := logger.FromContext(ctx)

	if task.EndpointID == nil {
		log.Error().Msg("deliver task without an endpoint, dropping")
		e.complete(ctx, task.ID)
		return
	}
	endpointID := *task.EndpointID
	ctx = logger.With(ctx, map[string]any{"endpoint_id": endpointID})
	log = logger.FromContext(ctx)

	snapshot := e.snapshotProvider.Current()
	if snapshot == nil {
		log.Error().Msg("config snapshot is not loaded, leaving task queued")
		return
	}

	ep := snapshot.Endpoint(endpointID)
	if ep == nil {
		// An endpoint missing from a snapshot older than the task may just be
		// one this worker has not seen yet: the fan-out that created the task
		// did see it. Wait for the snapshot rather than dropping the delivery.
		if snapshot.BuiltAt().Before(task.CreatedAt) {
			e.deferTask(ctx, task.ID, "config snapshot predates the task")
			return
		}
		log.Debug().Msg("endpoint no longer exists, dropping task")
		e.complete(ctx, task.ID)
		return
	}
	if !ep.Deliverable() {
		// Disabled or deleted: there is nothing to deliver to, and a retry would not
		// change that.
		log.Debug().Msg("endpoint is not deliverable, dropping task")
		e.complete(ctx, task.ID)
		return
	}

	// The lane is this endpoint's dedicated slice of capacity: its AIMD
	// concurrency, its rate limit and its circuit breaker, all in this worker's
	// memory.
	lane := e.laneFor(ep)
	if lane != nil {
		if !lane.Allow() {
			// The breaker is open: no DNS lookup, no connect, no handshake. The task is
			// re-queued with the normal retry delay.
			e.requeueForBreaker(ctx, task)
			return
		}

		if !lane.TryAcquire() {
			// The lane is full. Wait a little for a slot, but not on the engine's
			// in-flight budget: a waiter has nothing on the wire, and if waiters
			// counted, enough tasks behind one dead endpoint would fill the budget
			// and stop this worker claiming other tenants' work.
			slot.release()
			waitCtx, cancel := context.WithTimeout(ctx, e.opts.LaneWaitTimeout)
			err := lane.Acquire(waitCtx)
			cancel()
			if err != nil {
				// Saturated. The excess goes back to Postgres, where it is durable and
				// costs this worker nothing until the lane has room again.
				e.deferForSaturation(ctx, task.ID)
				return
			}
			if err := slot.acquire(ctx); err != nil {
				lane.Release()
				e.deferForSaturation(ctx, task.ID)
				return
			}
		}
		defer lane.Release()
	}

	claimedAt := time.Now()

	stored, err := e.messageStore.LoadPayload(ctx, task.MsgID, task.MsgCreatedAt)
	if err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			log.Warn().Msg("message no longer exists, dropping task")
			e.complete(ctx, task.ID)
			return
		}
		log.Error().Err(err).Msg("failed to load payload")
		return
	}
	payload, err := e.payloadCodec.Decode(stored)
	if err != nil {
		log.Error().Err(err).Msg("failed to decode payload, dropping task")
		e.complete(ctx, task.ID)
		return
	}

	headers, err := e.buildHeaders(task.MsgID, payload, ep)
	if err != nil {
		log.Error().Err(err).Msg("failed to sign message, dropping task")
		e.complete(ctx, task.ID)
		return
	}

	// Everything up to here is the internal delivery latency: claim to bytes
	// on the wire, excluding the endpoint's own response time.
	if e.metrics.InternalDuration != nil {
		e.metrics.InternalDuration(task.OrgID, time.Since(claimedAt).Seconds())
	}

	result := e.deliveryClient.Deliver(ctx, DeliveryRequest{
		URL:     ep.URL,
		Body:    payload,
		Headers: headers,
		Timeout: e.opts.RequestTimeout,
	})

	if lane != nil {
		if result.Succeeded() {
			lane.OnSuccess()
		} else {
			lane.OnFailure(result)
		}
	}
	e.applyHealthTransitions(ctx, ep, lane, result.Succeeded())

	e.recordAttempt(ctx, task, ep, result)
}

// laneFor returns this endpoint's lane, or nil when lanes are not configured.
func (e *Engine) laneFor(ep *config.Endpoint) *Lane {
	if e.laneManager == nil {
		return nil
	}
	return e.laneManager.For(ep)
}

// requeueForBreaker puts a task back with the delay its next attempt would
// have had, without recording an attempt: no request was made, so there is
// nothing to record.
func (e *Engine) requeueForBreaker(ctx context.Context, task entities.DeliveryTask) {
	delay, retryable := e.policy.Delay(int(task.Attempt), retry.Outcome{})
	if !retryable {
		// Out of attempts and the endpoint is still down: stop carrying it.
		logger.FromContext(ctx).Debug().Msg("breaker open and attempts exhausted, dropping task")
		e.complete(ctx, task.ID)
		return
	}
	if err := e.taskQueue.Retry(ctx, task.ID, delay); err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("failed to requeue task behind an open breaker")
	}
}

// applyHealthTransitions records the endpoint state that must outlive this
// worker: the start of a failure run, auto-disable after a sustained one, and
// promotion to the quarantine pool.
//
// What has already been written is tracked on the lane, never on the config
// snapshot: the snapshot is shared by every concurrent delivery and mutating
// it is a data race.
func (e *Engine) applyHealthTransitions(ctx context.Context, ep *config.Endpoint, lane *Lane, succeeded bool) {
	if e.endpointState == nil || lane == nil {
		return
	}
	log := logger.FromContext(ctx)
	now := time.Now().UTC()

	if succeeded {
		// Any success ends the run. The write only happens if there was something to
		// clear.
		if lane.EndFailureRun() || ep.FirstFailureAt != nil {
			if err := e.endpointState.SetFirstFailure(ctx, ep.ID, nil); err != nil {
				log.Warn().Err(err).Msg("failed to clear the endpoint failure run")
				return
			}
			e.invalidate()
		}
		return
	}

	start := lane.FailureRunStart(ep.FirstFailureAt)
	if start == nil {
		if lane.BeginFailureRun(now) {
			if err := e.endpointState.SetFirstFailure(ctx, ep.ID, &now); err != nil {
				log.Warn().Err(err).Msg("failed to record the endpoint failure run")
			} else {
				e.invalidate()
			}
		}
		start = &now
	}

	// Auto-disable is deliberately much slower than the breaker: the breaker
	// stops dialling for a while, this switches the endpoint off until its
	// owner turns it back on.
	failingFor := now.Sub(*start)
	if e.opts.DisableAfter > 0 && failingFor >= e.opts.DisableAfter && ep.DisabledAt == nil {
		if !lane.MarkDisabled() {
			return
		}
		if err := e.endpointState.SetDisabled(ctx, ep.ID, &now); err != nil {
			log.Warn().Err(err).Msg("failed to auto-disable the endpoint")
			return
		}
		e.invalidate()
		log.Warn().Dur("failing_for", failingFor).Msg("endpoint auto-disabled after sustained failure")
		e.emitEndpointDisabled(ctx, ep, failingFor)
		return
	}

	// Quarantine contains a retry storm from a customer who has been down for
	// hours: subsequent tasks route to the quarantine pool and stop consuming
	// healthy capacity entirely.
	if e.opts.QuarantineAfter > 0 && ep.Pool != e.opts.QuarantinePool &&
		lane.Breaker().OpenFor() >= e.opts.QuarantineAfter {
		if !lane.MarkQuarantined() {
			return
		}
		if err := e.endpointState.SetPool(ctx, ep.ID, e.opts.QuarantinePool); err != nil {
			log.Warn().Err(err).Msg("failed to quarantine the endpoint")
			return
		}
		e.invalidate()
		log.Warn().
			Dur("breaker_open_for", lane.Breaker().OpenFor()).
			Str("pool", e.opts.QuarantinePool).
			Msg("endpoint moved to the quarantine pool")
	}
}

// emitAttemptEvents publishes the operational webhooks about one delivery: a
// warning partway through the retry schedule, a recovery notice, and a final
// exhausted notice. They are emitted once each, tracked on the lane, so a
// tenant is not told the same thing on every retry.
func (e *Engine) emitAttemptEvents(
	ctx context.Context,
	task entities.DeliveryTask,
	ep *config.Endpoint,
	attempt entities.DeliveryAttempt,
) {
	if e.operational == nil {
		return
	}
	lane := e.laneFor(ep)

	event := OperationalEvent{
		OrgID:      task.OrgID,
		AppID:      task.AppID,
		EndpointID: ep.ID,
		MsgID:      task.MsgID,
		Detail: map[string]any{
			"attemptNumber":      attempt.AttemptNumber,
			"responseStatusCode": attempt.ResponseStatusCode,
			"url":                ep.URL,
		},
	}

	switch {
	case attempt.Status == entities.AttemptSucceeded:
		// Only worth saying when we said something was wrong earlier.
		if lane == nil || !lane.ClearFailingNotice() {
			return
		}
		event.Type = EventAttemptRecovered

	case attempt.Status == entities.AttemptFailed:
		event.Type = EventAttemptExhausted
		if lane != nil {
			lane.ClearFailingNotice()
		}

	case attempt.AttemptNumber == e.opts.FailingNotifyAttempt:
		// Fired once, partway through the schedule, so the tenant hears about
		// a struggling endpoint before the attempts run out.
		if lane != nil && !lane.MarkFailingNotice() {
			return
		}
		event.Type = EventAttemptFailing

	default:
		return
	}

	if err := e.operational.Emit(ctx, event); err != nil {
		// An operational webhook must never fail a real delivery.
		logger.FromContext(ctx).Warn().Err(err).Str("event", event.Type).Msg("failed to emit an operational event")
	}
}

// emitEndpointDisabled tells the tenant an endpoint was switched off.
func (e *Engine) emitEndpointDisabled(ctx context.Context, ep *config.Endpoint, failingFor time.Duration) {
	if e.operational == nil {
		return
	}
	err := e.operational.Emit(ctx, OperationalEvent{
		OrgID:      ep.OrgID,
		Type:       EventEndpointDisabled,
		AppID:      ep.AppID,
		EndpointID: ep.ID,
		Detail: map[string]any{
			"url":             ep.URL,
			"failingForHours": failingFor.Hours(),
		},
	})
	if err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("failed to emit the endpoint disabled event")
	}
}

func (e *Engine) invalidate() {
	if e.invalidator != nil {
		e.invalidator.Invalidate()
	}
}

// Lanes exposes the lane manager, for the maintenance loop's eviction pass and
// for metrics.
func (e *Engine) Lanes() *LaneManager { return e.laneManager }

// buildHeaders applies the endpoint's custom headers first and the signature
// headers last, so a custom header can never override a signature.
func (e *Engine) buildHeaders(msgID string, payload []byte, ep *config.Endpoint) (map[string]string, error) {
	keys := make([]signing.Key, 0, len(ep.Secrets))
	for _, secret := range ep.Secrets {
		keys = append(keys, signing.Key{Secret: secret.Key, Type: secret.Type})
	}

	signed, err := e.signer.Headers(msgID, time.Now().UTC(), payload, keys)
	if err != nil {
		return nil, err
	}

	headers := make(map[string]string, len(ep.CustomHeaders)+len(signed))
	for name, value := range ep.CustomHeaders {
		headers[strings.ToLower(name)] = value
	}
	for name, value := range signed {
		headers[name] = value
	}
	return headers, nil
}

// recordAttempt writes the attempt and decides the fate of the queue row. The
// two travel together through the writer, so the row is only removed after the
// attempt is durable.
func (e *Engine) recordAttempt(ctx context.Context, task entities.DeliveryTask, ep *config.Endpoint, result DeliveryResult) {
	log := logger.FromContext(ctx)
	now := time.Now().UTC()

	attemptID, err := ids.NewAt(ids.PrefixAttempt, now)
	if err != nil {
		log.Error().Err(err).Msg("failed to generate attempt id")
		return
	}

	// Unlike a message, an attempt's created_at is the real time rather than
	// the second-resolution timestamp inside its KSUID: several attempts for
	// one endpoint can land in the same second, and "the latest attempt" has
	// to be answerable. The day partition is still the id's day.
	attempt := entities.DeliveryAttempt{
		ID:                 attemptID,
		CreatedAt:          now,
		OrgID:              task.OrgID,
		AppID:              task.AppID,
		MsgID:              task.MsgID,
		EndpointID:         ep.ID,
		URL:                ep.URL,
		ResponseStatusCode: int16(result.StatusCode),
		ResponseBody:       utils.Truncate(responseBody(result), e.opts.ResponseBodyLimit),
		ResponseDurationMS: int32(result.Duration.Milliseconds()),
		AttemptNumber:      task.Attempt + 1,
		TriggerType:        task.TriggerType,
	}

	outcome := &TaskOutcome{TaskID: task.ID, Action: TaskComplete}

	switch {
	case result.Succeeded():
		attempt.Status = entities.AttemptSucceeded

	case !task.TriggerType.Retryable():
		// A manual resend or a bulk replay gets one attempt: record it, done.
		attempt.Status = entities.AttemptFailed

	default:
		delay, retryable := e.policy.Delay(int(task.Attempt), retry.Outcome{
			StatusCode: result.StatusCode,
			Timeout:    result.Timeout,
			RetryAfter: result.RetryAfter,
		})
		if retryable {
			attempt.Status = entities.AttemptPendingRetry
			attempt.NextAttemptAt = utils.Ptr(now.Add(delay))
			outcome = &TaskOutcome{TaskID: task.ID, Action: TaskRetry, Delay: delay}
		} else {
			attempt.Status = entities.AttemptFailed
		}
	}

	e.reportMetrics(task.OrgID, attempt.Status, result)
	e.emitAttemptEvents(ctx, task, ep, attempt)

	if err := e.attemptSink.Enqueue(ctx, AttemptRecord{Attempt: attempt, Task: outcome}); err != nil {
		// The task keeps its lock, which expires and returns it to the queue.
		log.Error().Err(err).Msg("failed to buffer attempt record")
	}
}

func (e *Engine) reportMetrics(orgID string, status entities.AttemptStatus, result DeliveryResult) {
	outcome := "failure"
	switch {
	case status == entities.AttemptSucceeded:
		outcome = "success"
	case result.Err != nil:
		outcome = "error"
	}

	if e.metrics.DeliveryDuration != nil {
		e.metrics.DeliveryDuration(orgID, outcome, result.Duration.Seconds())
	}
	if e.metrics.DeliveriesTotal != nil {
		e.metrics.DeliveriesTotal(orgID, outcome, statusClass(result.StatusCode))
	}
}

// deferForSaturation puts a task back for the lane requeue delay without
// counting an attempt. Unlike deferTask it does not touch the config snapshot:
// a full lane says nothing about the configuration being stale, and asking for
// a reload on every saturated task would rebuild the snapshot in a loop for
// as long as the endpoint stays down.
func (e *Engine) deferForSaturation(ctx context.Context, taskID int64) {
	logger.FromContext(ctx).Debug().Msg("deferring task: endpoint lane is saturated")
	if err := e.taskQueue.Defer(ctx, taskID, e.opts.LaneRequeueDelay); err != nil {
		// The lock expires and the maintenance loop returns the task anyway.
		logger.FromContext(ctx).Warn().Err(err).Int64("task_id", taskID).Msg("failed to defer task")
	}
}

// deferTask pushes a task into the near future without counting an attempt,
// and asks the local snapshot to catch up.
func (e *Engine) deferTask(ctx context.Context, taskID int64, reason string) {
	logger.FromContext(ctx).Debug().Str("reason", reason).Msg("deferring task")

	if e.invalidator != nil {
		e.invalidator.Invalidate()
	}
	if err := e.taskQueue.Defer(ctx, taskID, e.opts.StaleSnapshotDelay); err != nil {
		// The lock expires and the maintenance loop returns the task anyway.
		logger.FromContext(ctx).Warn().Err(err).Int64("task_id", taskID).Msg("failed to defer task")
	}
}

func (e *Engine) complete(ctx context.Context, taskID int64) {
	if err := e.taskQueue.Complete(ctx, taskID); err != nil {
		logger.FromContext(ctx).Error().Err(err).Int64("task_id", taskID).Msg("failed to complete task")
	}
}

// responseBody prefers the endpoint's body and falls back to the transport
// error, so the portal shows something useful for a connection failure.
func responseBody(result DeliveryResult) string {
	if result.Body != "" {
		return result.Body
	}
	if result.Err != nil {
		return result.Err.Error()
	}
	return ""
}

func statusClass(code int) string {
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
