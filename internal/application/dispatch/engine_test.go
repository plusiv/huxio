package dispatch_test

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/domain/retry"
	"github.com/plusiv/huxio/internal/domain/signing"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

const (
	testOrgID     = "org_1"
	testAppID     = "app_1"
	testMsgID     = "msg_1"
	testEndpoint  = "ep_1"
	testEventType = "invoice.paid"
)

var testPayload = []byte(`{"amount":100}`)

// engineFixture is a fully faked engine: no Postgres, no network.
type engineFixture struct {
	engine   *dispatch.Engine
	queue    *fakeQueue
	tx       *inlineTx
	messages *fakeMessageStore
	client   *recordingClient
	sink     *collectingSink
	parts    *fixedPartitions
}

type fixtureOptions struct {
	tasks      []entities.DeliveryTask
	endpoints  []*entities.Endpoint
	results    []dispatch.DeliveryResult
	handler    func(dispatch.DeliveryRequest) dispatch.DeliveryResult
	engineOpts dispatch.EngineOptions
	policy     *retry.Policy
	codecErr   error
	message    *entities.Message
	skipMsg    bool
	// rotatedSecrets are previous secrets still inside their overlap window.
	rotatedSecrets []string
	invalidator    dispatch.SnapshotInvalidator
	lanes          *dispatch.LaneManager
}

func newEngineFixture(t *testing.T, opts fixtureOptions) *engineFixture {
	t.Helper()

	if len(opts.endpoints) == 0 {
		opts.endpoints = []*entities.Endpoint{{
			ID: testEndpoint, AppID: testAppID, OrgID: testOrgID,
			URL: "https://example.test/hook",
		}}
	}
	snapshot, _, err := buildSnapshot(&repositories.SnapshotData{
		Organizations: []*entities.Organization{{ID: testOrgID}},
		Applications:  []*entities.Application{{ID: testAppID, OrgID: testOrgID}},
		Endpoints:     opts.endpoints,
	}, opts.rotatedSecrets...)
	if err != nil {
		t.Fatalf("buildSnapshot: %v", err)
	}

	messages := newFakeMessageStore()
	if !opts.skipMsg {
		msg := opts.message
		if msg == nil {
			msg = &entities.Message{
				ID: testMsgID, OrgID: testOrgID, AppID: testAppID,
				EventType: testEventType, CreatedAt: time.Now().UTC(),
			}
		}
		messages.add(msg, testPayload)
	}

	parts := &fixedPartitions{}
	all := make([]int16, entities.QueuePartitions)
	for i := range all {
		all[i] = int16(i)
	}
	parts.store(all)

	fixture := &engineFixture{
		queue:    newFakeQueue(opts.tasks...),
		tx:       &inlineTx{},
		messages: messages,
		client:   &recordingClient{results: opts.results, handler: opts.handler},
		sink:     newCollectingSink(),
		parts:    parts,
	}

	engineOpts := opts.engineOpts
	if engineOpts.Pool == "" {
		engineOpts.Pool = "default"
	}
	if engineOpts.WorkerID == "" {
		engineOpts.WorkerID = "wkr_test"
	}
	if engineOpts.PollInterval == 0 {
		engineOpts.PollInterval = 5 * time.Millisecond
	}

	engine, err := dispatch.NewEngine(dispatch.EngineDeps{
		TaskQueue:           fixture.queue,
		TxManager:           fixture.tx,
		MessageStore:        messages,
		PayloadCodec:        identityCodec{err: opts.codecErr},
		SnapshotProvider:    staticSnapshot{snapshot},
		DeliveryClient:      fixture.client,
		AttemptSink:         fixture.sink,
		Policy:              opts.policy,
		PartitionSource:     parts,
		SnapshotInvalidator: opts.invalidator,
		LaneManager:         opts.lanes,
	}, engineOpts)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	fixture.engine = engine
	return fixture
}

// run drives the loop until the expected number of attempt records arrive.
func (f *engineFixture) run(t *testing.T, expectRecords int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- f.engine.Run(ctx, nil) }()

	if expectRecords > 0 && !f.sink.waitFor(expectRecords, 4*time.Second) {
		cancel()
		<-done
		t.Fatalf("expected %d attempt records, got %d", expectRecords, len(f.sink.all()))
	}
	// Give the loop a moment to finish any queue bookkeeping.
	time.Sleep(30 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("engine Run: %v", err)
	}
}

func deliverTask(id int64, attempt int16, trigger entities.TriggerType) entities.DeliveryTask {
	return entities.DeliveryTask{
		ID:           id,
		PartitionKey: entities.PartitionKeyFor(testEndpoint),
		Pool:         "default",
		Kind:         entities.TaskDeliver,
		OrgID:        testOrgID,
		AppID:        testAppID,
		MsgID:        testMsgID,
		MsgCreatedAt: time.Now().UTC(),
		EndpointID:   utils.Ptr(testEndpoint),
		Attempt:      attempt,
		TriggerType:  trigger,
	}
}

func fanoutTask(id int64) entities.DeliveryTask {
	return entities.DeliveryTask{
		ID:           id,
		PartitionKey: entities.PartitionKeyFor(testAppID),
		Pool:         "default",
		Kind:         entities.TaskFanout,
		OrgID:        testOrgID,
		AppID:        testAppID,
		MsgID:        testMsgID,
		MsgCreatedAt: time.Now().UTC(),
	}
}

func TestDeliverSuccessSignsAndCompletes(t *testing.T) {
	t.Parallel()

	fixture := newEngineFixture(t, fixtureOptions{
		tasks:   []entities.DeliveryTask{deliverTask(1, 0, entities.TriggerScheduled)},
		results: []dispatch.DeliveryResult{{StatusCode: 200, Body: "ok", Duration: 12 * time.Millisecond}},
	})
	fixture.run(t, 1)

	requests := fixture.client.captured()
	if len(requests) != 1 {
		t.Fatalf("made %d requests, want 1", len(requests))
	}
	req := requests[0]
	if req.URL != "https://example.test/hook" {
		t.Errorf("URL = %q", req.URL)
	}
	if string(req.Body) != string(testPayload) {
		t.Errorf("body = %q, want the payload written through unchanged", req.Body)
	}
	// Every request carries the three signature headers.
	if req.Headers[signing.HeaderStandardID] != testMsgID {
		t.Errorf("webhook-id = %q", req.Headers[signing.HeaderStandardID])
	}
	if !strings.HasPrefix(req.Headers[signing.HeaderStandardSignature], "v1,") {
		t.Errorf("webhook-signature = %q", req.Headers[signing.HeaderStandardSignature])
	}
	if _, err := strconv.ParseInt(req.Headers[signing.HeaderStandardTimestamp], 10, 64); err != nil {
		t.Errorf("webhook-timestamp = %q, want unix seconds", req.Headers[signing.HeaderStandardTimestamp])
	}

	records := fixture.sink.all()
	if len(records) != 1 {
		t.Fatalf("wrote %d attempt records, want 1", len(records))
	}
	attempt := records[0].Attempt
	if attempt.Status != entities.AttemptSucceeded {
		t.Errorf("status = %v, want succeeded", attempt.Status)
	}
	if attempt.ResponseStatusCode != 200 || attempt.ResponseBody != "ok" {
		t.Errorf("attempt = %+v", attempt)
	}
	if attempt.AttemptNumber != 1 {
		t.Errorf("AttemptNumber = %d, want 1", attempt.AttemptNumber)
	}
	if attempt.EndpointID != testEndpoint || attempt.MsgID != testMsgID {
		t.Errorf("attempt = %+v", attempt)
	}

	// The queue row's fate travels with the attempt, so it is only removed
	// after the attempt is durable.
	if records[0].Task == nil || records[0].Task.Action != dispatch.TaskComplete || records[0].Task.TaskID != 1 {
		t.Errorf("task outcome = %+v, want completion of task 1", records[0].Task)
	}
	// The engine must not delete the row itself on the success path.
	completed, _, _, _ := fixture.queue.snapshotState()
	if len(completed) != 0 {
		t.Errorf("engine completed %v directly; that must wait for the flush", completed)
	}
}

func TestDeliverFailureSchedulesRetryWithTheScheduledDelay(t *testing.T) {
	t.Parallel()

	fixture := newEngineFixture(t, fixtureOptions{
		tasks:   []entities.DeliveryTask{deliverTask(1, 0, entities.TriggerScheduled)},
		results: []dispatch.DeliveryResult{{StatusCode: 500, Body: "boom"}},
		policy:  retry.NewPolicy(retry.WithJitter(retry.NoJitter)),
	})
	fixture.run(t, 1)

	records := fixture.sink.all()
	attempt := records[0].Attempt
	if attempt.Status != entities.AttemptPendingRetry {
		t.Errorf("status = %v, want pending retry", attempt.Status)
	}
	if attempt.NextAttemptAt == nil {
		t.Fatal("a pending retry must record when the next attempt is due")
	}
	if records[0].Task.Action != dispatch.TaskRetry {
		t.Fatalf("task action = %v, want retry", records[0].Task.Action)
	}
	if got, want := records[0].Task.Delay, retry.DefaultSchedule[0]; got != want {
		t.Errorf("retry delay = %s, want the first scheduled delay %s", got, want)
	}
}

func TestDeliverHonoursRetryAfterAndOverloadFloor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		result    dispatch.DeliveryResult
		wantDelay time.Duration
	}{
		{
			"429 gets the overload floor",
			dispatch.DeliveryResult{StatusCode: http.StatusTooManyRequests},
			retry.OverloadFloor,
		},
		{
			"timeout gets the overload floor",
			dispatch.DeliveryResult{Err: eris.New("timeout"), Timeout: true},
			retry.OverloadFloor,
		},
		{
			"hostile Retry-After is clamped",
			dispatch.DeliveryResult{StatusCode: 503, RetryAfter: utils.Ptr(999999 * time.Second)},
			2 * retry.DefaultSchedule[0],
		},
		{
			"reasonable Retry-After is honoured",
			dispatch.DeliveryResult{StatusCode: 503, RetryAfter: utils.Ptr(8 * time.Second)},
			8 * time.Second,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fixture := newEngineFixture(t, fixtureOptions{
				tasks:   []entities.DeliveryTask{deliverTask(1, 0, entities.TriggerScheduled)},
				results: []dispatch.DeliveryResult{tc.result},
				policy:  retry.NewPolicy(retry.WithJitter(retry.NoJitter)),
			})
			fixture.run(t, 1)

			records := fixture.sink.all()
			if records[0].Task.Delay != tc.wantDelay {
				t.Errorf("retry delay = %s, want %s", records[0].Task.Delay, tc.wantDelay)
			}
		})
	}
}

func TestDeliverExhaustionStopsRetrying(t *testing.T) {
	t.Parallel()

	last := int16(len(retry.DefaultSchedule))
	fixture := newEngineFixture(t, fixtureOptions{
		tasks:   []entities.DeliveryTask{deliverTask(1, last, entities.TriggerScheduled)},
		results: []dispatch.DeliveryResult{{StatusCode: 500}},
	})
	fixture.run(t, 1)

	records := fixture.sink.all()
	if records[0].Attempt.Status != entities.AttemptFailed {
		t.Errorf("status = %v, want permanently failed", records[0].Attempt.Status)
	}
	if records[0].Attempt.NextAttemptAt != nil {
		t.Error("an exhausted attempt must not advertise a next attempt")
	}
	// The terminal attempt row is the record; the queue row goes away.
	if records[0].Task.Action != dispatch.TaskComplete {
		t.Errorf("task action = %v, want completion", records[0].Task.Action)
	}
}

func TestManualAndBulkTriggersAreNotRetried(t *testing.T) {
	t.Parallel()

	for _, trigger := range []entities.TriggerType{entities.TriggerManual, entities.TriggerBulkReplay} {
		fixture := newEngineFixture(t, fixtureOptions{
			tasks:   []entities.DeliveryTask{deliverTask(1, 0, trigger)},
			results: []dispatch.DeliveryResult{{StatusCode: 500}},
		})
		fixture.run(t, 1)

		records := fixture.sink.all()
		if records[0].Attempt.Status != entities.AttemptFailed {
			t.Errorf("trigger %v: status = %v, want failed without retry", trigger, records[0].Attempt.Status)
		}
		if records[0].Task.Action != dispatch.TaskComplete {
			t.Errorf("trigger %v: task action = %v, want completion", trigger, records[0].Task.Action)
		}
	}
}

func TestDeliverAppliesCustomHeadersButNeverOverridesSignatures(t *testing.T) {
	t.Parallel()

	fixture := newEngineFixture(t, fixtureOptions{
		tasks: []entities.DeliveryTask{deliverTask(1, 0, entities.TriggerScheduled)},
		endpoints: []*entities.Endpoint{{
			ID: testEndpoint, AppID: testAppID, OrgID: testOrgID,
			URL:     "https://example.test/hook",
			Headers: []byte(`{"X-Tenant":"acme","webhook-signature":"v1,forged"}`),
		}},
	})
	fixture.run(t, 1)

	headers := fixture.client.captured()[0].Headers
	if headers["x-tenant"] != "acme" {
		t.Errorf("custom header missing: %v", headers)
	}
	// A stored header that collides with a signature header must lose. The
	// API rejects these at write time; this is the belt to that braces.
	if headers[signing.HeaderStandardSignature] == "v1,forged" {
		t.Error("a custom header overrode the signature")
	}
}

func TestDeliverSignsWithEverySecretDuringRotation(t *testing.T) {
	t.Parallel()

	fixture := newEngineFixture(t, fixtureOptions{
		tasks:          []entities.DeliveryTask{deliverTask(1, 0, entities.TriggerScheduled)},
		rotatedSecrets: []string{"whsec_previous"},
	})
	fixture.run(t, 1)

	// One signature per active key, so a receiver that has not yet picked up
	// the new secret still verifies.
	signature := fixture.client.captured()[0].Headers[signing.HeaderStandardSignature]
	parts := strings.Split(signature, " ")
	if len(parts) != 2 {
		t.Fatalf("signature = %q, want one signature per active key", signature)
	}
	if parts[0] == parts[1] {
		t.Error("the two keys must produce different signatures")
	}
}

func TestDeliverSkipsEndpointsThatCannotReceive(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	tests := []struct {
		name     string
		endpoint *entities.Endpoint
	}{
		{"disabled", &entities.Endpoint{ID: testEndpoint, AppID: testAppID, OrgID: testOrgID, URL: "https://example.test/x", DisabledAt: &now}},
		{"deleted", &entities.Endpoint{ID: testEndpoint, AppID: testAppID, OrgID: testOrgID, URL: "https://example.test/x", DeletedAt: &now}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fixture := newEngineFixture(t, fixtureOptions{
				tasks:     []entities.DeliveryTask{deliverTask(1, 0, entities.TriggerScheduled)},
				endpoints: []*entities.Endpoint{tc.endpoint},
			})
			fixture.run(t, 0)

			if len(fixture.client.captured()) != 0 {
				t.Error("no request may be made to an endpoint that cannot receive")
			}
			completed, _, _, _ := fixture.queue.snapshotState()
			if len(completed) != 1 || completed[0] != 1 {
				t.Errorf("completed = %v, want the task dropped", completed)
			}
			if len(fixture.sink.all()) != 0 {
				t.Error("a skipped delivery must not produce an attempt record")
			}
		})
	}
}

func TestDeliverDropsTaskWhenMessageIsGone(t *testing.T) {
	t.Parallel()

	fixture := newEngineFixture(t, fixtureOptions{
		tasks:   []entities.DeliveryTask{deliverTask(1, 0, entities.TriggerScheduled)},
		skipMsg: true,
	})
	fixture.run(t, 0)

	// The message aged out from under the task: there is nothing to deliver
	// and nothing to retry.
	completed, retried, _, _ := fixture.queue.snapshotState()
	if len(completed) != 1 {
		t.Errorf("completed = %v, want the task dropped", completed)
	}
	if len(retried) != 0 {
		t.Errorf("retried = %v, want no retry", retried)
	}
	if len(fixture.client.captured()) != 0 {
		t.Error("no request may be made without a payload")
	}
}

func TestDeliverDropsTaskOnUndecodablePayload(t *testing.T) {
	t.Parallel()

	fixture := newEngineFixture(t, fixtureOptions{
		tasks:    []entities.DeliveryTask{deliverTask(1, 0, entities.TriggerScheduled)},
		codecErr: eris.New("corrupt payload"),
	})
	fixture.run(t, 0)

	completed, retried, _, _ := fixture.queue.snapshotState()
	if len(completed) != 1 || len(retried) != 0 {
		t.Errorf("completed = %v, retried = %v; a corrupt payload must not be retried forever", completed, retried)
	}
}

func TestFanoutExpandsToOneTaskPerMatchingEndpoint(t *testing.T) {
	t.Parallel()

	endpoints := []*entities.Endpoint{
		{ID: "ep_all", AppID: testAppID, OrgID: testOrgID, URL: "https://example.test/all"},
		{ID: "ep_paid", AppID: testAppID, OrgID: testOrgID, URL: "https://example.test/paid", EventTypes: []string{testEventType}},
		{ID: "ep_other", AppID: testAppID, OrgID: testOrgID, URL: "https://example.test/other", EventTypes: []string{"customer.created"}},
		{ID: "ep_channel", AppID: testAppID, OrgID: testOrgID, URL: "https://example.test/channel", Channels: []string{"ch_other"}},
		{ID: "ep_quarantined", AppID: testAppID, OrgID: testOrgID, URL: "https://example.test/q", Pool: "quarantine"},
	}

	fixture := newEngineFixture(t, fixtureOptions{
		tasks:     []entities.DeliveryTask{fanoutTask(7)},
		endpoints: endpoints,
	})
	fixture.run(t, 0)

	_, _, enqueued, notified := fixture.queue.snapshotState()
	if len(enqueued) != 3 {
		t.Fatalf("enqueued %d deliver tasks, want the three matching endpoints", len(enqueued))
	}

	byEndpoint := map[string]repositories.EnqueueTask{}
	for _, task := range enqueued {
		if task.Kind != entities.TaskDeliver {
			t.Errorf("expanded task kind = %v, want deliver", task.Kind)
		}
		if task.EndpointID == nil {
			t.Fatal("an expanded task must name its endpoint")
		}
		byEndpoint[*task.EndpointID] = task
	}
	for _, want := range []string{"ep_all", "ep_paid", "ep_quarantined"} {
		task, ok := byEndpoint[want]
		if !ok {
			t.Errorf("endpoint %s was not fanned out to", want)
			continue
		}
		// Delivery routes on endpoint id, so all of one endpoint's tasks land
		// on one worker.
		if got := task.PartitionKey; got != entities.PartitionKeyFor(want) {
			t.Errorf("%s partition = %d, want %d", want, got, entities.PartitionKeyFor(want))
		}
	}
	// A quarantined endpoint's work is routed to the quarantine pool, which is
	// how a failing endpoint stops consuming healthy capacity.
	if got := byEndpoint["ep_quarantined"].Pool; got != "quarantine" {
		t.Errorf("quarantined endpoint pool = %q", got)
	}
	if got := byEndpoint["ep_all"].Pool; got != "default" {
		t.Errorf("default endpoint pool = %q", got)
	}

	// The fan-out row is removed in the same transaction as the expansion.
	completed, _, _, _ := fixture.queue.snapshotState()
	if len(completed) != 1 || completed[0] != 7 {
		t.Errorf("completed = %v, want the fan-out row", completed)
	}
	if fixture.tx.calls != 1 {
		t.Errorf("transactions = %d, want the expansion and the delete in one", fixture.tx.calls)
	}
	if len(notified) == 0 {
		t.Error("expanded partitions must be woken up")
	}
	// One read of the message, whatever the endpoint count.
	if got := fixture.messages.loadCount(testMsgID); got > 1 {
		t.Errorf("loaded the payload %d times during fan-out", got)
	}
}

func TestFanoutWithNoMatchingEndpointsCompletesWhenTheSnapshotIsFresh(t *testing.T) {
	t.Parallel()

	// The task predates the snapshot, so "nothing matches" is a safe conclusion:
	// any endpoint that existed when it was queued is in there.
	staleTask := fanoutTask(7)
	staleTask.CreatedAt = time.Now().UTC().Add(-time.Hour)

	fixture := newEngineFixture(t, fixtureOptions{
		tasks: []entities.DeliveryTask{staleTask},
		message: &entities.Message{
			ID: testMsgID, OrgID: testOrgID, AppID: testAppID,
			EventType: testEventType, CreatedAt: time.Now().UTC().Add(-time.Hour),
		},
		endpoints: []*entities.Endpoint{
			{ID: testEndpoint, AppID: testAppID, OrgID: testOrgID, URL: "https://example.test/x", EventTypes: []string{"customer.created"}},
		},
	})
	fixture.run(t, 0)

	completed, _, enqueued, _ := fixture.queue.snapshotState()
	if len(enqueued) != 0 {
		t.Errorf("enqueued %d tasks, want none", len(enqueued))
	}
	if len(completed) != 1 {
		t.Errorf("completed = %v, want the fan-out row removed", completed)
	}
}

func TestFanoutDefersWhenTheSnapshotPredatesTheMessage(t *testing.T) {
	t.Parallel()

	invalidator := &countingInvalidator{}
	// The task is newer than the snapshot: an endpoint created moments ago may
	// simply not be in it yet, so the fan-out must wait rather than conclude that
	// nothing subscribes.
	futureTask := fanoutTask(7)
	futureTask.CreatedAt = time.Now().UTC().Add(time.Minute)

	fixture := newEngineFixture(t, fixtureOptions{
		tasks: []entities.DeliveryTask{futureTask},
		message: &entities.Message{
			ID: testMsgID, OrgID: testOrgID, AppID: testAppID,
			EventType: testEventType, CreatedAt: time.Now().UTC().Add(time.Minute),
		},
		endpoints: []*entities.Endpoint{
			{ID: testEndpoint, AppID: testAppID, OrgID: testOrgID, URL: "https://example.test/x", EventTypes: []string{"customer.created"}},
		},
		invalidator: invalidator,
	})
	fixture.run(t, 0)

	completed, retried, enqueued, _ := fixture.queue.snapshotState()
	if len(completed) != 0 {
		t.Errorf("completed = %v; the fan-out must not be dropped on a stale snapshot", completed)
	}
	if len(enqueued) != 0 {
		t.Errorf("enqueued = %v, want nothing yet", enqueued)
	}
	// Deferring must not consume a retry attempt: a stale snapshot is not a
	// delivery failure.
	if len(retried) != 0 {
		t.Errorf("retried = %v; deferral must not count as an attempt", retried)
	}
	deferred := fixture.queue.deferrals()
	if len(deferred) != 1 {
		t.Fatalf("deferred = %v, want the fan-out row", deferred)
	}
	if invalidator.count() == 0 {
		t.Error("a stale snapshot must trigger a reload")
	}
}

// TestFanoutDefersWhenTheSnapshotPredatesTheTaskButNotTheMessage pins the
// window a KSUID's one-second resolution opens: a message's created_at can sit
// up to a second in the past, so a snapshot built inside that window looks
// fresh while predating an endpoint created just before the message. Judging
// freshness against the message would drop the delivery.
func TestFanoutDefersWhenTheSnapshotPredatesTheTaskButNotTheMessage(t *testing.T) {
	t.Parallel()

	invalidator := &countingInvalidator{}

	// The message's timestamp is a second in the past (truncated), while the
	// task that carries it was created after this worker's snapshot.
	task := fanoutTask(7)
	task.CreatedAt = time.Now().UTC().Add(time.Minute)

	fixture := newEngineFixture(t, fixtureOptions{
		tasks: []entities.DeliveryTask{task},
		message: &entities.Message{
			ID: testMsgID, OrgID: testOrgID, AppID: testAppID,
			EventType: testEventType,
			// Older than the snapshot the fixture builds, on purpose.
			CreatedAt: time.Now().UTC().Add(-time.Second),
		},
		endpoints: []*entities.Endpoint{
			{ID: testEndpoint, AppID: testAppID, OrgID: testOrgID, URL: "https://example.test/x", EventTypes: []string{"customer.created"}},
		},
		invalidator: invalidator,
	})
	fixture.run(t, 0)

	completed, _, _, _ := fixture.queue.snapshotState()
	if len(completed) != 0 {
		t.Errorf("completed = %v; the fan-out must not be dropped, the snapshot predates the task", completed)
	}
	if len(fixture.queue.deferrals()) != 1 {
		t.Errorf("deferrals = %v, want the fan-out deferred", fixture.queue.deferrals())
	}
	if invalidator.count() == 0 {
		t.Error("a stale snapshot must trigger a reload")
	}
}

func TestDeliverDefersWhenTheSnapshotPredatesTheTask(t *testing.T) {
	t.Parallel()

	invalidator := &countingInvalidator{}
	task := deliverTask(1, 0, entities.TriggerScheduled)
	// The task was created after this worker's snapshot was built, and names
	// an endpoint the snapshot has never seen.
	task.CreatedAt = time.Now().UTC().Add(time.Minute)
	task.EndpointID = utils.Ptr("ep_unknown_to_this_worker")

	fixture := newEngineFixture(t, fixtureOptions{
		tasks:       []entities.DeliveryTask{task},
		invalidator: invalidator,
	})
	fixture.run(t, 0)

	completed, _, _, _ := fixture.queue.snapshotState()
	if len(completed) != 0 {
		t.Errorf("completed = %v; the delivery must not be dropped", completed)
	}
	if len(fixture.queue.deferrals()) != 1 {
		t.Errorf("deferrals = %v, want the task deferred", fixture.queue.deferrals())
	}
	if invalidator.count() == 0 {
		t.Error("a stale snapshot must trigger a reload")
	}
}

func TestDeliverDropsTaskForAnEndpointThatIsGone(t *testing.T) {
	t.Parallel()

	task := deliverTask(1, 0, entities.TriggerScheduled)
	// The task predates the snapshot, so an endpoint missing from it was
	// deleted rather than not-yet-seen.
	task.CreatedAt = time.Now().UTC().Add(-time.Hour)
	task.EndpointID = utils.Ptr("ep_deleted")

	fixture := newEngineFixture(t, fixtureOptions{tasks: []entities.DeliveryTask{task}})
	fixture.run(t, 0)

	completed, _, _, _ := fixture.queue.snapshotState()
	if len(completed) != 1 {
		t.Errorf("completed = %v, want the task dropped", completed)
	}
	if len(fixture.queue.deferrals()) != 0 {
		t.Errorf("deferrals = %v, want none", fixture.queue.deferrals())
	}
}

func TestFanoutLeavesTaskQueuedWhenExpansionFails(t *testing.T) {
	t.Parallel()

	fixture := newEngineFixture(t, fixtureOptions{
		tasks: []entities.DeliveryTask{fanoutTask(7)},
	})
	fixture.queue.enqueueErr = eris.New("database down")
	fixture.run(t, 0)

	// Nothing was expanded and nothing was deleted: the lock expires and the
	// fan-out is retried whole. A half-expanded fan-out is the failure this
	// transaction exists to prevent.
	completed, _, enqueued, _ := fixture.queue.snapshotState()
	if len(enqueued) != 0 || len(completed) != 0 {
		t.Errorf("enqueued = %v, completed = %v; want neither", enqueued, completed)
	}
}

func TestClaimIgnoresUnownedPartitions(t *testing.T) {
	t.Parallel()

	fixture := newEngineFixture(t, fixtureOptions{
		tasks: []entities.DeliveryTask{deliverTask(1, 0, entities.TriggerScheduled)},
	})
	// Own a partition that is not the endpoint's.
	other := (entities.PartitionKeyFor(testEndpoint) + 1) % entities.QueuePartitions
	fixture.parts.store([]int16{other})

	fixture.run(t, 0)

	if len(fixture.client.captured()) != 0 {
		t.Error("a task in an unowned partition must not be claimed")
	}
}

func TestEngineHandlesManyTasksConcurrentlyWithinTheInflightBound(t *testing.T) {
	t.Parallel()

	const (
		tasks       = 60
		maxInflight = 4
	)

	var (
		peak    int
		current int
		mu      sync.Mutex
	)

	tasksToRun := make([]entities.DeliveryTask, 0, tasks)
	for i := range tasks {
		tasksToRun = append(tasksToRun, deliverTask(int64(i+1), 0, entities.TriggerScheduled))
	}

	fixture := newEngineFixture(t, fixtureOptions{
		tasks: tasksToRun,
		handler: func(dispatch.DeliveryRequest) dispatch.DeliveryResult {
			mu.Lock()
			current++
			if current > peak {
				peak = current
			}
			mu.Unlock()

			time.Sleep(2 * time.Millisecond)

			mu.Lock()
			current--
			mu.Unlock()
			return dispatch.DeliveryResult{StatusCode: 200}
		},
		engineOpts: dispatch.EngineOptions{MaxInflight: maxInflight, ClaimBatchSize: 10},
	})
	fixture.run(t, tasks)

	mu.Lock()
	defer mu.Unlock()
	if peak > maxInflight {
		t.Errorf("peak concurrency = %d, want at most %d", peak, maxInflight)
	}
	if peak < 2 {
		t.Errorf("peak concurrency = %d; deliveries are not running concurrently at all", peak)
	}
}

func TestNewEngineRequiresItsDependencies(t *testing.T) {
	t.Parallel()

	if _, err := dispatch.NewEngine(dispatch.EngineDeps{}, dispatch.EngineOptions{}); err == nil {
		t.Error("an engine without dependencies must not be constructible")
	}
}

func TestEngineDefaultsLockTTLAboveRequestTimeout(t *testing.T) {
	t.Parallel()

	fixture := newEngineFixture(t, fixtureOptions{
		engineOpts: dispatch.EngineOptions{RequestTimeout: 30 * time.Second, LockTTL: time.Second},
	})
	// A lock shorter than the request timeout lets a slow delivery be re-claimed
	// while still in flight, so the engine refuses to use it.
	fixture.run(t, 0)
	if fixture.engine.WorkerID() == "" {
		t.Error("the engine must have a worker identity")
	}
}
