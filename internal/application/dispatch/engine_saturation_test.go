package dispatch_test

import (
	"context"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/utils"
)

const (
	slowEndpoint = "ep_slow"
	fastEndpoint = "ep_fast"
	slowURL      = "https://slow.test/hook"
	fastURL      = "https://fast.test/hook"
)

func twoEndpoints() []*entities.Endpoint {
	return []*entities.Endpoint{
		{ID: slowEndpoint, AppID: testAppID, OrgID: testOrgID, URL: slowURL},
		{ID: fastEndpoint, AppID: testAppID, OrgID: testOrgID, URL: fastURL},
	}
}

func deliverTaskFor(id int64, endpointID string) entities.DeliveryTask {
	task := deliverTask(id, 0, entities.TriggerScheduled)
	task.EndpointID = utils.Ptr(endpointID)
	task.PartitionKey = entities.PartitionKeyFor(endpointID)
	return task
}

// One endpoint that never answers must not take the rest of the worker with
// it. Its lane holds one request; the tasks queued behind that lane must not
// sit on the worker's in-flight budget while they wait, or the budget fills
// with waiters and every other endpoint's work stays unclaimed until a waiter
// gives up. That is head-of-line blocking across tenants, and it is exactly
// what the lanes exist to rule out. Measured against the binary: with a fifth
// of tenants tarpitted, healthy p99 went from 17ms to 3s.
func TestSaturatedLaneDoesNotStallOtherEndpoints(t *testing.T) {
	t.Parallel()

	const maxInflight = 4

	release := make(chan struct{})
	fastDone := make(chan struct{}, 8)

	tasks := []entities.DeliveryTask{
		deliverTaskFor(1, slowEndpoint), deliverTaskFor(2, slowEndpoint),
		deliverTaskFor(3, slowEndpoint), deliverTaskFor(4, slowEndpoint),
		deliverTaskFor(5, fastEndpoint), deliverTaskFor(6, fastEndpoint),
		deliverTaskFor(7, fastEndpoint), deliverTaskFor(8, fastEndpoint),
	}

	fixture := newEngineFixture(t, fixtureOptions{
		tasks:     tasks,
		endpoints: twoEndpoints(),
		handler: func(req dispatch.DeliveryRequest) dispatch.DeliveryResult {
			if req.URL == slowURL {
				<-release
				return dispatch.DeliveryResult{StatusCode: 200}
			}
			fastDone <- struct{}{}
			return dispatch.DeliveryResult{StatusCode: 200}
		},
		lanes: dispatch.NewLaneManager(dispatch.LaneManagerOptions{
			Defaults: dispatch.LaneOptions{InitialConcurrency: 1, MaxConcurrency: 1},
		}),
		engineOpts: dispatch.EngineOptions{
			MaxInflight:     maxInflight,
			ClaimBatchSize:  10,
			LaneWaitTimeout: 2 * time.Second,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fixture.engine.Run(ctx, nil) }()

	// The four slow tasks are claimed first: one is on the wire, three are
	// waiting for its lane. The fast tasks must still get through promptly,
	// well inside the lane wait timeout.
	deadline := time.After(time.Second)
	for got := 0; got < 4; {
		select {
		case <-fastDone:
			got++
		case <-deadline:
			close(release)
			cancel()
			<-done
			t.Fatalf("only %d of 4 fast deliveries happened within 1s while a slow endpoint was saturated", got)
		}
	}

	close(release)
	if !fixture.sink.waitFor(8, 4*time.Second) {
		cancel()
		<-done
		t.Fatalf("expected 8 attempt records once the slow endpoint answered, got %d", len(fixture.sink.all()))
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("engine Run: %v", err)
	}
}

// A task deferred because its lane is full is a capacity problem, not a
// configuration problem: it must come back after the lane requeue delay, and
// it must not ask the config snapshot to reload.
func TestSaturatedLaneDefersWithTheLaneRequeueDelayAndKeepsTheSnapshot(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	invalidator := &countingInvalidator{}

	fixture := newEngineFixture(t, fixtureOptions{
		tasks:     []entities.DeliveryTask{deliverTaskFor(1, slowEndpoint), deliverTaskFor(2, slowEndpoint)},
		endpoints: twoEndpoints(),
		handler: func(dispatch.DeliveryRequest) dispatch.DeliveryResult {
			<-release
			return dispatch.DeliveryResult{StatusCode: 200}
		},
		lanes: dispatch.NewLaneManager(dispatch.LaneManagerOptions{
			Defaults: dispatch.LaneOptions{InitialConcurrency: 1, MaxConcurrency: 1},
		}),
		invalidator: invalidator,
		engineOpts: dispatch.EngineOptions{
			MaxInflight:        4,
			ClaimBatchSize:     10,
			LaneWaitTimeout:    20 * time.Millisecond,
			LaneRequeueDelay:   1500 * time.Millisecond,
			StaleSnapshotDelay: 250 * time.Millisecond,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fixture.engine.Run(ctx, nil) }()

	deadline := time.Now().Add(2 * time.Second)
	for len(fixture.queue.deferrals()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	cancel()
	<-done

	// Whichever of the two lost the race for the single slot is the one deferred.
	deferrals := fixture.queue.deferrals()
	if len(deferrals) != 1 {
		t.Fatalf("deferrals = %v, want exactly one task deferred", deferrals)
	}
	for id, delay := range deferrals {
		if delay != 1500*time.Millisecond {
			t.Errorf("task %d deferred by %s, want the lane requeue delay of 1.5s", id, delay)
		}
	}
	if n := invalidator.count(); n != 0 {
		t.Errorf("snapshot invalidated %d times for a saturated lane; a full lane says nothing about the config", n)
	}
}
