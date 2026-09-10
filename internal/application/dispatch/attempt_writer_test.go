package dispatch_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/rotisserie/eris"
)

// fakeAttemptRepo records every COPY batch, and can block or fail one.
type fakeAttemptRepo struct {
	mu      sync.Mutex
	batches [][]entities.DeliveryAttempt
	err     error
	blockOn chan struct{}
	flushes atomic.Int32
	signal  chan struct{}
}

func newFakeAttemptRepo() *fakeAttemptRepo {
	return &fakeAttemptRepo{signal: make(chan struct{}, 128)}
}

func (r *fakeAttemptRepo) CopyAttempts(_ context.Context, attempts []entities.DeliveryAttempt) (int64, error) {
	if r.blockOn != nil {
		<-r.blockOn
	}

	r.mu.Lock()
	if r.err != nil {
		err := r.err
		r.mu.Unlock()
		return 0, err
	}
	r.batches = append(r.batches, append([]entities.DeliveryAttempt(nil), attempts...))
	r.mu.Unlock()

	r.flushes.Add(1)
	select {
	case r.signal <- struct{}{}:
	default:
	}
	return int64(len(attempts)), nil
}

func (r *fakeAttemptRepo) ListFailedDeliveries(context.Context, repositories.AttemptFilters, int) ([]repositories.FailedDelivery, error) {
	return nil, nil
}

func (r *fakeAttemptRepo) Stats(context.Context, repositories.AttemptFilters) (repositories.AttemptStats, error) {
	return repositories.AttemptStats{}, nil
}

func (r *fakeAttemptRepo) GetAttempt(context.Context, repositories.AttemptFilters, ...repositories.OrderBy) (*entities.DeliveryAttempt, error) {
	return nil, repositories.ErrNotFound
}

func (r *fakeAttemptRepo) GetAttempts(context.Context, repositories.AttemptFilters, repositories.CursorPagination, ...repositories.OrderBy) (repositories.CursorResult[*entities.DeliveryAttempt], error) {
	return repositories.CursorResult[*entities.DeliveryAttempt]{}, nil
}

func (r *fakeAttemptRepo) written() [][]entities.DeliveryAttempt {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]entities.DeliveryAttempt(nil), r.batches...)
}

func (r *fakeAttemptRepo) total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, batch := range r.batches {
		total += len(batch)
	}
	return total
}

// waitForFlushes blocks until at least n flushes have happened.
func (r *fakeAttemptRepo) waitForFlushes(n int32, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for r.flushes.Load() < n {
		select {
		case <-r.signal:
		case <-deadline:
			return false
		}
	}
	return true
}

// countingHistogram and countingGauge capture the writer's metric reports.
type countingHistogram struct {
	mu      sync.Mutex
	samples []float64
}

func (h *countingHistogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.samples = append(h.samples, v)
}

func (h *countingHistogram) observed() []float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]float64(nil), h.samples...)
}

type countingGauge struct {
	value atomic.Int64
}

func (g *countingGauge) Set(v float64) { g.value.Store(int64(v)) }

func attemptRecord(id string, taskID int64, action dispatch.TaskAction, delay time.Duration) dispatch.AttemptRecord {
	return dispatch.AttemptRecord{
		Attempt: entities.DeliveryAttempt{
			ID: id, OrgID: testOrgID, AppID: testAppID, MsgID: testMsgID,
			EndpointID: testEndpoint, URL: "https://example.test/hook",
			Status: entities.AttemptSucceeded, CreatedAt: time.Now().UTC(),
		},
		Task: &dispatch.TaskOutcome{TaskID: taskID, Action: action, Delay: delay},
	}
}

// startWriter runs a writer and returns a stop function that waits for the
// final flush, exactly as graceful shutdown does.
func startWriter(t *testing.T, writer *dispatch.AttemptWriter) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- writer.Run(ctx) }()

	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("writer Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("writer did not stop")
		}
	}
}

func TestWriterFlushesOnBatchSize(t *testing.T) {
	t.Parallel()

	repo := newFakeAttemptRepo()
	queue := newFakeQueue()
	batchSize := &countingHistogram{}
	depth := &countingGauge{}

	writer := dispatch.NewAttemptWriter(repo, queue, dispatch.AttemptWriterOptions{
		BatchSize:     10,
		BufferSize:    64,
		FlushInterval: time.Hour, // isolate the size trigger
	}, batchSize, depth)
	stop := startWriter(t, writer)
	defer stop()

	ctx := context.Background()
	for i := range 10 {
		if err := writer.Enqueue(ctx, attemptRecord("atmpt_"+string(rune('a'+i)), int64(i+1), dispatch.TaskComplete, 0)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	if !repo.waitForFlushes(1, 2*time.Second) {
		t.Fatal("a full batch must flush without waiting for the timer")
	}
	batches := repo.written()
	if len(batches) != 1 || len(batches[0]) != 10 {
		t.Fatalf("batches = %v, want one batch of ten", batchShape(batches))
	}
	// One COPY per ten records, not ten inserts.
	if observed := batchSize.observed(); len(observed) != 1 || observed[0] != 10 {
		t.Errorf("batch size histogram = %v, want a single observation of 10", observed)
	}
}

func TestWriterFlushesOnInterval(t *testing.T) {
	t.Parallel()

	repo := newFakeAttemptRepo()
	writer := dispatch.NewAttemptWriter(repo, newFakeQueue(), dispatch.AttemptWriterOptions{
		BatchSize:     500,
		BufferSize:    512,
		FlushInterval: 10 * time.Millisecond,
	}, nil, nil)
	stop := startWriter(t, writer)
	defer stop()

	// A trickle of deliveries must still be recorded promptly.
	if err := writer.Enqueue(context.Background(), attemptRecord("atmpt_1", 1, dispatch.TaskComplete, 0)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !repo.waitForFlushes(1, 2*time.Second) {
		t.Fatal("the flush interval must flush a partial batch")
	}
	if repo.total() != 1 {
		t.Errorf("wrote %d records, want 1", repo.total())
	}
}

func TestWriterCompletesQueueRowsOnlyAfterTheFlush(t *testing.T) {
	t.Parallel()

	repo := newFakeAttemptRepo()
	repo.blockOn = make(chan struct{})
	queue := newFakeQueue()

	writer := dispatch.NewAttemptWriter(repo, queue, dispatch.AttemptWriterOptions{
		BatchSize:     2,
		BufferSize:    16,
		FlushInterval: time.Hour,
	}, nil, nil)
	stop := startWriter(t, writer)
	defer stop()

	ctx := context.Background()
	if err := writer.Enqueue(ctx, attemptRecord("atmpt_1", 1, dispatch.TaskComplete, 0)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := writer.Enqueue(ctx, attemptRecord("atmpt_2", 2, dispatch.TaskRetry, 5*time.Second)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// The flush is blocked, so nothing may have happened to the queue rows: a
	// row removed before its attempt is durable is a delivery with no record.
	time.Sleep(50 * time.Millisecond)
	completed, retried, _, _ := queue.snapshotState()
	if len(completed) != 0 || len(retried) != 0 {
		t.Fatalf("queue was touched before the flush: completed = %v, retried = %v", completed, retried)
	}

	close(repo.blockOn)
	if !repo.waitForFlushes(1, 2*time.Second) {
		t.Fatal("flush did not happen")
	}

	deadline := time.After(2 * time.Second)
	for {
		completed, retried, _, _ = queue.snapshotState()
		if len(completed) == 1 && len(retried) == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("after the flush: completed = %v, retried = %v", completed, retried)
		case <-time.After(5 * time.Millisecond):
		}
	}
	if completed[0] != 1 {
		t.Errorf("completed = %v, want task 1", completed)
	}
	if retried[2] != 5*time.Second {
		t.Errorf("retried = %v, want task 2 delayed 5s", retried)
	}
}

func TestWriterLeavesQueueAloneWhenTheFlushFails(t *testing.T) {
	t.Parallel()

	repo := newFakeAttemptRepo()
	repo.err = eris.New("copy failed")
	queue := newFakeQueue()

	writer := dispatch.NewAttemptWriter(repo, queue, dispatch.AttemptWriterOptions{
		BatchSize:     1,
		BufferSize:    8,
		FlushInterval: time.Hour,
	}, nil, nil)
	stop := startWriter(t, writer)
	defer stop()

	if err := writer.Enqueue(context.Background(), attemptRecord("atmpt_1", 1, dispatch.TaskComplete, 0)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// The attempt log is a log, not a lock: the queue row keeps its lock, the
	// lock expires, and the delivery is retried. At-least-once holds.
	completed, retried, _, _ := queue.snapshotState()
	if len(completed) != 0 || len(retried) != 0 {
		t.Errorf("a failed flush must not touch the queue: completed = %v, retried = %v", completed, retried)
	}
}

func TestWriterFlushesEverythingOnShutdown(t *testing.T) {
	t.Parallel()

	repo := newFakeAttemptRepo()
	queue := newFakeQueue()
	writer := dispatch.NewAttemptWriter(repo, queue, dispatch.AttemptWriterOptions{
		BatchSize:     500,
		BufferSize:    512,
		FlushInterval: time.Hour, // nothing would flush on its own
	}, nil, nil)
	stop := startWriter(t, writer)

	ctx := context.Background()
	const buffered = 37
	for i := range buffered {
		if err := writer.Enqueue(ctx, attemptRecord("atmpt_"+string(rune('a'+i%26)), int64(i+1), dispatch.TaskComplete, 0)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	// Shutdown step 5. Skipping it loses attempt records on every deploy.
	stop()

	if got := repo.total(); got != buffered {
		t.Errorf("wrote %d records on shutdown, want all %d", got, buffered)
	}
	completed, _, _, _ := queue.snapshotState()
	if len(completed) != buffered {
		t.Errorf("completed %d tasks on shutdown, want %d", len(completed), buffered)
	}
}

func TestWriterEnqueueBlocksWhenFull(t *testing.T) {
	t.Parallel()

	repo := newFakeAttemptRepo()
	repo.blockOn = make(chan struct{})

	// A buffer of one, with the flush blocked: the second Enqueue has nowhere
	// to go and must block rather than drop the record or grow the buffer.
	writer := dispatch.NewAttemptWriter(repo, newFakeQueue(), dispatch.AttemptWriterOptions{
		BatchSize:     1,
		BufferSize:    1,
		FlushInterval: time.Hour,
	}, nil, nil)
	stop := startWriter(t, writer)
	defer func() {
		close(repo.blockOn)
		stop()
	}()

	ctx := context.Background()
	// Fill the pipeline: one record in flight in the blocked flush, one in the
	// buffer.
	for range 2 {
		if err := writer.Enqueue(ctx, attemptRecord("atmpt_x", 1, dispatch.TaskComplete, 0)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	blockedCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	err := writer.Enqueue(blockedCtx, attemptRecord("atmpt_blocked", 2, dispatch.TaskComplete, 0))
	if err == nil {
		t.Fatal("Enqueue must block while the buffer is full")
	}
	if !eris.Is(err, context.DeadlineExceeded) {
		t.Errorf("Enqueue error = %v, want the caller's context deadline", err)
	}
}

func TestWriterIgnoresRecordsWithoutTaskOutcomes(t *testing.T) {
	t.Parallel()

	repo := newFakeAttemptRepo()
	queue := newFakeQueue()
	writer := dispatch.NewAttemptWriter(repo, queue, dispatch.AttemptWriterOptions{
		BatchSize:     1,
		BufferSize:    8,
		FlushInterval: time.Hour,
	}, nil, nil)
	stop := startWriter(t, writer)
	defer stop()

	// A manual resend records an attempt without owning a queue row.
	record := attemptRecord("atmpt_1", 0, dispatch.TaskComplete, 0)
	record.Task = nil
	if err := writer.Enqueue(context.Background(), record); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !repo.waitForFlushes(1, 2*time.Second) {
		t.Fatal("flush did not happen")
	}

	completed, retried, _, _ := queue.snapshotState()
	if len(completed) != 0 || len(retried) != 0 {
		t.Errorf("queue was touched for a record with no task: completed = %v, retried = %v", completed, retried)
	}
}

func batchShape(batches [][]entities.DeliveryAttempt) []int {
	shape := make([]int, 0, len(batches))
	for _, batch := range batches {
		shape = append(shape, len(batch))
	}
	return shape
}
