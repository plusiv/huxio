package dispatch

import (
	"context"
	"sync"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/rotisserie/eris"
)

// AttemptWriterOptions configures the batched writer.
type AttemptWriterOptions struct {
	// BufferSize is the channel depth. When it fills, Enqueue blocks; records
	// are never dropped and the buffer never grows.
	BufferSize int
	// BatchSize flushes as soon as this many records are buffered.
	BatchSize int
	// FlushInterval flushes this long after the first record of a batch, so a
	// trickle of deliveries is still recorded promptly.
	FlushInterval time.Duration
}

// Histogram and Gauge are the metric ports the writer reports through.
type Histogram interface {
	Observe(float64)
}

// Gauge reports a single value.
type Gauge interface {
	Set(float64)
}

// AttemptWriter batches attempt records into one COPY per hundreds of
// deliveries, which is the largest single throughput win in the design. A
// single goroutine owns the flush, so ordering between an attempt and the
// removal of its queue row is guaranteed.
type AttemptWriter struct {
	attemptRepo repositories.AttemptRepository
	taskQueue   Queue
	opts        AttemptWriterOptions

	records chan AttemptRecord

	batchSize Histogram
	depth     Gauge

	closeOnce sync.Once
	done      chan struct{}
}

// NewAttemptWriter builds the writer.
func NewAttemptWriter(
	attemptRepo repositories.AttemptRepository,
	taskQueue Queue,
	opts AttemptWriterOptions,
	batchSize Histogram,
	depth Gauge,
) *AttemptWriter {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 500
	}
	if opts.BufferSize < opts.BatchSize {
		opts.BufferSize = max(opts.BatchSize, 8192)
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = 5 * time.Millisecond
	}
	return &AttemptWriter{
		attemptRepo: attemptRepo,
		taskQueue:   taskQueue,
		opts:        opts,
		records:     make(chan AttemptRecord, opts.BufferSize),
		batchSize:   batchSize,
		depth:       depth,
		done:        make(chan struct{}),
	}
}

// Enqueue buffers a record. It blocks while the buffer is full rather than
// dropping the record or growing the buffer: a full buffer means Postgres is
// the bottleneck, and blocking is what propagates that backpressure up into
// task claiming.
func (w *AttemptWriter) Enqueue(ctx context.Context, record AttemptRecord) error {
	select {
	case w.records <- record:
		if w.depth != nil {
			w.depth.Set(float64(len(w.records)))
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return eris.New("attempt writer is closed")
	}
}

// Run drains the buffer until ctx is cancelled, then performs one final
// flush. Whatever is still buffered has already been delivered, so skipping
// that flush would lose the record of real HTTP requests on every deploy.
// Shutdown waits for it.
func (w *AttemptWriter) Run(ctx context.Context) error {
	batch := make([]AttemptRecord, 0, w.opts.BatchSize)
	timer := time.NewTimer(w.opts.FlushInterval)
	if !timer.Stop() {
		<-timer.C
	}
	timerRunning := false

	stopTimer := func() {
		if timerRunning && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerRunning = false
	}

	for {
		select {
		case record := <-w.records:
			batch = append(batch, record)
			if w.depth != nil {
				w.depth.Set(float64(len(w.records)))
			}
			if len(batch) == 1 {
				timer.Reset(w.opts.FlushInterval)
				timerRunning = true
			}
			if len(batch) >= w.opts.BatchSize {
				stopTimer()
				batch = w.flush(ctx, batch)
			}

		case <-timer.C:
			timerRunning = false
			batch = w.flush(ctx, batch)

		case <-ctx.Done():
			stopTimer()
			// Drain whatever is still buffered before returning, on a context that is
			// no longer cancelled.
			flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()

			for {
				select {
				case record := <-w.records:
					batch = append(batch, record)
					if len(batch) >= w.opts.BatchSize {
						batch = w.flush(flushCtx, batch)
					}
				default:
					batch = w.flush(flushCtx, batch)
					w.closeOnce.Do(func() { close(w.done) })
					if w.depth != nil {
						w.depth.Set(0)
					}
					return nil
				}
			}
		}
	}
}

// flush writes a batch and then finalises the queue rows it covers. It returns
// the emptied batch slice for reuse.
func (w *AttemptWriter) flush(ctx context.Context, batch []AttemptRecord) []AttemptRecord {
	if len(batch) == 0 {
		return batch
	}
	log := logger.FromContext(ctx)

	attempts := make([]entities.DeliveryAttempt, 0, len(batch))
	for _, record := range batch {
		attempts = append(attempts, record.Attempt)
	}

	if _, err := w.attemptRepo.CopyAttempts(ctx, attempts); err != nil {
		// The attempt log is a log, not a lock. The queue rows are left untouched,
		// so their locks expire and the deliveries are retried: at-least-once holds,
		// and receivers dedupe on message id.
		log.Error().Err(err).Int("records", len(attempts)).Msg("attempt batch flush failed")
		return batch[:0]
	}
	if w.batchSize != nil {
		w.batchSize.Observe(float64(len(attempts)))
	}

	w.finalize(ctx, batch)
	return batch[:0]
}

// finalize applies the queue outcomes for a flushed batch, grouping the
// completions into one round trip.
func (w *AttemptWriter) finalize(ctx context.Context, batch []AttemptRecord) {
	log := logger.FromContext(ctx)

	completed := make([]int64, 0, len(batch))
	for _, record := range batch {
		if record.Task == nil {
			continue
		}
		switch record.Task.Action {
		case TaskComplete:
			completed = append(completed, record.Task.TaskID)
		case TaskRetry:
			if err := w.taskQueue.Retry(ctx, record.Task.TaskID, record.Task.Delay); err != nil {
				// The lock expires and the maintenance loop returns the task, so a failure
				// here costs latency, not a delivery.
				log.Error().Err(err).Int64("task_id", record.Task.TaskID).Msg("failed to schedule retry")
			}
		}
	}

	if len(completed) == 0 {
		return
	}
	if err := w.completeMany(ctx, completed); err != nil {
		log.Error().Err(err).Int("tasks", len(completed)).Msg("failed to complete tasks")
	}
}

// completeMany removes finished rows, preferring the batched form when the
// queue supports it.
func (w *AttemptWriter) completeMany(ctx context.Context, ids []int64) error {
	type batchCompleter interface {
		CompleteMany(ctx context.Context, ids []int64) error
	}
	if batched, ok := w.taskQueue.(batchCompleter); ok {
		return batched.CompleteMany(ctx, ids)
	}
	for _, id := range ids {
		if err := w.taskQueue.Complete(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

// Depth reports how many records are buffered, the primary saturation signal.
func (w *AttemptWriter) Depth() int { return len(w.records) }

var _ AttemptSink = (*AttemptWriter)(nil)
