package dispatch_test

import (
	"context"
	"sync"
	"time"

	"github.com/plusiv/huxio/internal/application/config"
	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/secrets"
	"github.com/rotisserie/eris"
)

// fakeQueue is an in-memory queue recording every operation the engine
// performs on it.
type fakeQueue struct {
	mu sync.Mutex

	ready    []entities.DeliveryTask
	claimErr error

	claims     int
	completed  []int64
	retried    map[int64]time.Duration
	released   []int64
	enqueued   []repositories.EnqueueTask
	notified   []int16
	deferred   map[int64]time.Duration
	enqueueErr error
}

func newFakeQueue(tasks ...entities.DeliveryTask) *fakeQueue {
	return &fakeQueue{ready: tasks, retried: map[int64]time.Duration{}, deferred: map[int64]time.Duration{}}
}

func (q *fakeQueue) Claim(_ context.Context, req repositories.ClaimRequest) ([]entities.DeliveryTask, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.claims++
	if q.claimErr != nil {
		return nil, q.claimErr
	}

	owned := map[int16]struct{}{}
	for _, p := range req.Partitions {
		owned[p] = struct{}{}
	}

	claimed := make([]entities.DeliveryTask, 0, req.Limit)
	remaining := make([]entities.DeliveryTask, 0, len(q.ready))
	for _, task := range q.ready {
		_, isOwned := owned[task.PartitionKey]
		if len(claimed) < req.Limit && isOwned && task.Pool == req.Pool {
			claimed = append(claimed, task)
			continue
		}
		remaining = append(remaining, task)
	}
	q.ready = remaining
	return claimed, nil
}

func (q *fakeQueue) Complete(_ context.Context, id int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.completed = append(q.completed, id)
	return nil
}

func (q *fakeQueue) CompleteMany(_ context.Context, ids []int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.completed = append(q.completed, ids...)
	return nil
}

func (q *fakeQueue) Retry(_ context.Context, id int64, delay time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.retried[id] = delay
	return nil
}

func (q *fakeQueue) Defer(_ context.Context, id int64, delay time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.deferred[id] = delay
	return nil
}

func (q *fakeQueue) Release(_ context.Context, ids []int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.released = append(q.released, ids...)
	return nil
}

func (q *fakeQueue) Enqueue(_ context.Context, tasks []repositories.EnqueueTask) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.enqueueErr != nil {
		return q.enqueueErr
	}
	q.enqueued = append(q.enqueued, tasks...)
	return nil
}

func (q *fakeQueue) Notify(_ context.Context, partitionKeys []int16) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.notified = append(q.notified, partitionKeys...)
	return nil
}

func (q *fakeQueue) claimCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.claims
}

func (q *fakeQueue) snapshotState() (completed []int64, retried map[int64]time.Duration, enqueued []repositories.EnqueueTask, notified []int16) {
	q.mu.Lock()
	defer q.mu.Unlock()

	retriedCopy := make(map[int64]time.Duration, len(q.retried))
	for id, delay := range q.retried {
		retriedCopy[id] = delay
	}
	return append([]int64(nil), q.completed...), retriedCopy,
		append([]repositories.EnqueueTask(nil), q.enqueued...),
		append([]int16(nil), q.notified...)
}

func (q *fakeQueue) deferrals() map[int64]time.Duration {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make(map[int64]time.Duration, len(q.deferred))
	for id, delay := range q.deferred {
		out[id] = delay
	}
	return out
}

// countingInvalidator records snapshot reload requests.
type countingInvalidator struct {
	mu    sync.Mutex
	calls int
}

func (i *countingInvalidator) Invalidate() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.calls++
}

func (i *countingInvalidator) count() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.calls
}

// inlineTx runs the unit of work without a transaction, and can be made to
// fail the commit.
type inlineTx struct {
	commitErr error
	calls     int
}

func (t *inlineTx) WithinTransaction(ctx context.Context, fn func(context.Context) error) error {
	t.calls++
	if err := fn(ctx); err != nil {
		return err
	}
	return t.commitErr
}

// fakeMessageStore serves messages and payloads by id.
type fakeMessageStore struct {
	mu       sync.Mutex
	messages map[string]*entities.Message
	payloads map[string][]byte
	loads    map[string]int
	loadErr  error
}

func newFakeMessageStore() *fakeMessageStore {
	return &fakeMessageStore{
		messages: map[string]*entities.Message{},
		payloads: map[string][]byte{},
		loads:    map[string]int{},
	}
}

func (s *fakeMessageStore) add(msg *entities.Message, payload []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages[msg.ID] = msg
	s.payloads[msg.ID] = payload
}

func (s *fakeMessageStore) LoadMessage(_ context.Context, id string, _ time.Time) (*entities.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	msg, ok := s.messages[id]
	if !ok {
		return nil, repositories.ErrNotFound
	}
	return msg, nil
}

func (s *fakeMessageStore) LoadPayload(_ context.Context, id string, _ time.Time) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	payload, ok := s.payloads[id]
	if !ok {
		return nil, repositories.ErrNotFound
	}
	s.loads[id]++
	return payload, nil
}

func (s *fakeMessageStore) loadCount(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loads[id]
}

// identityCodec stores payloads uncompressed, so tests can assert on the
// bytes that reach the wire.
type identityCodec struct {
	err error
}

func (c identityCodec) Decode(stored []byte) ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	return stored, nil
}

// recordingClient captures every outbound request and returns scripted
// results.
type recordingClient struct {
	mu       sync.Mutex
	requests []dispatch.DeliveryRequest
	results  []dispatch.DeliveryResult
	next     int
	// handler overrides the scripted results when set.
	handler func(dispatch.DeliveryRequest) dispatch.DeliveryResult
}

func (c *recordingClient) Deliver(_ context.Context, req dispatch.DeliveryRequest) dispatch.DeliveryResult {
	c.mu.Lock()
	c.requests = append(c.requests, req)
	handler := c.handler
	var result dispatch.DeliveryResult
	if handler == nil {
		result = dispatch.DeliveryResult{StatusCode: 200}
		if len(c.results) > 0 {
			result = c.results[min(c.next, len(c.results)-1)]
			c.next++
		}
	}
	c.mu.Unlock()

	// The handler runs outside the lock: holding it would serialise every
	// delivery and hide the engine's real concurrency.
	if handler != nil {
		return handler(req)
	}
	return result
}

func (c *recordingClient) captured() []dispatch.DeliveryRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]dispatch.DeliveryRequest(nil), c.requests...)
}

// collectingSink records what the engine wanted written, without batching.
type collectingSink struct {
	mu      sync.Mutex
	records []dispatch.AttemptRecord
	err     error
	signal  chan struct{}
}

func newCollectingSink() *collectingSink {
	return &collectingSink{signal: make(chan struct{}, 256)}
}

func (s *collectingSink) Enqueue(_ context.Context, record dispatch.AttemptRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.records = append(s.records, record)
	select {
	case s.signal <- struct{}{}:
	default:
	}
	return nil
}

func (s *collectingSink) all() []dispatch.AttemptRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]dispatch.AttemptRecord(nil), s.records...)
}

// waitFor blocks until n records have arrived or the deadline passes.
func (s *collectingSink) waitFor(n int, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		s.mu.Lock()
		count := len(s.records)
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

// staticSnapshot serves a prebuilt configuration snapshot.
type staticSnapshot struct {
	snapshot *config.Snapshot
}

func (s staticSnapshot) Current() *config.Snapshot { return s.snapshot }

// buildSnapshot seals every endpoint secret, including any rotated-out
// secrets supplied as plaintext, and assembles a snapshot.
func buildSnapshot(data *repositories.SnapshotData, rotated ...string) (*config.Snapshot, *secrets.Sealer, error) {
	key, err := secrets.GenerateKey()
	if err != nil {
		return nil, nil, err
	}
	sealer, err := secrets.NewSealer([]string{key})
	if err != nil {
		return nil, nil, err
	}

	for _, ep := range data.Endpoints {
		if len(ep.Secret.Sealed) == 0 {
			sealed, err := sealer.Seal([]byte("whsec_" + ep.ID))
			if err != nil {
				return nil, nil, err
			}
			ep.Secret = entities.SealedSecret{Sealed: sealed}
		}
		if ep.SecretType == "" {
			ep.SecretType = entities.SecretTypeHMAC256
		}
		for _, plaintext := range rotated {
			sealed, err := sealer.Seal([]byte(plaintext))
			if err != nil {
				return nil, nil, err
			}
			expires := time.Now().Add(time.Hour)
			ep.OldSecrets = append(ep.OldSecrets, entities.SealedSecret{Sealed: sealed, ExpiresAt: &expires})
		}
	}

	snapshot, problems := config.BuildSnapshot(data, sealer)
	if len(problems) > 0 {
		return nil, nil, eris.Errorf("snapshot build problems: %v", problems)
	}
	return snapshot, sealer, nil
}

// fixedPartitions owns exactly the partitions it is given.
type fixedPartitions struct {
	mu         sync.RWMutex
	partitions []int16
}

func (p *fixedPartitions) Partitions() []int16 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.partitions
}

func (p *fixedPartitions) store(partitions []int16) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.partitions = partitions
}
