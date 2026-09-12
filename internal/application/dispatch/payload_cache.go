package dispatch

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
)

// PayloadCacheOptions configures the per-process payload cache.
type PayloadCacheOptions struct {
	// MaxBytes bounds the payload bytes held. Past it the least recently used
	// entries are evicted. A single payload larger than the bound is never
	// cached.
	MaxBytes int64
	// TTL is how long an entry is served. A fan-out's deliveries run within
	// milliseconds of it; the first retry, seconds later, is expected to miss
	// and read through.
	TTL time.Duration
	// Now lets a test drive the clock instead of sleeping.
	Now func() time.Time
	// OnHit and OnMiss feed the cache metrics.
	OnHit  func()
	OnMiss func()
}

// CachingMessageStore serves LoadPayload from memory when the payload was
// recently loaded on this worker, and reads through otherwise.
//
// A fan-out loads a message once and then queues one deliver task per
// matching endpoint; every one of those tasks needs the same payload bytes,
// and without the cache each is a round trip to Postgres: a message to ten
// endpoints costs eleven reads of one row. The fan-out seeds the cache, and
// the deliveries that follow it on this worker read from memory. Deliver
// tasks routed to another worker miss and read through, so the cache changes
// cost, never correctness.
//
// Cached bytes are the stored form, codec header included, exactly what
// LoadPayload returns. They are shared with every caller: nothing on the
// delivery path mutates a payload.
type CachingMessageStore struct {
	inner MessageStore
	opts  PayloadCacheOptions

	mu      sync.Mutex
	entries map[string]*list.Element
	// order is most recently used first.
	order *list.List
	bytes int64
}

type payloadEntry struct {
	id        string
	payload   []byte
	expiresAt time.Time
}

// NewCachingMessageStore wraps a store with a bounded, expiring payload cache.
func NewCachingMessageStore(inner MessageStore, opts PayloadCacheOptions) *CachingMessageStore {
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = 64 << 20
	}
	if opts.TTL <= 0 {
		opts.TTL = 30 * time.Second
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &CachingMessageStore{
		inner:   inner,
		opts:    opts,
		entries: make(map[string]*list.Element),
		order:   list.New(),
	}
}

// LoadMessage reads through and seeds the cache with the payload it carries.
func (s *CachingMessageStore) LoadMessage(ctx context.Context, id string, createdAt time.Time) (*entities.Message, error) {
	msg, err := s.inner.LoadMessage(ctx, id, createdAt)
	if err != nil {
		return nil, err
	}
	if msg.Payload != nil {
		s.put(msg.ID, msg.Payload)
	}
	return msg, nil
}

// LoadPayload serves from the cache when it can, and reads through and keeps
// the result when it cannot.
func (s *CachingMessageStore) LoadPayload(ctx context.Context, id string, createdAt time.Time) ([]byte, error) {
	if payload, ok := s.get(id); ok {
		if s.opts.OnHit != nil {
			s.opts.OnHit()
		}
		return payload, nil
	}
	if s.opts.OnMiss != nil {
		s.opts.OnMiss()
	}
	payload, err := s.inner.LoadPayload(ctx, id, createdAt)
	if err != nil {
		return nil, err
	}
	s.put(id, payload)
	return payload, nil
}

// Len reports how many payloads are cached.
func (s *CachingMessageStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.order.Len()
}

// Bytes reports how many payload bytes are cached.
func (s *CachingMessageStore) Bytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}

func (s *CachingMessageStore) get(id string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	el, ok := s.entries[id]
	if !ok {
		return nil, false
	}
	entry, _ := el.Value.(*payloadEntry)
	if !s.opts.Now().Before(entry.expiresAt) {
		s.removeLocked(el)
		return nil, false
	}
	s.order.MoveToFront(el)
	return entry.payload, true
}

func (s *CachingMessageStore) put(id string, payload []byte) {
	size := int64(len(payload))
	if size > s.opts.MaxBytes {
		// Not worth emptying the whole cache for one oversized payload.
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if el, ok := s.entries[id]; ok {
		s.removeLocked(el)
	}
	for s.bytes+size > s.opts.MaxBytes && s.order.Len() > 0 {
		s.removeLocked(s.order.Back())
	}
	el := s.order.PushFront(&payloadEntry{
		id:        id,
		payload:   payload,
		expiresAt: s.opts.Now().Add(s.opts.TTL),
	})
	s.entries[id] = el
	s.bytes += size
}

func (s *CachingMessageStore) removeLocked(el *list.Element) {
	entry, _ := el.Value.(*payloadEntry)
	s.order.Remove(el)
	delete(s.entries, entry.id)
	s.bytes -= int64(len(entry.payload))
}

var _ MessageStore = (*CachingMessageStore)(nil)
