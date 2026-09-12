package dispatch_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/domain/entities"
)

func storedPayload(body string) []byte {
	// One codec header byte, then the bytes, as the message repo stores them.
	return append([]byte{0}, []byte(body)...)
}

func TestCachingStoreServesDeliveriesFromTheFanoutLoad(t *testing.T) {
	t.Parallel()

	inner := newFakeMessageStore()
	msg := &entities.Message{ID: "msg_1", Payload: storedPayload(`{"a":1}`)}
	inner.add(msg, msg.Payload)
	store := dispatch.NewCachingMessageStore(inner, dispatch.PayloadCacheOptions{})

	// The fan-out reads the message once...
	if _, err := store.LoadMessage(context.Background(), "msg_1", time.Time{}); err != nil {
		t.Fatalf("LoadMessage: %v", err)
	}
	// ...and every deliver task it queued reads the payload.
	for i := 0; i < 10; i++ {
		got, err := store.LoadPayload(context.Background(), "msg_1", time.Time{})
		if err != nil {
			t.Fatalf("LoadPayload: %v", err)
		}
		if string(got) != string(msg.Payload) {
			t.Fatalf("payload = %q, want %q", got, msg.Payload)
		}
	}
	if n := inner.loadCount("msg_1"); n != 0 {
		t.Errorf("payload read from the store %d times after the fan-out already loaded it, want 0", n)
	}
}

func TestCachingStoreReadsThroughOnAMissAndKeepsTheResult(t *testing.T) {
	t.Parallel()

	var hits, misses atomic.Int32
	inner := newFakeMessageStore()
	inner.add(&entities.Message{ID: "msg_1"}, storedPayload(`{}`))
	store := dispatch.NewCachingMessageStore(inner, dispatch.PayloadCacheOptions{
		OnHit:  func() { hits.Add(1) },
		OnMiss: func() { misses.Add(1) },
	})

	// No fan-out on this worker seeded it: the first read goes to the store.
	for i := 0; i < 3; i++ {
		if _, err := store.LoadPayload(context.Background(), "msg_1", time.Time{}); err != nil {
			t.Fatalf("LoadPayload: %v", err)
		}
	}
	if n := inner.loadCount("msg_1"); n != 1 {
		t.Errorf("store reads = %d, want 1: the miss must populate the cache", n)
	}
	if hits.Load() != 2 || misses.Load() != 1 {
		t.Errorf("hits/misses = %d/%d, want 2/1", hits.Load(), misses.Load())
	}
}

func TestCachingStoreExpiresEntries(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_000_000, 0)
	inner := newFakeMessageStore()
	msg := &entities.Message{ID: "msg_1", Payload: storedPayload(`{}`)}
	inner.add(msg, msg.Payload)
	store := dispatch.NewCachingMessageStore(inner, dispatch.PayloadCacheOptions{
		TTL: time.Second,
		Now: func() time.Time { return now },
	})

	if _, err := store.LoadMessage(context.Background(), "msg_1", time.Time{}); err != nil {
		t.Fatalf("LoadMessage: %v", err)
	}
	if _, err := store.LoadPayload(context.Background(), "msg_1", time.Time{}); err != nil {
		t.Fatalf("LoadPayload: %v", err)
	}
	if n := inner.loadCount("msg_1"); n != 0 {
		t.Fatalf("store reads = %d before expiry, want 0", n)
	}

	// The first retry lands seconds later, well past the TTL: it reads through.
	now = now.Add(2 * time.Second)
	if _, err := store.LoadPayload(context.Background(), "msg_1", time.Time{}); err != nil {
		t.Fatalf("LoadPayload after expiry: %v", err)
	}
	if n := inner.loadCount("msg_1"); n != 1 {
		t.Errorf("store reads = %d after expiry, want 1", n)
	}
}

func TestCachingStoreEvictsLeastRecentlyUsedPastMaxBytes(t *testing.T) {
	t.Parallel()

	inner := newFakeMessageStore()
	forty := storedPayload(strings.Repeat("x", 39))
	for _, id := range []string{"a", "b", "c"} {
		inner.add(&entities.Message{ID: id}, forty)
	}
	inner.add(&entities.Message{ID: "huge"}, storedPayload(strings.Repeat("x", 199)))
	store := dispatch.NewCachingMessageStore(inner, dispatch.PayloadCacheOptions{MaxBytes: 100})

	load := func(id string) {
		t.Helper()
		if _, err := store.LoadPayload(context.Background(), id, time.Time{}); err != nil {
			t.Fatalf("LoadPayload(%s): %v", id, err)
		}
	}

	load("a")
	load("b")
	load("a") // a is now the most recently used
	load("c") // 120 bytes would exceed the bound: b, the least recently used, goes

	if got := store.Bytes(); got != 80 {
		t.Errorf("cached bytes = %d, want 80 (two of three payloads)", got)
	}
	load("a")
	if n := inner.loadCount("a"); n != 1 {
		t.Errorf("a was read from the store %d times, want 1: it was recently used and must survive", n)
	}
	load("b")
	if n := inner.loadCount("b"); n != 2 {
		t.Errorf("b was read from the store %d times, want 2: it was the least recently used and must have been evicted", n)
	}

	// A payload larger than the whole cache is served but never kept.
	load("huge")
	load("huge")
	if n := inner.loadCount("huge"); n != 2 {
		t.Errorf("oversized payload read %d times, want 2: it must not be cached", n)
	}
	if got := store.Len(); got != 2 {
		t.Errorf("entries = %d after the oversized payload, want 2: it must not have evicted anything", got)
	}
}
