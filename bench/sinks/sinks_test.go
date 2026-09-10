package sinks_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/plusiv/huxio/bench/sinks"
)

func post(t *testing.T, sink *sinks.Sink, timeout time.Duration) (*http.Response, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sink.URL(), strings.NewReader(`{"a":1}`))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	return sink.Client().Do(req)
}

func TestNullSinkAnswersImmediately(t *testing.T) {
	t.Parallel()

	sink := sinks.New(sinks.Options{Kind: sinks.KindNull})
	defer sink.Close()

	started := time.Now()
	resp, err := post(t, sink, 2*time.Second)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d", resp.StatusCode)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Errorf("the null sink took %s; it must be the fixed-cost baseline", elapsed)
	}

	stats := sink.Stats()
	if stats.Requests != 1 {
		t.Errorf("requests = %d", stats.Requests)
	}
	if stats.Bytes != int64(len(`{"a":1}`)) {
		t.Errorf("bytes = %d, want the request body length", stats.Bytes)
	}
}

func TestTarpitHoldsUntilTheSenderGivesUp(t *testing.T) {
	t.Parallel()

	sink := sinks.New(sinks.Options{Kind: sinks.KindTarpit, Delay: time.Hour})
	defer sink.Close()

	// The sender's own timeout is what ends a tarpitted request, which is the
	// behaviour the isolation scenario depends on.
	if _, err := post(t, sink, 200*time.Millisecond); err == nil {
		t.Fatal("expected the request to time out")
	}
	if got := sink.Stats().Requests; got != 1 {
		t.Errorf("requests = %d, want the held request counted", got)
	}
}

func TestTarpitReleasesOnClose(t *testing.T) {
	t.Parallel()

	sink := sinks.New(sinks.Options{Kind: sinks.KindTarpit, Delay: time.Hour})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp, err := post(t, sink, 30*time.Second)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()

	// Wait until the request is actually being held.
	deadline := time.Now().Add(5 * time.Second)
	for sink.Stats().Requests == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the tarpit never received the request")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Closing must not wait out an hour-long hold, or a benchmark run could
	// never shut down.
	closed := make(chan struct{})
	go func() {
		sink.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close blocked on a held request")
	}
	wg.Wait()
}

func TestFlakySinkFailsAtRoughlyTheConfiguredRate(t *testing.T) {
	t.Parallel()

	sink := sinks.New(sinks.Options{Kind: sinks.KindFlaky, FailureRate: 0.5})
	defer sink.Close()

	const requests = 400
	failures := 0
	for range requests {
		resp, err := post(t, sink, 5*time.Second)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		if resp.StatusCode == http.StatusInternalServerError {
			failures++
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	// Loose bounds: this asserts the knob works, not that the RNG is fair.
	if failures < requests/4 || failures > 3*requests/4 {
		t.Errorf("%d of %d requests failed at a 50%% rate", failures, requests)
	}
	if got := sink.Stats().Failed; int(got) != failures {
		t.Errorf("sink counted %d failures, client saw %d", got, failures)
	}
}

func TestFlakySinkAlwaysFailsAtRateOne(t *testing.T) {
	t.Parallel()

	sink := sinks.New(sinks.Options{Kind: sinks.KindFlaky, FailureRate: 1})
	defer sink.Close()

	for range 20 {
		resp, err := post(t, sink, 5*time.Second)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500 at a failure rate of one", resp.StatusCode)
		}
		_ = resp.Body.Close()
	}
}

func TestSlowlorisTricklesItsBody(t *testing.T) {
	t.Parallel()

	sink := sinks.New(sinks.Options{Kind: sinks.KindSlowloris, Delay: 2 * time.Second})
	defer sink.Close()

	// The headers arrive at once; the body does not. A sender that reads the
	// body without a deadline hangs here, which is the point.
	resp, err := post(t, sink, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d", resp.StatusCode)
	}
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Error("reading the whole trickled body must not complete within the client deadline")
	}
}

func TestTLSVariant(t *testing.T) {
	t.Parallel()

	sink := sinks.New(sinks.Options{Kind: sinks.KindNull, TLS: true})
	defer sink.Close()

	if !strings.HasPrefix(sink.URL(), "https://") {
		t.Fatalf("URL = %q, want https", sink.URL())
	}
	if sink.TLSConfig() == nil {
		t.Error("a TLS sink must expose a trustable config, or nothing can call it")
	}

	resp, err := post(t, sink, 5*time.Second)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d", resp.StatusCode)
	}
}

func TestSinkTracksPeakConcurrency(t *testing.T) {
	t.Parallel()

	sink := sinks.New(sinks.Options{Kind: sinks.KindTarpit, Delay: 2 * time.Second})
	defer sink.Close()

	const concurrent = 5
	var wg sync.WaitGroup
	for range concurrent {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := post(t, sink, 10*time.Second)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	// Peak concurrency is how the isolation scenario proves a lane's bound
	// was respected.
	if got := sink.Stats().MaxInFlight; got < 2 {
		t.Errorf("MaxInFlight = %d, want the concurrent requests to overlap", got)
	}
	if got := sink.Stats().MaxInFlight; got > concurrent {
		t.Errorf("MaxInFlight = %d, want at most %d", got, concurrent)
	}
}
