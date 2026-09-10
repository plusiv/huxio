// Package sinks holds the receiving ends of the benchmark harness: four tiny
// servers that behave like the four endpoint failure modes that matter. A
// benchmark a skeptic can reproduce is worth more than any number in a README,
// so these are deliberately trivial.
package sinks

import (
	"crypto/tls"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"
)

// Kind names a sink behaviour.
type Kind string

// The sink behaviours.
const (
	// KindNull answers 200 immediately: maximum throughput, fixed cost.
	KindNull Kind = "null"
	// KindTarpit answers 200 after a long delay: head-of-line blocking and
	// lane isolation.
	KindTarpit Kind = "tarpit"
	// KindFlaky answers 500 with a configurable probability: retry storms,
	// breaker behaviour, backoff.
	KindFlaky Kind = "flaky"
	// KindSlowloris trickles a response body: read timeouts and connection
	// leaks.
	KindSlowloris Kind = "slowloris"
)

// Options configures a sink.
type Options struct {
	Kind Kind
	// Delay is the tarpit's hold time and the slowloris's total trickle time.
	Delay time.Duration
	// FailureRate is the flaky sink's probability of answering 500, in [0,1].
	FailureRate float64
	// TLS serves over HTTPS, which is where handshake cost shows up and where
	// connection reuse either works or does not.
	TLS bool
	// Observer, when set, is called as each request arrives with the message id
	// from the signature headers. The harness uses it to measure
	// ingest-to-delivery latency per tenant without touching the database.
	Observer func(msgID string, at time.Time)
	// Now lets a test drive the timers forward instead of sleeping.
	Now func() time.Time
}

// Stats is what a sink observed.
type Stats struct {
	Requests    int64
	Bytes       int64
	Failed      int64
	MaxInFlight int64
}

// Sink is one running receiver.
type Sink struct {
	server *httptest.Server
	opts   Options

	requests    atomic.Int64
	bytes       atomic.Int64
	failed      atomic.Int64
	inFlight    atomic.Int64
	maxInFlight atomic.Int64

	// failureRate is adjustable so a scenario can let a flaky endpoint recover,
	// which is what the retry storm's second half measures.
	failureRate atomic.Pointer[float64]

	// release closes to let a tarpit's held requests go, so shutdown does not
	// wait out every delay.
	release chan struct{}
}

// New starts a sink. Callers must Close it.
func New(opts Options) *Sink {
	if opts.Kind == "" {
		opts.Kind = KindNull
	}
	if opts.Delay <= 0 {
		switch opts.Kind {
		case KindTarpit:
			opts.Delay = 30 * time.Second
		case KindSlowloris:
			opts.Delay = 60 * time.Second
		}
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	sink := &Sink{opts: opts, release: make(chan struct{})}
	rate := opts.FailureRate
	sink.failureRate.Store(&rate)

	handler := http.HandlerFunc(sink.serve)
	if opts.TLS {
		sink.server = httptest.NewTLSServer(handler)
	} else {
		sink.server = httptest.NewServer(handler)
	}
	return sink
}

// URL is the address to point endpoints at.
func (s *Sink) URL() string { return s.server.URL }

// Client returns an HTTP client that trusts this sink, for tests.
func (s *Sink) Client() *http.Client { return s.server.Client() }

// TLSConfig exposes the sink's certificate, for a delivery client that must
// trust a self-signed benchmark sink.
func (s *Sink) TLSConfig() *tls.Config {
	transport, ok := s.server.Client().Transport.(*http.Transport)
	if !ok || transport.TLSClientConfig == nil {
		return nil
	}
	return transport.TLSClientConfig.Clone()
}

// Stats reports what this sink has seen.
func (s *Sink) Stats() Stats {
	return Stats{
		Requests:    s.requests.Load(),
		Bytes:       s.bytes.Load(),
		Failed:      s.failed.Load(),
		MaxInFlight: s.maxInFlight.Load(),
	}
}

// SetFailureRate adjusts the flaky sink's failure probability, which is how a
// scenario makes a broken endpoint recover without tearing it down.
func (s *Sink) SetFailureRate(rate float64) {
	clamped := min(max(rate, 0), 1)
	s.failureRate.Store(&clamped)
}

func (s *Sink) currentFailureRate() float64 {
	if rate := s.failureRate.Load(); rate != nil {
		return *rate
	}
	return s.opts.FailureRate
}

// Close releases any held requests and shuts the sink down.
func (s *Sink) Close() {
	select {
	case <-s.release:
	default:
		close(s.release)
	}
	s.server.Close()
}

func (s *Sink) serve(w http.ResponseWriter, r *http.Request) {
	inFlight := s.inFlight.Add(1)
	for {
		observed := s.maxInFlight.Load()
		if inFlight <= observed || s.maxInFlight.CompareAndSwap(observed, inFlight) {
			break
		}
	}
	defer s.inFlight.Add(-1)

	written, _ := io.Copy(io.Discard, r.Body)
	s.bytes.Add(written)
	s.requests.Add(1)

	if s.opts.Observer != nil {
		msgID := r.Header.Get("webhook-id")
		if msgID == "" {
			msgID = r.Header.Get("huxio-id")
		}
		s.opts.Observer(msgID, s.opts.Now())
	}

	switch s.opts.Kind {
	case KindTarpit:
		// Hold the request. The sender's own timeout is what ends this.
		select {
		case <-time.After(s.opts.Delay):
		case <-r.Context().Done():
			return
		case <-s.release:
		}
		w.WriteHeader(http.StatusOK)

	case KindFlaky:
		if rand.Float64() < s.currentFailureRate() {
			s.failed.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("flaky sink failure"))
			return
		}
		w.WriteHeader(http.StatusOK)

	case KindSlowloris:
		// Trickle the body: the response starts immediately and never quite
		// finishes, which is what catches a missing read timeout.
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		controller := http.NewResponseController(w)

		const chunks = 60
		interval := s.opts.Delay / chunks
		for range chunks {
			if _, err := w.Write([]byte("still here...\n")); err != nil {
				return
			}
			_ = controller.Flush()
			select {
			case <-time.After(interval):
			case <-r.Context().Done():
				return
			case <-s.release:
				return
			}
		}

	default:
		w.WriteHeader(http.StatusOK)
	}
}

// Listener exposes the sink's address, for tests that need the port.
func (s *Sink) Listener() net.Addr { return s.server.Listener.Addr() }
