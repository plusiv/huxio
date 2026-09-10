package harness

import (
	"testing"
	"time"
)

// A fast delivery can reach the sink before the ingest response has made it
// back to the sender and been recorded. The arrival must still be credited
// once the send is known, or the run under-counts exactly the deliveries that
// were quickest.
func TestObserveDeliveryBeforeSendIsRecorded(t *testing.T) {
	t.Parallel()

	h := &harness{
		sentAt:    map[string]sendRecord{},
		pending:   map[string][]time.Time{},
		latencies: map[string][]float64{},
		delivered: map[string]int64{},
	}

	sent := time.Now()
	h.observeDelivery("msg_early", sent.Add(3*time.Millisecond))
	if got := h.delivered["org_a"]; got != 0 {
		t.Fatalf("delivery credited before the send was known: %d", got)
	}

	h.recordSend("org_a", "msg_early", sent)

	if got := h.delivered["org_a"]; got != 1 {
		t.Fatalf("delivered = %d, want 1 after the send is recorded", got)
	}
	if n := len(h.latencies["org_a"]); n != 1 || h.latencies["org_a"][0] < 2.9 || h.latencies["org_a"][0] > 3.1 {
		t.Fatalf("latency samples = %v, want one sample of about 3ms", h.latencies["org_a"])
	}

	// A second arrival for a message the harness never sent stays uncounted.
	h.observeDelivery("msg_unknown", sent)
	h.resetLatencies()
	if len(h.pending) != 0 {
		t.Fatalf("pending arrivals survived a reset: %v", h.pending)
	}
}
