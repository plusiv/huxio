package attemptstream_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/plusiv/huxio/internal/adapters/inbound/http/attemptstream"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
)

type listFunc func(context.Context, usecases.ListAttemptsInput) (repositories.CursorResult[*entities.DeliveryAttempt], error)

func (f listFunc) ListAttempts(ctx context.Context, in usecases.ListAttemptsInput) (repositories.CursorResult[*entities.DeliveryAttempt], error) {
	return f(ctx, in)
}

func TestStreamRetriesQueriesAndAdvancesPastWrittenAttempts(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response := httptest.NewRecorder()
	input := usecases.ListAttemptsInput{
		OrgID: "org_1", AppIDOrUID: "app_1", EndpointID: "ep_1", MsgID: "msg_1", Limit: 50,
	}
	var initial, newest time.Time
	calls := 0
	list := listFunc(func(_ context.Context, in usecases.ListAttemptsInput) (repositories.CursorResult[*entities.DeliveryAttempt], error) {
		calls++
		if in.OrgID != input.OrgID || in.AppIDOrUID != input.AppIDOrUID ||
			in.EndpointID != input.EndpointID || in.MsgID != input.MsgID || in.Limit != input.Limit {
			t.Fatalf("stream changed caller scope or filters: %+v", in)
		}
		if in.Filters.After == nil {
			t.Fatal("stream must request only new attempts")
		}
		switch calls {
		case 1:
			initial = *in.Filters.After
			return repositories.CursorResult[*entities.DeliveryAttempt]{}, errors.New("temporary read failure")
		case 2:
			if !in.Filters.After.Equal(initial) {
				t.Error("a failed query must not advance the stream")
			}
			newest = initial.Add(2 * time.Second)
			return repositories.CursorResult[*entities.DeliveryAttempt]{Items: []*entities.DeliveryAttempt{
				{ID: "atmpt_new", CreatedAt: newest},
				{ID: "atmpt_old", CreatedAt: initial.Add(time.Second)},
			}}, nil
		default:
			if !in.Filters.After.Equal(newest.Add(time.Microsecond)) {
				t.Errorf("next query starts at %s, want past %s", in.Filters.After, newest)
			}
			cancel()
			return repositories.CursorResult[*entities.DeliveryAttempt]{}, nil
		}
	})

	attemptstream.Serve(ctx, response, list, input, func(attempt *entities.DeliveryAttempt) ([]byte, error) {
		return []byte(`{"id":"` + attempt.ID + `"}`), nil
	})

	if calls != 3 {
		t.Fatalf("stream ended after %d queries, want recovery and a follow-up query", calls)
	}
	want := "event: attempt\nid: atmpt_old\ndata: {\"id\":\"atmpt_old\"}\n\n" +
		"event: attempt\nid: atmpt_new\ndata: {\"id\":\"atmpt_new\"}\n\n"
	if got := response.Body.String(); got != want {
		t.Errorf("stream events = %q, want %q", got, want)
	}
	if response.Code != http.StatusOK || !response.Flushed {
		t.Errorf("stream status = %d, flushed = %t", response.Code, response.Flushed)
	}
	for name, want := range map[string]string{
		"Content-Type": "text/event-stream", "Cache-Control": "no-cache", "X-Accel-Buffering": "no",
	} {
		if got := response.Header().Get(name); !strings.EqualFold(got, want) {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
}
