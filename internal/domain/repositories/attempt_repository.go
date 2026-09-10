package repositories

import (
	"context"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
)

// AttemptFilters holds the predicates for delivery attempt queries. Every
// combination maps onto attempt_endpoint_idx or attempt_failed_idx.
type AttemptFilters struct {
	ID              *Filter[string]
	OrgID           *Filter[string]
	AppID           *Filter[string]
	MsgID           *Filter[string]
	EndpointID      *Filter[string]
	Status          *Filter[entities.AttemptStatus]
	StatusCodeClass *int
	CreatedAt       *Filter[time.Time]
	TriggerType     *Filter[entities.TriggerType]
}

// FailedDelivery names one message whose most recent attempt to an endpoint
// failed. It is what recover and replay expand into new deliveries.
type FailedDelivery struct {
	MsgID      string
	EndpointID string
}

// AttemptStats counts attempts by outcome over a window, for the endpoint
// stats endpoint and the portal.
type AttemptStats struct {
	Succeeded int64
	Pending   int64
	Failed    int64
}

// AttemptRepository is the port for delivery attempt persistence. Writes go
// through the batched writer, never one row at a time.
type AttemptRepository interface {
	// CopyAttempts bulk-inserts a batch of attempt records in one round trip.
	CopyAttempts(ctx context.Context, attempts []entities.DeliveryAttempt) (int64, error)

	// ListFailedDeliveries returns the deliveries whose most recent attempt
	// failed, which is what "replay everything that failed" means: a message
	// that failed once and then succeeded on retry is not replayed.
	ListFailedDeliveries(ctx context.Context, filters AttemptFilters, limit int) ([]FailedDelivery, error)

	// Stats counts attempts by outcome for the supplied filters.
	Stats(ctx context.Context, filters AttemptFilters) (AttemptStats, error)

	GetAttempt(ctx context.Context, filters AttemptFilters, order ...OrderBy) (*entities.DeliveryAttempt, error)
	GetAttempts(ctx context.Context, filters AttemptFilters, cursor CursorPagination, order ...OrderBy) (CursorResult[*entities.DeliveryAttempt], error)
}
