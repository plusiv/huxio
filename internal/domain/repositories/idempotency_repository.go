package repositories

import (
	"context"
	"time"

	"github.com/plusiv/huxio/internal/domain/entities"
)

// IdempotencyRepository is the port for cached POST responses.
type IdempotencyRepository interface {
	// Begin inserts an in-progress row. It returns (nil, true) when the caller
	// won the race and should handle the request, or (existing, false) when a
	// record already exists: complete records replay their response, and
	// in-progress ones produce a 409.
	Begin(ctx context.Context, key string, lockTTL time.Duration) (*entities.IdempotencyRecord, bool, error)
	// Complete stores the response to replay for subsequent duplicates.
	Complete(ctx context.Context, key string, statusCode int16, response []byte, ttl time.Duration) error
	// Abort removes an in-progress row so a failed request can be retried.
	Abort(ctx context.Context, key string) error
	// PurgeExpired deletes records past their expiry.
	PurgeExpired(ctx context.Context) (int64, error)
}
