package entities

import "time"

// IdempotencyStatus is the lifecycle state of a cached POST response.
type IdempotencyStatus int16

// Idempotency record states.
const (
	// IdempotencyInProgress is a short-lived lock that makes two concurrent
	// duplicates safe: the second gets a 409 rather than a second message.
	IdempotencyInProgress IdempotencyStatus = 0
	// IdempotencyComplete carries the stored response to replay verbatim.
	IdempotencyComplete IdempotencyStatus = 1
)

// IdempotencyRecord is the cached response for a POST carrying an
// Idempotency-Key, so a client that retries after a timeout gets the original
// answer instead of creating a second message.
type IdempotencyRecord struct {
	Key        string
	Status     IdempotencyStatus
	Response   []byte
	StatusCode *int16
	ExpiresAt  time.Time
	CreatedAt  time.Time
}
