package entities

import "time"

// AttemptStatus is the outcome recorded for one delivery attempt. The numeric
// values are persisted, so they may never be renumbered.
type AttemptStatus int16

// Attempt outcomes.
const (
	// AttemptSucceeded means the endpoint answered with a 2xx.
	AttemptSucceeded AttemptStatus = 0
	// AttemptPendingRetry means the attempt failed and another is scheduled.
	AttemptPendingRetry AttemptStatus = 1
	// AttemptFailed means the attempt failed and no further retry will happen.
	AttemptFailed AttemptStatus = 2
)

// TriggerType records what caused an attempt. Manual and bulk attempts are
// never retried on failure.
type TriggerType int16

// Attempt triggers.
const (
	// TriggerScheduled is the normal queue-driven delivery.
	TriggerScheduled TriggerType = 0
	// TriggerManual is a single resend from the API or portal.
	TriggerManual TriggerType = 1
	// TriggerBulkReplay is one delivery of a filtered bulk replay.
	TriggerBulkReplay TriggerType = 2
)

// Retryable reports whether a failure of an attempt with this trigger should
// be re-enqueued.
func (t TriggerType) Retryable() bool { return t == TriggerScheduled }

// DeliveryAttempt is one HTTP request we made to one endpoint for one message,
// and what came back. Append-only and the highest-volume table in the system,
// which is why it is written in batches.
type DeliveryAttempt struct {
	ID                 string        `json:"id"`
	CreatedAt          time.Time     `json:"timestamp"`
	OrgID              string        `json:"-"`
	AppID              string        `json:"-"`
	MsgID              string        `json:"msgId"`
	EndpointID         string        `json:"endpointId"`
	URL                string        `json:"url"`
	Status             AttemptStatus `json:"status"`
	ResponseStatusCode int16         `json:"responseStatusCode"`
	ResponseBody       string        `json:"response"`
	ResponseDurationMS int32         `json:"responseDurationMs"`
	AttemptNumber      int16         `json:"attemptNumber"`
	TriggerType        TriggerType   `json:"triggerType"`
	NextAttemptAt      *time.Time    `json:"nextAttempt,omitempty"`
	DeletedAt          *time.Time    `json:"-"`
}
