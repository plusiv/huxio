package entities

import "time"

// Message is one event a tenant asked us to deliver, with its raw payload.
// Written once, read a handful of times, then aged out by dropping the whole
// day partition.
//
// Payload holds the tenant's JSON exactly as stored: possibly compressed,
// always opaque. It is validated once at ingest and never unmarshalled.
type Message struct {
	ID          string     `json:"id"`
	CreatedAt   time.Time  `json:"timestamp"`
	OrgID       string     `json:"-"`
	AppID       string     `json:"-"`
	EventType   string     `json:"eventType"`
	UID         *string    `json:"eventId,omitempty"`
	Channels    []string   `json:"channels,omitempty"`
	Payload     []byte     `json:"-"`
	PayloadSize int        `json:"-"`
	ExpiresAt   time.Time  `json:"-"`
	DeletedAt   *time.Time `json:"-"`
}
