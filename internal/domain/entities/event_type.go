package entities

import (
	"encoding/json"
	"time"
)

// EventType is a named kind of event a tenant can send, such as invoice.paid.
// Endpoints subscribe by name; the optional JSON schema drives sample payloads
// and generated receiver docs.
type EventType struct {
	OrgID       string          `json:"-"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Schemas     json.RawMessage `json:"schemas,omitempty"`
	CreatedAt   time.Time       `json:"createdAt"`
	UpdatedAt   time.Time       `json:"updatedAt"`
	DeletedAt   *time.Time      `json:"-"`
}

// Archived reports whether the tenant has retired this event type. The row
// stays so historical messages referencing it still render.
func (e *EventType) Archived() bool { return e.DeletedAt != nil }
