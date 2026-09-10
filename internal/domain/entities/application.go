package entities

import (
	"encoding/json"
	"time"
)

// Application is one of a tenant's own customers. Endpoints hang off an
// application, so "every destination for this customer" is one lookup.
type Application struct {
	ID        string          `json:"id"`
	OrgID     string          `json:"-"`
	UID       *string         `json:"uid,omitempty"`
	Name      string          `json:"name"`
	RateLimit *int            `json:"rateLimit,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
	DeletedAt *time.Time      `json:"-"`
}
