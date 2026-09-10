package entities

import (
	"encoding/json"
	"time"
)

// SecretType names the signing algorithm an endpoint's secret is used with.
type SecretType string

// The supported signing algorithms. hmac256 is the default because ed25519 is
// roughly 50x more expensive to sign.
const (
	SecretTypeHMAC256 SecretType = "hmac256"
	SecretTypeEd25519 SecretType = "ed25519"
)

// Valid reports whether s is a supported signing algorithm.
func (s SecretType) Valid() bool {
	return s == SecretTypeHMAC256 || s == SecretTypeEd25519
}

// SealedSecret is a signing secret held encrypted at rest, with the expiry of
// its rotation window when it is a superseded secret.
type SealedSecret struct {
	Sealed    []byte     `json:"secret"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
}

// Expired reports whether a rotated-out secret's overlap window has closed.
func (s SealedSecret) Expired(now time.Time) bool {
	return s.ExpiresAt != nil && now.After(*s.ExpiresAt)
}

// Endpoint is a destination URL that receives signed HTTP POSTs. It owns its
// signing secret, its subscription filters, and its health state.
type Endpoint struct {
	ID          string          `json:"id"`
	AppID       string          `json:"-"`
	OrgID       string          `json:"-"`
	UID         *string         `json:"uid,omitempty"`
	URL         string          `json:"url"`
	Description string          `json:"description"`
	Version     int             `json:"version"`
	RateLimit   *int            `json:"rateLimit,omitempty"`
	EventTypes  []string        `json:"filterTypes,omitempty"`
	Channels    []string        `json:"channels,omitempty"`
	Headers     json.RawMessage `json:"-"`

	Secret     SealedSecret   `json:"-"`
	SecretType SecretType     `json:"-"`
	OldSecrets []SealedSecret `json:"-"`

	Pool           string     `json:"-"`
	FirstFailureAt *time.Time `json:"-"`
	DisabledAt     *time.Time `json:"disabledAt,omitempty"`
	CreatedAt      time.Time  `json:"createdAt"`
	UpdatedAt      time.Time  `json:"updatedAt"`
	DeletedAt      *time.Time `json:"-"`
}

// Disabled reports whether the endpoint has been switched off, automatically
// after sustained failure or manually by its owner. Distinct from deleted: a
// disabled endpoint can be re-enabled.
func (e *Endpoint) Disabled() bool { return e.DisabledAt != nil }

// Deleted reports whether the endpoint's owner removed it.
func (e *Endpoint) Deleted() bool { return e.DeletedAt != nil }

// Deliverable reports whether the endpoint may receive deliveries right now.
func (e *Endpoint) Deliverable() bool { return !e.Disabled() && !e.Deleted() }

// WantsEventType reports whether this endpoint subscribes to the named event
// type. A nil or empty filter list means every event type.
func (e *Endpoint) WantsEventType(name string) bool {
	if len(e.EventTypes) == 0 {
		return true
	}
	for _, t := range e.EventTypes {
		if t == name {
			return true
		}
	}
	return false
}

// WantsChannels reports whether a message carrying the supplied channels
// matches this endpoint's channel filter. An endpoint without a filter accepts
// every message; an endpoint with one requires at least one shared channel.
func (e *Endpoint) WantsChannels(channels []string) bool {
	if len(e.Channels) == 0 {
		return true
	}
	if len(channels) == 0 {
		return false
	}
	want := make(map[string]struct{}, len(e.Channels))
	for _, c := range e.Channels {
		want[c] = struct{}{}
	}
	for _, c := range channels {
		if _, ok := want[c]; ok {
			return true
		}
	}
	return false
}

// ActiveSecrets returns every secret that must produce a signature at now:
// the current secret plus every unexpired rotated-out secret, so receivers can
// roll over without downtime.
func (e *Endpoint) ActiveSecrets(now time.Time) []SealedSecret {
	out := make([]SealedSecret, 0, 1+len(e.OldSecrets))
	out = append(out, e.Secret)
	for _, s := range e.OldSecrets {
		if !s.Expired(now) {
			out = append(out, s)
		}
	}
	return out
}
