// Package entities holds the pure domain structs. Nothing here imports a web
// framework, a database driver, or any other infrastructure package.
package entities

import "time"

// Organization is a tenant of the service. Every other row belongs to exactly
// one organization; it is the boundary for authentication, quotas and metrics.
type Organization struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	CreatedAt time.Time  `json:"createdAt"`
	UpdatedAt time.Time  `json:"updatedAt"`
	DeletedAt *time.Time `json:"-"`
}
