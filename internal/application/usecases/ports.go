// Package usecases holds the application layer: orchestration of domain rules
// over repository ports. Nothing here imports a web framework or a database
// driver.
package usecases

import (
	"context"

	"github.com/plusiv/huxio/internal/application/config"
)

// SnapshotProvider hands out the current configuration snapshot. Every read is
// a pointer load, so use cases and the delivery engine never query the
// configuration tables.
type SnapshotProvider interface {
	Current() *config.Snapshot
}

// PayloadCodec compresses payloads for storage and expands them for delivery.
// The bytes are never interpreted.
type PayloadCodec interface {
	Encode(raw []byte) []byte
	Decode(stored []byte) ([]byte, error)
}

// SecretSealer seals and opens signing secrets at rest.
type SecretSealer interface {
	Seal(plaintext []byte) ([]byte, error)
	Open(sealed []byte) ([]byte, error)
}

// ConfigInvalidator publishes a configuration change so every process reloads
// its snapshot.
type ConfigInvalidator interface {
	NotifyConfigChanged(ctx context.Context, orgID string) error
}
