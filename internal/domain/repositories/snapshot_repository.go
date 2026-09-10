package repositories

import (
	"context"

	"github.com/plusiv/huxio/internal/domain/entities"
)

// SnapshotData is the whole configuration set, loaded in full into process
// memory. Apps, endpoints and event types are small and rarely written, so
// every delivery reads them with a pointer load rather than a query.
type SnapshotData struct {
	Organizations []*entities.Organization
	Applications  []*entities.Application
	Endpoints     []*entities.Endpoint
	EventTypes    []*entities.EventType
}

// SnapshotRepository is the port used to build a config snapshot and to listen
// for invalidations.
type SnapshotRepository interface {
	// LoadSnapshot reads every live configuration row in as few round trips as
	// possible.
	LoadSnapshot(ctx context.Context) (*SnapshotData, error)
	// NotifyConfigChanged publishes an invalidation to every process.
	NotifyConfigChanged(ctx context.Context, orgID string) error
}
