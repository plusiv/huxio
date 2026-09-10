package postgres

import (
	"context"

	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/rotisserie/eris"
)

// ConfigNotifyChannel is the LISTEN/NOTIFY channel used to invalidate config
// snapshots. It drops silently on connection loss, which is why every process
// also refreshes unconditionally on a timer.
const ConfigNotifyChannel = "huxio_config"

// SnapshotRepo implements repositories.SnapshotRepository. The whole config
// set is small, so it is loaded in full with one batch of four queries.
type SnapshotRepo struct {
	store *Store
}

// NewSnapshotRepo builds the repository.
func NewSnapshotRepo(store *Store) *SnapshotRepo { return &SnapshotRepo{store: store} }

// LoadSnapshot reads every live configuration row.
func (r *SnapshotRepo) LoadSnapshot(ctx context.Context) (*repositories.SnapshotData, error) {
	data := &repositories.SnapshotData{}

	orgRows, err := r.store.Querier(ctx).Query(ctx,
		`SELECT `+organizationColumns+` FROM organization WHERE deleted_at IS NULL`)
	if err != nil {
		return nil, eris.Wrap(err, "load organizations")
	}
	data.Organizations, err = collectAndClose(orgRows, scanOrganization)
	if err != nil {
		return nil, eris.Wrap(err, "scan organizations")
	}

	appRows, err := r.store.Querier(ctx).Query(ctx,
		`SELECT `+applicationColumns+` FROM application WHERE deleted_at IS NULL`)
	if err != nil {
		return nil, eris.Wrap(err, "load applications")
	}
	data.Applications, err = collectAndClose(appRows, scanApplication)
	if err != nil {
		return nil, eris.Wrap(err, "scan applications")
	}

	epRows, err := r.store.Querier(ctx).Query(ctx,
		`SELECT `+endpointColumns+` FROM endpoint WHERE deleted_at IS NULL`)
	if err != nil {
		return nil, eris.Wrap(err, "load endpoints")
	}
	data.Endpoints, err = collectAndClose(epRows, scanEndpoint)
	if err != nil {
		return nil, eris.Wrap(err, "scan endpoints")
	}

	etRows, err := r.store.Querier(ctx).Query(ctx,
		`SELECT `+eventTypeColumns+` FROM event_type WHERE deleted_at IS NULL`)
	if err != nil {
		return nil, eris.Wrap(err, "load event types")
	}
	data.EventTypes, err = collectAndClose(etRows, scanEventType)
	if err != nil {
		return nil, eris.Wrap(err, "scan event types")
	}

	return data, nil
}

// NotifyConfigChanged publishes an invalidation to every process. An empty
// orgID means "reload everything".
func (r *SnapshotRepo) NotifyConfigChanged(ctx context.Context, orgID string) error {
	if _, err := r.store.Querier(ctx).Exec(ctx, `SELECT pg_notify($1, $2)`, ConfigNotifyChannel, orgID); err != nil {
		return eris.Wrap(err, "notify config change")
	}
	return nil
}

// Interface assertions: every repository satisfies the port it implements.
var (
	_ repositories.OrganizationRepository = (*OrganizationRepo)(nil)
	_ repositories.ApplicationRepository  = (*ApplicationRepo)(nil)
	_ repositories.EventTypeRepository    = (*EventTypeRepo)(nil)
	_ repositories.EndpointRepository     = (*EndpointRepo)(nil)
	_ repositories.MessageRepository      = (*MessageRepo)(nil)
	_ repositories.AttemptRepository      = (*AttemptRepo)(nil)
	_ repositories.QueueRepository        = (*QueueRepo)(nil)
	_ repositories.LeaseRepository        = (*LeaseRepo)(nil)
	_ repositories.IdempotencyRepository  = (*IdempotencyRepo)(nil)
	_ repositories.MaintenanceRepository  = (*MaintenanceRepo)(nil)
	_ repositories.SnapshotRepository     = (*SnapshotRepo)(nil)
	_ repositories.TxManager              = (*Store)(nil)
)
