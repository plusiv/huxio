package usecases

import (
	"context"

	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/utils"
)

// PoolAssignment moves an endpoint between worker pools.
type PoolAssignment struct {
	OrgID      string
	AppIDOrUID string
	EndpointID string
	Pool       string
}

// AdminUseCase serves the operational routes: forcing the stuck-task sweep,
// reading the lease table, and moving endpoints between pools. These are the
// levers an operator reaches for during an incident, so they are deliberately
// boring and observable.
type AdminUseCase struct {
	queueRepo    repositories.QueueRepository
	leaseRepo    repositories.LeaseRepository
	endpointRepo repositories.EndpointRepository
	resolver     appResolver
	invalidator  ConfigInvalidator
}

// NewAdminUseCase builds the use case.
func NewAdminUseCase(
	queueRepo repositories.QueueRepository,
	leaseRepo repositories.LeaseRepository,
	endpointRepo repositories.EndpointRepository,
	snapshotProvider SnapshotProvider,
	appRepo repositories.ApplicationRepository,
	localInvalidator SnapshotInvalidator,
	clusterInvalidator ConfigInvalidator,
) *AdminUseCase {
	return &AdminUseCase{
		queueRepo:    queueRepo,
		leaseRepo:    leaseRepo,
		endpointRepo: endpointRepo,
		resolver:     newAppResolver(snapshotProvider, appRepo, localInvalidator),
		invalidator:  clusterInvalidator,
	}
}

// RescueStuck returns tasks whose holder died back to the queue, on demand.
// The maintenance loop does this on a timer; this is the button for when
// somebody is watching.
func (uc *AdminUseCase) RescueStuck(ctx context.Context) (int64, error) {
	rescued, err := uc.queueRepo.RescueStuck(ctx)
	if err != nil {
		return 0, apperrors.NewInternalError(err)
	}
	return rescued, nil
}

// QueueStats reports depth and lag per pool.
func (uc *AdminUseCase) QueueStats(ctx context.Context, pools []string) ([]repositories.QueueStats, error) {
	stats, err := uc.queueRepo.Stats(ctx, pools)
	if err != nil {
		return nil, apperrors.NewInternalError(err)
	}
	return stats, nil
}

// PartitionLeases returns the current lease table for a pool.
func (uc *AdminUseCase) PartitionLeases(ctx context.Context, pool string) ([]entities.PartitionLease, error) {
	leases, err := uc.leaseRepo.ListPartitionLeases(ctx, pool)
	if err != nil {
		return nil, apperrors.NewInternalError(err)
	}
	return leases, nil
}

// MoveEndpointPool reassigns one endpoint to another worker pool. This is how
// a large tenant gets dedicated capacity, and how a quarantined endpoint is
// released once its owner has fixed it.
func (uc *AdminUseCase) MoveEndpointPool(ctx context.Context, in PoolAssignment) error {
	if utils.IsBlank(in.Pool) {
		return apperrors.NewValidationError("pool is required")
	}

	app, err := uc.resolver.resolve(ctx, in.OrgID, in.AppIDOrUID)
	if err != nil {
		return err
	}

	ep, err := uc.endpointRepo.GetEndpoint(ctx, repositories.EndpointFilters{
		AppID: repositories.Eq(app.ID),
		ID:    repositories.Eq(in.EndpointID),
	})
	if err != nil {
		return apperrors.NewNotFoundError("endpoint")
	}

	if err := uc.endpointRepo.SetPool(ctx, ep.ID, in.Pool); err != nil {
		return apperrors.NewInternalError(err)
	}
	if uc.invalidator != nil {
		// Workers route by the endpoint's pool, so they need the new value before
		// the next fan-out.
		_ = uc.invalidator.NotifyConfigChanged(ctx, app.OrgID)
	}
	return nil
}

// ReEnableEndpoint switches an auto-disabled endpoint back on and clears its
// failure run.
func (uc *AdminUseCase) ReEnableEndpoint(ctx context.Context, in PoolAssignment) error {
	app, err := uc.resolver.resolve(ctx, in.OrgID, in.AppIDOrUID)
	if err != nil {
		return err
	}
	ep, err := uc.endpointRepo.GetEndpoint(ctx, repositories.EndpointFilters{
		AppID: repositories.Eq(app.ID),
		ID:    repositories.Eq(in.EndpointID),
	})
	if err != nil {
		return apperrors.NewNotFoundError("endpoint")
	}

	if err := uc.endpointRepo.SetDisabled(ctx, ep.ID, nil); err != nil {
		return apperrors.NewInternalError(err)
	}
	if err := uc.endpointRepo.SetFirstFailure(ctx, ep.ID, nil); err != nil {
		return apperrors.NewInternalError(err)
	}
	if uc.invalidator != nil {
		_ = uc.invalidator.NotifyConfigChanged(ctx, app.OrgID)
	}
	return nil
}
