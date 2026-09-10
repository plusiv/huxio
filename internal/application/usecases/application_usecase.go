package usecases

import (
	"context"
	"encoding/json"
	"time"

	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/ids"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

// CreateApplicationInput is a create request.
type CreateApplicationInput struct {
	OrgID     string
	Name      string
	UID       *string
	RateLimit *int
	Metadata  json.RawMessage
}

// UpdateApplicationInput is a full or partial update. Nil fields are left
// unchanged, which is what makes one input serve both PUT and PATCH.
type UpdateApplicationInput struct {
	Name      *string
	UID       *string
	RateLimit *int
	Metadata  json.RawMessage
}

// ListApplicationsInput is a list request.
type ListApplicationsInput struct {
	OrgID  string
	Search string
	Cursor string
	Limit  int
}

// ApplicationUseCase owns application lifecycle. Every mutation publishes a
// config invalidation so all processes reload their snapshot.
type ApplicationUseCase struct {
	appRepo     repositories.ApplicationRepository
	invalidator ConfigInvalidator
}

// NewApplicationUseCase builds the use case.
func NewApplicationUseCase(appRepo repositories.ApplicationRepository, invalidator ConfigInvalidator) *ApplicationUseCase {
	return &ApplicationUseCase{appRepo: appRepo, invalidator: invalidator}
}

// CreateApplication inserts an application.
func (uc *ApplicationUseCase) CreateApplication(ctx context.Context, in CreateApplicationInput) (*entities.Application, error) {
	if utils.IsBlank(in.Name) {
		return nil, apperrors.NewValidationError("name is required")
	}
	if in.RateLimit != nil && *in.RateLimit <= 0 {
		return nil, apperrors.NewValidationError("rateLimit must be positive")
	}
	if len(in.Metadata) > 0 && !json.Valid(in.Metadata) {
		return nil, apperrors.NewValidationError("metadata must be well-formed JSON")
	}

	now := time.Now().UTC()
	app := &entities.Application{
		ID:        ids.New(ids.PrefixApplication),
		OrgID:     in.OrgID,
		UID:       utils.NilIfBlank(utils.Deref(in.UID, "")),
		Name:      in.Name,
		RateLimit: in.RateLimit,
		Metadata:  in.Metadata,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := uc.appRepo.CreateApplication(ctx, app); err != nil {
		if eris.Is(err, repositories.ErrConflict) {
			return nil, apperrors.NewConflictError("an application with this uid already exists")
		}
		return nil, apperrors.NewInternalError(err)
	}
	uc.invalidate(ctx, in.OrgID)
	return app, nil
}

// GetApplication resolves an application by id or tenant-assigned uid.
func (uc *ApplicationUseCase) GetApplication(ctx context.Context, orgID, idOrUID string) (*entities.Application, error) {
	app, err := uc.appRepo.GetApplication(ctx, repositories.ApplicationFilters{
		OrgID: repositories.Eq(orgID),
		ID:    repositories.Eq(idOrUID),
	})
	if err == nil {
		return app, nil
	}
	if !eris.Is(err, repositories.ErrNotFound) {
		return nil, apperrors.NewInternalError(err)
	}

	app, err = uc.appRepo.GetApplication(ctx, repositories.ApplicationFilters{
		OrgID: repositories.Eq(orgID),
		UID:   repositories.Eq(idOrUID),
	})
	if err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return nil, apperrors.NewNotFoundError("application")
		}
		return nil, apperrors.NewInternalError(err)
	}
	return app, nil
}

// UpdateApplication applies the supplied fields and returns the updated row.
func (uc *ApplicationUseCase) UpdateApplication(
	ctx context.Context,
	orgID, idOrUID string,
	in UpdateApplicationInput,
) (*entities.Application, error) {
	app, err := uc.GetApplication(ctx, orgID, idOrUID)
	if err != nil {
		return nil, err
	}

	if in.Name != nil {
		if utils.IsBlank(*in.Name) {
			return nil, apperrors.NewValidationError("name cannot be blank")
		}
		app.Name = *in.Name
	}
	if in.UID != nil {
		app.UID = utils.NilIfBlank(*in.UID)
	}
	if in.RateLimit != nil {
		if *in.RateLimit <= 0 {
			return nil, apperrors.NewValidationError("rateLimit must be positive")
		}
		app.RateLimit = in.RateLimit
	}
	if len(in.Metadata) > 0 {
		if !json.Valid(in.Metadata) {
			return nil, apperrors.NewValidationError("metadata must be well-formed JSON")
		}
		app.Metadata = in.Metadata
	}
	app.UpdatedAt = time.Now().UTC()

	if err := uc.appRepo.UpdateApplication(ctx, app); err != nil {
		if eris.Is(err, repositories.ErrConflict) {
			return nil, apperrors.NewConflictError("an application with this uid already exists")
		}
		if eris.Is(err, repositories.ErrNotFound) {
			return nil, apperrors.NewNotFoundError("application")
		}
		return nil, apperrors.NewInternalError(err)
	}
	uc.invalidate(ctx, orgID)
	return app, nil
}

// DeleteApplication soft-deletes an application.
func (uc *ApplicationUseCase) DeleteApplication(ctx context.Context, orgID, idOrUID string) error {
	app, err := uc.GetApplication(ctx, orgID, idOrUID)
	if err != nil {
		return err
	}
	if err := uc.appRepo.DeleteApplication(ctx, app.ID); err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return apperrors.NewNotFoundError("application")
		}
		return apperrors.NewInternalError(err)
	}
	uc.invalidate(ctx, orgID)
	return nil
}

// ListApplications returns a cursor-paginated page.
func (uc *ApplicationUseCase) ListApplications(
	ctx context.Context,
	in ListApplicationsInput,
) (repositories.CursorResult[*entities.Application], error) {
	filters := repositories.ApplicationFilters{OrgID: repositories.Eq(in.OrgID)}
	if search := utils.NilIfBlank(in.Search); search != nil {
		filters.Name = &repositories.Filter[string]{IContains: search}
	}

	result, err := uc.appRepo.GetApplications(ctx, filters, repositories.CursorPagination{
		Cursor: in.Cursor, Limit: in.Limit,
	})
	if err != nil {
		return result, apperrors.NewInternalError(err)
	}
	return result, nil
}

// invalidate publishes a config change. A failure is logged and swallowed: the
// write already succeeded, and the 60s backstop refresh will pick it up.
func (uc *ApplicationUseCase) invalidate(ctx context.Context, orgID string) {
	if uc.invalidator == nil {
		return
	}
	if err := uc.invalidator.NotifyConfigChanged(ctx, orgID); err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("config invalidation failed")
	}
}
