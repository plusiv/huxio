package usecases

import (
	"context"
	"encoding/json"
	"regexp"
	"time"

	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

// eventTypeNamePattern accepts the dotted, lowercase names receivers expect,
// such as invoice.paid.
var eventTypeNamePattern = regexp.MustCompile(`^[a-zA-Z0-9\-_.]+$`)

// CreateEventTypeInput is a create request.
type CreateEventTypeInput struct {
	OrgID       string
	Name        string
	Description string
	Schemas     json.RawMessage
}

// UpdateEventTypeInput is an update request.
type UpdateEventTypeInput struct {
	Description *string
	Schemas     json.RawMessage
}

// ListEventTypesInput is a list request.
type ListEventTypesInput struct {
	OrgID           string
	IncludeArchived bool
	Cursor          string
	Limit           int
}

// EventTypeUseCase owns the tenant's event catalogue.
type EventTypeUseCase struct {
	eventTypeRepo repositories.EventTypeRepository
	invalidator   ConfigInvalidator
}

// NewEventTypeUseCase builds the use case.
func NewEventTypeUseCase(eventTypeRepo repositories.EventTypeRepository, invalidator ConfigInvalidator) *EventTypeUseCase {
	return &EventTypeUseCase{eventTypeRepo: eventTypeRepo, invalidator: invalidator}
}

// CreateEventType registers an event type. Re-creating an archived name
// revives it, because historical messages still reference it.
func (uc *EventTypeUseCase) CreateEventType(ctx context.Context, in CreateEventTypeInput) (*entities.EventType, error) {
	if err := validateEventTypeName(in.Name); err != nil {
		return nil, err
	}
	if len(in.Schemas) > 0 && !json.Valid(in.Schemas) {
		return nil, apperrors.NewValidationError("schemas must be well-formed JSON")
	}

	now := time.Now().UTC()
	et := &entities.EventType{
		OrgID:       in.OrgID,
		Name:        in.Name,
		Description: in.Description,
		Schemas:     in.Schemas,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := uc.eventTypeRepo.CreateEventType(ctx, et); err != nil {
		if eris.Is(err, repositories.ErrConflict) {
			return nil, apperrors.NewConflictError("an event type with this name already exists")
		}
		return nil, apperrors.NewInternalError(err)
	}
	uc.invalidate(ctx, in.OrgID)
	return et, nil
}

// GetEventType returns one event type, including archived ones so the portal
// can render historical messages.
func (uc *EventTypeUseCase) GetEventType(ctx context.Context, orgID, name string) (*entities.EventType, error) {
	et, err := uc.eventTypeRepo.GetEventType(ctx, repositories.EventTypeFilters{
		OrgID:           repositories.Eq(orgID),
		Name:            repositories.Eq(name),
		IncludeArchived: true,
	})
	if err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return nil, apperrors.NewNotFoundError("event type")
		}
		return nil, apperrors.NewInternalError(err)
	}
	return et, nil
}

// UpdateEventType writes the mutable fields.
func (uc *EventTypeUseCase) UpdateEventType(ctx context.Context, orgID, name string, in UpdateEventTypeInput) (*entities.EventType, error) {
	et, err := uc.GetEventType(ctx, orgID, name)
	if err != nil {
		return nil, err
	}
	if et.Archived() {
		return nil, apperrors.NewBusinessRuleError("cannot update an archived event type")
	}

	if in.Description != nil {
		et.Description = *in.Description
	}
	if len(in.Schemas) > 0 {
		if !json.Valid(in.Schemas) {
			return nil, apperrors.NewValidationError("schemas must be well-formed JSON")
		}
		et.Schemas = in.Schemas
	}
	et.UpdatedAt = time.Now().UTC()

	if err := uc.eventTypeRepo.UpdateEventType(ctx, et); err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return nil, apperrors.NewNotFoundError("event type")
		}
		return nil, apperrors.NewInternalError(err)
	}
	uc.invalidate(ctx, orgID)
	return et, nil
}

// ArchiveEventType retires an event type. Deletion is always soft here.
func (uc *EventTypeUseCase) ArchiveEventType(ctx context.Context, orgID, name string) error {
	if err := uc.eventTypeRepo.ArchiveEventType(ctx, orgID, name); err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return apperrors.NewNotFoundError("event type")
		}
		return apperrors.NewInternalError(err)
	}
	uc.invalidate(ctx, orgID)
	return nil
}

// UnarchiveEventType restores a retired event type.
func (uc *EventTypeUseCase) UnarchiveEventType(ctx context.Context, orgID, name string) error {
	if err := uc.eventTypeRepo.UnarchiveEventType(ctx, orgID, name); err != nil {
		if eris.Is(err, repositories.ErrNotFound) {
			return apperrors.NewNotFoundError("event type")
		}
		return apperrors.NewInternalError(err)
	}
	uc.invalidate(ctx, orgID)
	return nil
}

// ListEventTypes returns a cursor-paginated page.
func (uc *EventTypeUseCase) ListEventTypes(
	ctx context.Context,
	in ListEventTypesInput,
) (repositories.CursorResult[*entities.EventType], error) {
	result, err := uc.eventTypeRepo.GetEventTypes(ctx,
		repositories.EventTypeFilters{
			OrgID:           repositories.Eq(in.OrgID),
			IncludeArchived: in.IncludeArchived,
		},
		repositories.CursorPagination{Cursor: in.Cursor, Limit: in.Limit},
	)
	if err != nil {
		return result, apperrors.NewInternalError(err)
	}
	return result, nil
}

func validateEventTypeName(name string) error {
	if utils.IsBlank(name) {
		return apperrors.NewValidationError("name is required")
	}
	if len(name) > 256 {
		return apperrors.NewValidationError("name must be at most 256 characters")
	}
	if !eventTypeNamePattern.MatchString(name) {
		return apperrors.NewValidationError("name may contain only letters, digits, dots, dashes and underscores")
	}
	return nil
}

func (uc *EventTypeUseCase) invalidate(ctx context.Context, orgID string) {
	if uc.invalidator == nil {
		return
	}
	if err := uc.invalidator.NotifyConfigChanged(ctx, orgID); err != nil {
		logger.FromContext(ctx).Warn().Err(err).Msg("config invalidation failed")
	}
}
