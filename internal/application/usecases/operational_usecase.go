package usecases

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/plusiv/huxio/internal/application/dispatch"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

// OperationalAppUID is the reserved application every tenant's operational
// webhooks are delivered through. A tenant subscribes by creating endpoints on
// it, exactly as it would for its own events.
const OperationalAppUID = "huxio.operational"

// OperationalUseCase emits notifications about the delivery system as
// webhooks, through the normal ingest path. There is deliberately no second
// delivery mechanism: these retry, sign and appear in the log like anything
// else.
type OperationalUseCase struct {
	ingestUseCase *IngestUseCase
	appUseCase    *ApplicationUseCase

	// appIDs caches the reserved application per tenant, so a routine event
	// does not cost a lookup.
	mu     sync.RWMutex
	appIDs map[string]string
}

// NewOperationalUseCase builds the use case.
func NewOperationalUseCase(ingestUseCase *IngestUseCase, appUseCase *ApplicationUseCase) *OperationalUseCase {
	return &OperationalUseCase{
		ingestUseCase: ingestUseCase,
		appUseCase:    appUseCase,
		appIDs:        map[string]string{},
	}
}

// Emit publishes one operational event. Failures are returned but callers on
// the delivery path log and continue: an operational webhook is never allowed
// to fail a real delivery.
func (uc *OperationalUseCase) Emit(ctx context.Context, event dispatch.OperationalEvent) error {
	if utils.IsBlank(event.OrgID) || utils.IsBlank(event.Type) {
		return apperrors.NewValidationError("operational events need a tenant and a type")
	}

	appID, err := uc.reservedApp(ctx, event.OrgID)
	if err != nil {
		return err
	}

	payload := map[string]any{
		"type":      event.Type,
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
	}
	if event.AppID != "" {
		payload["appId"] = event.AppID
	}
	if event.EndpointID != "" {
		payload["endpointId"] = event.EndpointID
	}
	if event.MsgID != "" {
		payload["msgId"] = event.MsgID
	}
	for key, value := range event.Detail {
		payload[key] = value
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		return apperrors.NewInternalError(err)
	}

	_, err = uc.ingestUseCase.Ingest(ctx, IngestInput{
		OrgID:      event.OrgID,
		AppIDOrUID: appID,
		EventType:  event.Type,
		Payload:    encoded,
	})
	return err
}

// reservedApp returns the tenant's operational application, creating it on
// first use so a tenant can subscribe before anything has gone wrong.
func (uc *OperationalUseCase) reservedApp(ctx context.Context, orgID string) (string, error) {
	uc.mu.RLock()
	appID, ok := uc.appIDs[orgID]
	uc.mu.RUnlock()
	if ok {
		return appID, nil
	}

	app, err := uc.appUseCase.GetApplication(ctx, orgID, OperationalAppUID)
	switch {
	case err == nil:
		appID = app.ID
	case isNotFound(err):
		created, createErr := uc.appUseCase.CreateApplication(ctx, CreateApplicationInput{
			OrgID: orgID,
			Name:  "Operational webhooks",
			UID:   utils.Ptr(OperationalAppUID),
		})
		if createErr != nil {
			// Another node may have created it first.
			if eris.Is(createErr, repositories.ErrConflict) || isConflict(createErr) {
				existing, getErr := uc.appUseCase.GetApplication(ctx, orgID, OperationalAppUID)
				if getErr != nil {
					return "", getErr
				}
				appID = existing.ID
				break
			}
			return "", createErr
		}
		appID = created.ID
		logger.FromContext(ctx).Info().
			Str("org_id", orgID).
			Str("app_id", appID).
			Msg("created the reserved operational application")
	default:
		return "", err
	}

	uc.mu.Lock()
	uc.appIDs[orgID] = appID
	uc.mu.Unlock()
	return appID, nil
}

var _ dispatch.OperationalEmitter = (*OperationalUseCase)(nil)

func isNotFound(err error) bool {
	var appErr *apperrors.AppError
	return eris.As(err, &appErr) && appErr.Kind == apperrors.KindNotFound
}

func isConflict(err error) bool {
	var appErr *apperrors.AppError
	return eris.As(err, &appErr) && appErr.Kind == apperrors.KindConflict
}
