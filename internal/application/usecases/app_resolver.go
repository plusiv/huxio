package usecases

import (
	"context"

	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/rotisserie/eris"
)

// AppRef is the pair of identifiers every scoped operation needs.
type AppRef struct {
	ID    string
	OrgID string
}

// SnapshotInvalidator asks the local config snapshot to reload.
type SnapshotInvalidator interface {
	Invalidate()
}

// appResolver maps an id-or-uid path segment onto an application inside one
// tenant. It reads the config snapshot first and falls back to the database
// only on a miss.
//
// The fallback matters because a client's first action is usually "create an
// application, then immediately send to it", and the node handling the second
// request may be a few hundred milliseconds behind the change notification.
// Keeping reads off the configuration tables is a rule for the delivery hot
// path; a snapshot miss is by definition not that path, and one query beats
// returning 404 for a resource that demonstrably exists. The miss also
// triggers a reload, so it does not repeat.
type appResolver struct {
	snapshotProvider SnapshotProvider
	appRepo          repositories.ApplicationRepository
	invalidator      SnapshotInvalidator
}

func newAppResolver(
	snapshotProvider SnapshotProvider,
	appRepo repositories.ApplicationRepository,
	invalidator SnapshotInvalidator,
) appResolver {
	return appResolver{snapshotProvider: snapshotProvider, appRepo: appRepo, invalidator: invalidator}
}

// resolve returns the application, or a not-found error.
func (r appResolver) resolve(ctx context.Context, orgID, idOrUID string) (AppRef, error) {
	snapshot := r.snapshotProvider.Current()
	if snapshot == nil && r.appRepo == nil {
		return AppRef{}, apperrors.NewInternalError(eris.New("config snapshot is not loaded"))
	}
	if snapshot != nil {
		if app := snapshot.ResolveApp(orgID, idOrUID); app != nil {
			return AppRef{ID: app.ID, OrgID: app.OrgID}, nil
		}
	}
	if r.appRepo == nil {
		return AppRef{}, apperrors.NewNotFoundError("application")
	}

	app, err := r.appRepo.GetApplication(ctx, repositories.ApplicationFilters{
		OrgID: repositories.Eq(orgID),
		ID:    repositories.Eq(idOrUID),
	})
	if eris.Is(err, repositories.ErrNotFound) {
		app, err = r.appRepo.GetApplication(ctx, repositories.ApplicationFilters{
			OrgID: repositories.Eq(orgID),
			UID:   repositories.Eq(idOrUID),
		})
	}
	switch {
	case eris.Is(err, repositories.ErrNotFound):
		return AppRef{}, apperrors.NewNotFoundError("application")
	case err != nil:
		return AppRef{}, apperrors.NewInternalError(err)
	}

	// The snapshot is behind: ask it to catch up rather than paying for this
	// lookup on every subsequent request.
	if r.invalidator != nil {
		r.invalidator.Invalidate()
	}
	logger.FromContext(ctx).Debug().
		Str("app_id", app.ID).
		Msg("application resolved from the database; config snapshot was stale")

	return AppRef{ID: app.ID, OrgID: app.OrgID}, nil
}
