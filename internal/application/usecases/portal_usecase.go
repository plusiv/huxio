package usecases

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/utils"
)

// TokenIssuer mints portal tokens.
type TokenIssuer interface {
	IssuePortalToken(appID, orgID string, ttl time.Duration) (string, time.Time, error)
}

// PortalAccess is the response of the portal-access route: everything the
// sender's backend needs to redirect or iframe its own customer into the
// portal.
type PortalAccess struct {
	URL    string    `json:"url"`
	Token  string    `json:"token"`
	Expiry time.Time `json:"expiry"`
}

// PortalUseCase mints the short-lived, application-scoped tokens the consumer
// portal is entered with. The token is minted by the sender's backend and
// never in a browser.
type PortalUseCase struct {
	tokenIssuer TokenIssuer
	resolver    appResolver
	baseURL     string
	ttl         time.Duration
}

// NewPortalUseCase builds the use case.
func NewPortalUseCase(
	tokenIssuer TokenIssuer,
	snapshotProvider SnapshotProvider,
	appRepo repositories.ApplicationRepository,
	invalidator SnapshotInvalidator,
	baseURL string,
	ttl time.Duration,
) *PortalUseCase {
	if ttl <= 0 {
		ttl = 8 * time.Hour
	}
	return &PortalUseCase{
		tokenIssuer: tokenIssuer,
		resolver:    newAppResolver(snapshotProvider, appRepo, invalidator),
		baseURL:     strings.TrimRight(baseURL, "/"),
		ttl:         ttl,
	}
}

// Grant mints a portal token for one application.
func (uc *PortalUseCase) Grant(ctx context.Context, orgID, appIDOrUID string, ttl *time.Duration) (PortalAccess, error) {
	app, err := uc.resolver.resolve(ctx, orgID, appIDOrUID)
	if err != nil {
		return PortalAccess{}, err
	}

	lifetime := utils.Deref(ttl, uc.ttl)
	if lifetime <= 0 || lifetime > 24*time.Hour {
		// Portal tokens are short-lived by design: one leaking is the worst
		// bug this project can ship, so the blast radius is capped.
		return PortalAccess{}, apperrors.NewValidationError("token lifetime must be between 1 second and 24 hours")
	}

	token, expiry, err := uc.tokenIssuer.IssuePortalToken(app.ID, app.OrgID, lifetime)
	if err != nil {
		return PortalAccess{}, err
	}

	// The URL points at our own embedded portal, not a hosted service. The
	// token travels in the query once; the portal moves it into an HttpOnly
	// cookie and redirects, so it stops appearing in history or referrers.
	return PortalAccess{
		URL:    uc.baseURL + "/portal?token=" + url.QueryEscape(token),
		Token:  token,
		Expiry: expiry,
	}, nil
}
