// Package portal serves the consumer-facing portal: the endpoint owner's view
// of their own deliveries. It ships in the binary, server-rendered, with no
// build step and no third-party frontend assets.
//
// The portal derives the application it is showing from the token, never from
// the URL. There is therefore no request a portal session can make that names
// another application, which is a stronger guarantee than checking that a
// path segment matches: a leaked token cannot be pointed at anything.
package portal

import (
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/infrastructure/auth"
	"github.com/plusiv/huxio/internal/infrastructure/authctx"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
)

// SessionCookie holds the portal token for the browser session.
const SessionCookie = "huxio_portal"

// TokenParser validates a portal token.
type TokenParser interface {
	Parse(token string) (*auth.Claims, error)
}

// RequireSession authenticates a portal request from the session cookie, or
// from a token in the query string on the first hop (which is how the
// sender's backend hands a customer over). A query token is immediately moved
// into an HttpOnly cookie and removed from the URL, so it stops appearing in
// browser history, referrers and logs.
func RequireSession(parser TokenParser, secure bool) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			token, fromQuery := sessionToken(c)
			if token == "" {
				return apperrors.NewUnauthorizedError("this portal link is missing its token")
			}

			claims, err := parser.Parse(token)
			if err != nil {
				return err
			}
			// An organization token must not open the portal: the portal is the
			// endpoint owner's view, and an org token would carry the whole tenant's
			// data into it.
			if claims.Scope != auth.ScopePortal {
				return apperrors.NewForbiddenError("this token cannot open the portal")
			}
			if claims.OrgID == "" {
				return apperrors.NewUnauthorizedError("portal token is missing its organization")
			}

			if fromQuery {
				setSessionCookie(c, token, claims.ExpiresAt.Time, secure)
				// Redirect to the same path without the token, so the URL the customer
				// sees and shares carries no credential.
				return c.Redirect(http.StatusSeeOther, c.Request().URL.Path)
			}

			principal := authctx.Principal{OrgID: claims.OrgID, AppID: claims.Subject, Portal: true}
			req := c.Request()
			ctx := authctx.Context(req.Context(), principal)
			enriched := logger.FromContext(ctx).With().
				Str("org_id", principal.OrgID).
				Str("portal_app_id", principal.AppID).
				Logger()
			c.SetRequest(req.WithContext(logger.Context(ctx, enriched)))

			return next(c)
		}
	}
}

// sessionToken reads the token from the cookie, falling back to the query
// string on the entry hop.
func sessionToken(c *echo.Context) (token string, fromQuery bool) {
	if cookie, err := c.Request().Cookie(SessionCookie); err == nil && cookie.Value != "" {
		return cookie.Value, false
	}
	if query := c.QueryParam("token"); query != "" {
		return query, true
	}
	return "", false
}

func setSessionCookie(c *echo.Context, token string, expiry time.Time, secure bool) {
	if expiry.IsZero() {
		expiry = time.Now().Add(8 * time.Hour)
	}
	http.SetCookie(c.Response(), &http.Cookie{
		Name:  SessionCookie,
		Value: token,
		Path:  "/portal",
		// The cookie is not readable from JavaScript, is not sent cross-site, and
		// expires with the token itself.
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteStrictMode,
		Expires:  expiry,
	})
}

// clearSessionCookie ends a portal session.
func clearSessionCookie(c *echo.Context, secure bool) {
	http.SetCookie(c.Response(), &http.Cookie{
		Name:     SessionCookie,
		Value:    "",
		Path:     "/portal",
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
	})
}

// session is the authenticated portal viewer.
type session struct {
	OrgID string
	AppID string
}

// sessionFrom reads the portal session off the request context.
func sessionFrom(c *echo.Context) (session, error) {
	principal, ok := authctx.FromContext(c.Request().Context())
	if !ok || !principal.Portal || principal.AppID == "" {
		return session{}, apperrors.NewUnauthorizedError("no portal session")
	}
	return session{OrgID: principal.OrgID, AppID: principal.AppID}, nil
}
