package middleware

import (
	"strings"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/infrastructure/auth"
	"github.com/plusiv/huxio/internal/infrastructure/authctx"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
)

// TokenParser validates a bearer token and returns its claims.
type TokenParser interface {
	Parse(token string) (*auth.Claims, error)
}

// RequireAuth validates the bearer token and stores the principal on the
// request context. Every route below it can assume a principal exists.
func RequireAuth(parser TokenParser) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			token, err := bearerToken(c)
			if err != nil {
				return err
			}
			claims, err := parser.Parse(token)
			if err != nil {
				return err
			}

			principal := authctx.Principal{OrgID: claims.Subject}
			if claims.Scope == auth.ScopePortal {
				// A portal token's subject is the application; its tenant is carried
				// separately.
				principal = authctx.Principal{OrgID: claims.OrgID, AppID: claims.Subject, Portal: true}
				if principal.OrgID == "" {
					return apperrors.NewUnauthorizedError("portal token is missing its organization")
				}
			}

			req := c.Request()
			ctx := authctx.Context(req.Context(), principal)
			enriched := logger.FromContext(ctx).With().Str("org_id", principal.OrgID)
			if principal.Portal {
				enriched = enriched.Str("portal_app_id", principal.AppID)
			}
			c.SetRequest(req.WithContext(logger.Context(ctx, enriched.Logger())))
			return next(c)
		}
	}
}

// RequireAppScope asserts that a portal token may only act on its own
// application. It reads the application from the path parameter, so it must be
// registered on every route that carries one.
//
// This is the single most important authorization check in the system: a
// portal token that reaches another tenant's delivery log is the worst bug this
// project can ship.
func RequireAppScope(param string) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			principal, ok := authctx.FromContext(c.Request().Context())
			if !ok {
				return apperrors.NewUnauthorizedError("authentication required")
			}
			if !principal.Portal {
				return next(c)
			}
			if !principal.ScopedToApp(c.Param(param)) {
				return apperrors.NewForbiddenError("this token cannot access that application")
			}
			return next(c)
		}
	}
}

// RequireOrgToken rejects portal tokens on tenant-wide routes.
func RequireOrgToken() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			principal, ok := authctx.FromContext(c.Request().Context())
			if !ok {
				return apperrors.NewUnauthorizedError("authentication required")
			}
			if principal.Portal {
				return apperrors.NewForbiddenError("a portal token cannot access this resource")
			}
			return next(c)
		}
	}
}

func bearerToken(c *echo.Context) (string, error) {
	header := c.Request().Header.Get("Authorization")
	if header == "" {
		return "", apperrors.NewUnauthorizedError("missing Authorization header")
	}
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "bearer") || strings.TrimSpace(token) == "" {
		return "", apperrors.NewUnauthorizedError("expected an Authorization: Bearer <token> header")
	}
	return strings.TrimSpace(token), nil
}
