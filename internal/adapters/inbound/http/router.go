// Package http wires the inbound HTTP adapter: middleware order, route
// registration and the composed Echo instance.
package http

import (
	"net/http"

	"github.com/labstack/echo/v5"
	echomiddleware "github.com/labstack/echo/v5/middleware"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/binder"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/handlers"
	appmiddleware "github.com/plusiv/huxio/internal/adapters/inbound/http/middleware"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/openapi"
	"github.com/plusiv/huxio/internal/adapters/inbound/http/portal"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/telemetry"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// bodyLimitHeadroom allows for the JSON envelope around the payload, so the
// payload cap is enforced by the use case with its own error message rather
// than by the transport with a generic one.
const bodyLimitHeadroom = 16 << 10

// RouterConfig carries the transport-level settings.
type RouterConfig struct {
	MaxPayloadBytes  int64
	CORSAllowOrigins []string
	// RateLimitOrgRPS and RateLimitAppRPS are cluster-wide limits, divided
	// across API nodes at request time.
	RateLimitOrgRPS int
	RateLimitAppRPS int
	// Pools are the worker pools reported by the admin routes.
	Pools []string
	// PortalSecureCookie marks the portal session cookie Secure. It must be
	// on wherever the portal is served over HTTPS, which is everywhere except
	// local development.
	PortalSecureCookie bool
}

// RouterDeps is everything the router needs. It is a struct rather than a long
// parameter list because the route table keeps growing and a positional call
// with twenty handlers is a bug waiting to happen.
type RouterDeps struct {
	Config RouterConfig

	Metrics         *telemetry.Metrics
	TokenParser     appmiddleware.TokenParser
	IdempotencyRepo repositories.IdempotencyRepository

	// NodeCounter divides the cluster-wide rate limits across API nodes.
	NodeCounter appmiddleware.NodeCounter
	// PortalUI is optional: when set, the embedded consumer portal is served.
	// It is distinct from PortalTokenHandler, which serves the API route that
	// mints the tokens the portal is entered with.
	PortalUI *portal.Handler

	HealthHandler      *handlers.HealthHandler
	ApplicationHandler *handlers.ApplicationHandler
	EndpointHandler    *handlers.EndpointHandler
	EventTypeHandler   *handlers.EventTypeHandler
	MessageHandler     *handlers.MessageHandler
	AttemptHandler     *handlers.AttemptHandler
	PortalTokenHandler *handlers.PortalHandler
	AdminHandler       *handlers.AdminHandler
	StreamHandler      *handlers.StreamHandler
}

// NewTelemetryRouter composes the minimal surface a worker-only process
// serves: metrics and health. Without it a worker exposes no HTTP at all, so
// the delivery metrics it alone produces cannot be scraped and nothing about
// a split-role deployment is measurable.
func NewTelemetryRouter(metrics *telemetry.Metrics, health *handlers.HealthHandler) *echo.Echo {
	e := echo.New()
	e.HTTPErrorHandler = handlers.CustomHTTPErrorHandler

	e.Pre(echomiddleware.RemoveTrailingSlash())
	e.Use(appmiddleware.Recover())
	e.Use(appmiddleware.RequestID())

	registerTelemetryRoutes(e, e.Group("/api/v1"), metrics, health)

	return e
}

// registerTelemetryRoutes mounts the surface every role serves, whatever else
// it serves: liveness, readiness and the Prometheus scrape. Both routers call
// this so a worker's scrape surface cannot drift from the API's.
func registerTelemetryRoutes(
	e *echo.Echo,
	api *echo.Group,
	metrics *telemetry.Metrics,
	health *handlers.HealthHandler,
) {
	api.GET("/health", health.Health)
	api.GET("/health/ready", health.Ready)
	e.GET("/metrics", echo.WrapHandler(promhttp.HandlerFor(
		metrics.Registry(),
		promhttp.HandlerOpts{Registry: metrics.Registry()},
	)))
}

// NewRouter composes the Echo instance.
func NewRouter(deps RouterDeps) *echo.Echo {
	// Echo's CORS middleware panics on an empty origin list, so a config
	// without one would take the process down at startup rather than serve.
	if len(deps.Config.CORSAllowOrigins) == 0 {
		deps.Config.CORSAllowOrigins = []string{"*"}
	}

	e := echo.New()

	// AppError kinds become status codes here, and internal detail never
	// leaves the process.
	e.HTTPErrorHandler = handlers.CustomHTTPErrorHandler
	// One binder binds and validates, so no handler validates by hand.
	e.Binder = binder.New()

	// Normalise trailing slashes so /app and /app/ hit the same handler.
	// This has to be e.Pre, not e.Use: e.Use runs after routing, by which
	// point /app/ has already 404ed.
	e.Pre(echomiddleware.RemoveTrailingSlash())

	// Recover first: a panic in any later middleware must still become a 500
	// rather than killing the process.
	e.Use(appmiddleware.Recover())
	e.Use(appmiddleware.RequestID())
	e.Use(appmiddleware.RequestLogger())
	e.Use(appmiddleware.Metrics(deps.Metrics))
	e.Use(echomiddleware.CORSWithConfig(echomiddleware.CORSConfig{
		AllowOrigins: deps.Config.CORSAllowOrigins,
		AllowMethods: []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions},
		AllowHeaders: []string{echo.HeaderContentType, echo.HeaderAuthorization, appmiddleware.IdempotencyHeader, echo.HeaderAccept},
	}))

	// The portal is mounted outside /api/v1: it is a browser surface with its
	// own session cookie, not part of the JSON API.
	if deps.PortalUI != nil {
		registerPortal(e, deps)
	}

	api := e.Group("/api/v1")

	// Unauthenticated operational routes.
	registerTelemetryRoutes(e, api, deps.Metrics, deps.HealthHandler)

	// Everything below requires a bearer token. The rate limiter comes after
	// auth because it is keyed by tenant, and the idempotency middleware sits
	// in front of the routes rather than inside handlers because the key is
	// hashed with the tenant too.
	authed := api.Group("",
		appmiddleware.RequireAuth(deps.TokenParser),
		appmiddleware.RateLimitByOrg(appmiddleware.RateLimitOptions{
			RequestsPerSecond: deps.Config.RateLimitOrgRPS,
			Nodes:             deps.NodeCounter,
		}),
		appmiddleware.Idempotency(deps.IdempotencyRepo, appmiddleware.IdempotencyOptions{}),
	)

	// Creating and listing applications is tenant-wide work. There is no
	// middleware here: the handlers call orgToken, which rejects portal
	// tokens. Per-application routes below must stay reachable by portal
	// tokens, so the check cannot sit on this group.
	apps := authed.Group("/app")
	apps.POST("", deps.ApplicationHandler.CreateApplication)
	apps.GET("", deps.ApplicationHandler.ListApplications)

	// Per-application routes. RequireAppScope is what stops a portal token
	// reaching another application, and it is registered on the group so no
	// future route can forget it.
	app := apps.Group("/:app_id", appmiddleware.RequireAppScope("app_id"))
	app.GET("", deps.ApplicationHandler.GetApplication)
	app.PUT("", deps.ApplicationHandler.UpdateApplication)
	app.PATCH("", deps.ApplicationHandler.UpdateApplication)
	app.DELETE("", deps.ApplicationHandler.DeleteApplication)

	// The body limit is a second line of defence in front of the use case's
	// own payload cap, so an oversized body is refused before it is buffered.
	// Ingest carries its own limit as well: a burst from one application must
	// not consume the tenant's whole API budget.
	app.POST("/msg", deps.MessageHandler.CreateMessage,
		appmiddleware.RateLimitByApp("app_id", appmiddleware.RateLimitOptions{
			RequestsPerSecond: deps.Config.RateLimitAppRPS,
			Nodes:             deps.NodeCounter,
		}),
		echomiddleware.BodyLimit(deps.Config.MaxPayloadBytes+bodyLimitHeadroom),
	)
	app.GET("/msg", deps.MessageHandler.ListMessages)
	app.GET("/msg/:msg_id", deps.MessageHandler.GetMessage)

	app.POST("/endpoint", deps.EndpointHandler.CreateEndpoint)
	app.GET("/endpoint", deps.EndpointHandler.ListEndpoints)
	app.GET("/endpoint/:endpoint_id", deps.EndpointHandler.GetEndpoint)
	app.PUT("/endpoint/:endpoint_id", deps.EndpointHandler.UpdateEndpoint)
	app.PATCH("/endpoint/:endpoint_id", deps.EndpointHandler.UpdateEndpoint)
	app.DELETE("/endpoint/:endpoint_id", deps.EndpointHandler.DeleteEndpoint)
	app.GET("/endpoint/:endpoint_id/secret", deps.EndpointHandler.GetSecret)
	app.POST("/endpoint/:endpoint_id/secret/rotate", deps.EndpointHandler.RotateSecret)
	app.GET("/endpoint/:endpoint_id/headers", deps.EndpointHandler.GetHeaders)
	app.PATCH("/endpoint/:endpoint_id/headers", deps.EndpointHandler.PatchHeaders)
	app.GET("/endpoint/:endpoint_id/stats", deps.AttemptHandler.Stats)
	app.POST("/endpoint/:endpoint_id/recover", deps.AttemptHandler.Recover)

	// The delivery log, and the paths that put work back on the queue.
	app.GET("/attempt/msg/:msg_id", deps.AttemptHandler.ListByMessage)
	app.GET("/attempt/endpoint/:endpoint_id", deps.AttemptHandler.ListByEndpoint)
	app.GET("/msg/:msg_id/endpoint/:endpoint_id/attempt", deps.AttemptHandler.ListByMessageAndEndpoint)
	app.POST("/msg/:msg_id/endpoint/:endpoint_id/resend", deps.AttemptHandler.Resend)

	// Features the incumbent's SDKs know nothing about get their own paths
	// rather than extra fields on an existing response. A client parsing a
	// shape it already knows never has to cope with it changing.
	app.POST("/replay", deps.AttemptHandler.Replay)
	app.GET("/attempt/stream", deps.StreamHandler.StreamAttempts)

	// Portal tokens are minted by the sender's backend, never in a browser,
	// so this route is closed to portal tokens themselves.
	app.POST("/portal-access", deps.PortalTokenHandler.PortalAccess)

	// Tenant-wide configuration; the handlers reject portal tokens, as with
	// /app above.
	eventTypes := authed.Group("/event-type")
	eventTypes.GET("", deps.EventTypeHandler.ListEventTypes)
	eventTypes.POST("", deps.EventTypeHandler.CreateEventType)
	eventTypes.GET("/:event_type_name", deps.EventTypeHandler.GetEventType)
	eventTypes.PUT("/:event_type_name", deps.EventTypeHandler.UpdateEventType)
	eventTypes.DELETE("/:event_type_name", deps.EventTypeHandler.ArchiveEventType)

	// The only group that carries the check as middleware, because every
	// route under it is tenant-wide. The handlers assert it again: this is
	// the group where forgetting it would be most expensive.
	admin := authed.Group("/admin", appmiddleware.RequireOrgToken())
	admin.POST("/rescue-stuck", deps.AdminHandler.RescueStuck)
	admin.GET("/queue", deps.AdminHandler.QueueStats)
	admin.GET("/partitions", deps.AdminHandler.Partitions)
	admin.POST("/pool", deps.AdminHandler.MovePool)
	admin.POST("/endpoint/enable", deps.AdminHandler.ReEnableEndpoint)

	return e
}

// registerPortal mounts the embedded portal behind the session middleware.
// No portal URL contains an application id: the session decides which
// application you are looking at. That is stronger than checking a path
// against the token, because there is no new route on which someone could
// forget the check.
func registerPortal(e *echo.Echo, deps RouterDeps) {
	group := e.Group("/portal", portal.RequireSession(deps.TokenParser, deps.Config.PortalSecureCookie))

	group.GET("", deps.PortalUI.Endpoints)
	group.GET("/log", deps.PortalUI.Log)
	group.GET("/tail", deps.PortalUI.Tail)
	group.GET("/stream", deps.PortalUI.Stream)
	group.POST("/sign-out", deps.PortalUI.SignOut)

	group.POST("/endpoints", deps.PortalUI.CreateEndpoint)
	group.GET("/endpoints/:endpoint_id", deps.PortalUI.Endpoint)
	group.POST("/endpoints/:endpoint_id", deps.PortalUI.UpdateEndpoint)
	group.POST("/endpoints/:endpoint_id/delete", deps.PortalUI.DeleteEndpoint)
	group.POST("/endpoints/:endpoint_id/secret", deps.PortalUI.Secret)
	group.POST("/endpoints/:endpoint_id/test", deps.PortalUI.TestEvent)
	group.POST("/endpoints/:endpoint_id/resend", deps.PortalUI.Resend)
	group.POST("/endpoints/:endpoint_id/recover", deps.PortalUI.Recover)
}

// Spec generates the OpenAPI document from the routes actually registered on
// e. Deriving it means a route added without documentation still shows up,
// and a route removed disappears.
func Spec(e *echo.Echo, info openapi.Info, servers []openapi.Server) *openapi.Document {
	registered := e.Router().Routes()
	routes := make([]openapi.Route, 0, len(registered))
	for _, route := range registered {
		routes = append(routes, openapi.Route{Method: route.Method, Path: route.Path})
	}
	return openapi.Build(info, servers, routes, handlers.RouteDocs())
}
