package handlers

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/application/config"
)

// MaxSnapshotAge is how stale the config snapshot may be before readiness
// fails. Beyond it, LISTEN has died and this process is delivering with config
// nobody can see.
const MaxSnapshotAge = 5 * time.Minute

// HealthChecker reports whether the database can serve queries.
type HealthChecker interface {
	Healthy(ctx context.Context) error
}

// HealthHandler serves liveness and readiness. Getting this split wrong makes
// rolling deploys drop traffic.
type HealthHandler struct {
	database        HealthChecker
	snapshotManager *config.Manager
	version         string

	shuttingDown atomic.Bool
}

// NewHealthHandler builds the handler.
func NewHealthHandler(database HealthChecker, snapshotManager *config.Manager, version string) *HealthHandler {
	return &HealthHandler{database: database, snapshotManager: snapshotManager, version: version}
}

// BeginShutdown marks the process unready so the load balancer stops routing
// to it before draining starts.
func (h *HealthHandler) BeginShutdown() { h.shuttingDown.Store(true) }

type healthResponse struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}

type readinessResponse struct {
	Status            string  `json:"status"`
	Database          string  `json:"database"`
	ConfigSnapshotAge float64 `json:"configSnapshotAgeSeconds"`
	Detail            string  `json:"detail,omitempty"`
}

// Health reports only that the process is alive.
func (h *HealthHandler) Health(c *echo.Context) error {
	return c.JSON(http.StatusOK, healthResponse{Status: "ok", Version: h.version})
}

// Ready fails when the database pool is unhealthy, the config snapshot is
// stale, or shutdown has begun.
func (h *HealthHandler) Ready(c *echo.Context) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), 2*time.Second)
	defer cancel()

	body := readinessResponse{Status: "ready", Database: "ok"}

	if h.shuttingDown.Load() {
		body.Status = "shutting_down"
		body.Detail = "process is draining"
		return c.JSON(http.StatusServiceUnavailable, body)
	}

	if err := h.database.Healthy(ctx); err != nil {
		body.Status = "not_ready"
		body.Database = "unhealthy"
		body.Detail = "database is not reachable"
		return c.JSON(http.StatusServiceUnavailable, body)
	}

	snapshot := h.snapshotManager.Current()
	if snapshot == nil {
		body.Status = "not_ready"
		body.Detail = "config snapshot has not loaded"
		return c.JSON(http.StatusServiceUnavailable, body)
	}
	body.ConfigSnapshotAge = snapshot.Age().Seconds()
	if snapshot.Age() > MaxSnapshotAge {
		body.Status = "not_ready"
		body.Detail = "config snapshot is stale"
		return c.JSON(http.StatusServiceUnavailable, body)
	}

	return c.JSON(http.StatusOK, body)
}
