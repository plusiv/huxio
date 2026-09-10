package handlers

import (
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/utils"
)

// AdminHandler serves the operational routes.
type AdminHandler struct {
	adminUseCase *usecases.AdminUseCase
	// pools are the worker pool names the queue routes report on.
	pools []string
}

// NewAdminHandler builds the handler.
func NewAdminHandler(adminUseCase *usecases.AdminUseCase, pools []string) *AdminHandler {
	return &AdminHandler{adminUseCase: adminUseCase, pools: pools}
}

type rescueResponse struct {
	Rescued int64 `json:"rescued"`
}

type queueStatsResponse struct {
	Pool         string  `json:"pool"`
	Ready        int64   `json:"ready"`
	Delayed      int64   `json:"delayed"`
	Locked       int64   `json:"locked"`
	OldestAgeSec float64 `json:"oldestReadyAgeSeconds"`
}

type partitionLeaseResponse struct {
	PartitionKey int16      `json:"partitionKey"`
	Pool         string     `json:"pool"`
	OwnerID      *string    `json:"ownerId,omitempty"`
	HeartbeatAt  *time.Time `json:"heartbeatAt,omitempty"`
}

type poolAssignmentRequest struct {
	AppID      string `json:"appId"      validate:"required,max=256"`
	EndpointID string `json:"endpointId" validate:"required,max=256"`
	Pool       string `json:"pool"       validate:"required,max=128"`
}

type reEnableRequest struct {
	AppID      string `json:"appId"      validate:"required,max=256"`
	EndpointID string `json:"endpointId" validate:"required,max=256"`
}

// RescueStuck forces the stuck-task sweep, for when somebody is watching
// rather than waiting for the maintenance timer.
func (h *AdminHandler) RescueStuck(c *echo.Context) error {
	if _, err := orgToken(c); err != nil {
		return err
	}
	rescued, err := h.adminUseCase.RescueStuck(c.Request().Context())
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, rescueResponse{Rescued: rescued})
}

// QueueStats reports depth and lag per pool.
func (h *AdminHandler) QueueStats(c *echo.Context) error {
	if _, err := orgToken(c); err != nil {
		return err
	}

	pools := h.pools
	if requested := utils.NilIfBlank(c.QueryParam("pool")); requested != nil {
		pools = []string{*requested}
	}

	stats, err := h.adminUseCase.QueueStats(c.Request().Context(), pools)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, utils.Map(stats, func(stat repositories.QueueStats) queueStatsResponse {
		return queueStatsResponse{
			Pool:         stat.Pool,
			Ready:        stat.Ready,
			Delayed:      stat.Delayed,
			Locked:       stat.Locked,
			OldestAgeSec: stat.OldestAge.Seconds(),
		}
	}))
}

// Partitions returns the lease table, for debugging a rebalance.
func (h *AdminHandler) Partitions(c *echo.Context) error {
	if _, err := orgToken(c); err != nil {
		return err
	}

	leases, err := h.adminUseCase.PartitionLeases(c.Request().Context(), c.QueryParam("pool"))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, utils.Map(leases, func(lease entities.PartitionLease) partitionLeaseResponse {
		return partitionLeaseResponse{
			PartitionKey: lease.PartitionKey,
			Pool:         lease.Pool,
			OwnerID:      lease.OwnerID,
			HeartbeatAt:  lease.HeartbeatAt,
		}
	}))
}

// MovePool reassigns an endpoint to another worker pool: dedicated capacity
// for a large tenant, or releasing one from quarantine.
func (h *AdminHandler) MovePool(c *echo.Context) error {
	principal, err := orgToken(c)
	if err != nil {
		return err
	}

	var req poolAssignmentRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	err = h.adminUseCase.MoveEndpointPool(c.Request().Context(), usecases.PoolAssignment{
		OrgID:      principal.OrgID,
		AppIDOrUID: req.AppID,
		EndpointID: req.EndpointID,
		Pool:       req.Pool,
	})
	if err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

// ReEnableEndpoint switches an auto-disabled endpoint back on.
func (h *AdminHandler) ReEnableEndpoint(c *echo.Context) error {
	principal, err := orgToken(c)
	if err != nil {
		return err
	}

	var req reEnableRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	err = h.adminUseCase.ReEnableEndpoint(c.Request().Context(), usecases.PoolAssignment{
		OrgID:      principal.OrgID,
		AppIDOrUID: req.AppID,
		EndpointID: req.EndpointID,
	})
	if err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}
