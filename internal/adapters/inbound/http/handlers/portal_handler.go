package handlers

import (
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/application/usecases"
)

// PortalHandler mints portal access tokens.
type PortalHandler struct {
	portalUseCase *usecases.PortalUseCase
}

// NewPortalHandler builds the handler.
func NewPortalHandler(portalUseCase *usecases.PortalUseCase) *PortalHandler {
	return &PortalHandler{portalUseCase: portalUseCase}
}

type portalAccessRequest struct {
	// ExpirySeconds overrides the configured token lifetime.
	ExpirySeconds *int `json:"expiry" validate:"omitempty,min=1,max=86400"`
}

// PortalAccess mints a short-lived token scoped to one application. The
// sender's backend calls this and redirects or iframes the result.
func (h *PortalHandler) PortalAccess(c *echo.Context) error {
	principal, err := orgToken(c)
	if err != nil {
		return err
	}

	var req portalAccessRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	var ttl *time.Duration
	if req.ExpirySeconds != nil {
		lifetime := time.Duration(*req.ExpirySeconds) * time.Second
		ttl = &lifetime
	}

	access, err := h.portalUseCase.Grant(c.Request().Context(), principal.OrgID, c.Param("app_id"), ttl)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, access)
}
