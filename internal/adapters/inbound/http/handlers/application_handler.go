package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/utils"
)

// ApplicationHandler serves the application CRUD routes.
type ApplicationHandler struct {
	appUseCase *usecases.ApplicationUseCase
}

// NewApplicationHandler builds the handler.
func NewApplicationHandler(appUseCase *usecases.ApplicationUseCase) *ApplicationHandler {
	return &ApplicationHandler{appUseCase: appUseCase}
}

type createApplicationRequest struct {
	Name      string          `json:"name"      validate:"required,max=256"`
	UID       *string         `json:"uid"       validate:"omitempty,max=256"`
	RateLimit *int            `json:"rateLimit" validate:"omitempty,min=1"`
	Metadata  json.RawMessage `json:"metadata"`
}

type updateApplicationRequest struct {
	Name      *string         `json:"name"      validate:"omitempty,max=256"`
	UID       *string         `json:"uid"       validate:"omitempty,max=256"`
	RateLimit *int            `json:"rateLimit" validate:"omitempty,min=1"`
	Metadata  json.RawMessage `json:"metadata"`
}

type applicationResponse struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	UID       *string         `json:"uid,omitempty"`
	RateLimit *int            `json:"rateLimit,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	CreatedAt time.Time       `json:"createdAt"`
	UpdatedAt time.Time       `json:"updatedAt"`
}

func newApplicationResponse(app *entities.Application) applicationResponse {
	return applicationResponse{
		ID:        app.ID,
		Name:      app.Name,
		UID:       app.UID,
		RateLimit: app.RateLimit,
		Metadata:  app.Metadata,
		CreatedAt: app.CreatedAt,
		UpdatedAt: app.UpdatedAt,
	}
}

// CreateApplication creates an application.
func (h *ApplicationHandler) CreateApplication(c *echo.Context) error {
	principal, err := orgToken(c)
	if err != nil {
		return err
	}
	var req createApplicationRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	app, err := h.appUseCase.CreateApplication(c.Request().Context(), usecases.CreateApplicationInput{
		OrgID: principal.OrgID, Name: req.Name, UID: req.UID,
		RateLimit: req.RateLimit, Metadata: req.Metadata,
	})
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, newApplicationResponse(app))
}

// GetApplication returns one application, addressed by id or uid.
func (h *ApplicationHandler) GetApplication(c *echo.Context) error {
	principal, err := principal(c)
	if err != nil {
		return err
	}
	app, err := h.appUseCase.GetApplication(c.Request().Context(), principal.OrgID, c.Param("app_id"))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, newApplicationResponse(app))
}

// UpdateApplication replaces or patches an application. PUT and PATCH share
// one handler because every field is optional either way.
func (h *ApplicationHandler) UpdateApplication(c *echo.Context) error {
	principal, err := orgToken(c)
	if err != nil {
		return err
	}
	var req updateApplicationRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	app, err := h.appUseCase.UpdateApplication(c.Request().Context(), principal.OrgID, c.Param("app_id"),
		usecases.UpdateApplicationInput{
			Name: req.Name, UID: req.UID, RateLimit: req.RateLimit, Metadata: req.Metadata,
		})
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, newApplicationResponse(app))
}

// DeleteApplication soft-deletes an application.
func (h *ApplicationHandler) DeleteApplication(c *echo.Context) error {
	principal, err := orgToken(c)
	if err != nil {
		return err
	}
	if err := h.appUseCase.DeleteApplication(c.Request().Context(), principal.OrgID, c.Param("app_id")); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

// ListApplications returns a cursor-paginated page.
func (h *ApplicationHandler) ListApplications(c *echo.Context) error {
	principal, err := orgToken(c)
	if err != nil {
		return err
	}
	page := pagination(c)
	result, err := h.appUseCase.ListApplications(c.Request().Context(), usecases.ListApplicationsInput{
		OrgID: principal.OrgID, Search: c.QueryParam("search"),
		Cursor: page.Cursor, Limit: page.Limit,
	})
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, CursorResponse[applicationResponse]{
		Data:         utils.Map(result.Items, newApplicationResponse),
		Iterator:     result.NextCursor,
		PrevIterator: result.PrevCursor,
		Done:         !result.HasMore,
	})
}
