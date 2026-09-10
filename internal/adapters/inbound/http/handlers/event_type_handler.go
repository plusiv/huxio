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

// EventTypeHandler serves the event catalogue routes.
type EventTypeHandler struct {
	eventTypeUseCase *usecases.EventTypeUseCase
}

// NewEventTypeHandler builds the handler.
func NewEventTypeHandler(eventTypeUseCase *usecases.EventTypeUseCase) *EventTypeHandler {
	return &EventTypeHandler{eventTypeUseCase: eventTypeUseCase}
}

type createEventTypeRequest struct {
	Name        string          `json:"name"        validate:"required,max=256"`
	Description string          `json:"description" validate:"max=2048"`
	Schemas     json.RawMessage `json:"schemas"`
}

type updateEventTypeRequest struct {
	Description *string         `json:"description" validate:"omitempty,max=2048"`
	Schemas     json.RawMessage `json:"schemas"`
}

type eventTypeResponse struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Schemas     json.RawMessage `json:"schemas,omitempty"`
	Archived    bool            `json:"archived"`
	CreatedAt   time.Time       `json:"createdAt"`
	UpdatedAt   time.Time       `json:"updatedAt"`
}

func newEventTypeResponse(et *entities.EventType) eventTypeResponse {
	return eventTypeResponse{
		Name:        et.Name,
		Description: et.Description,
		Schemas:     et.Schemas,
		Archived:    et.Archived(),
		CreatedAt:   et.CreatedAt,
		UpdatedAt:   et.UpdatedAt,
	}
}

// CreateEventType registers an event type.
func (h *EventTypeHandler) CreateEventType(c *echo.Context) error {
	principal, err := orgToken(c)
	if err != nil {
		return err
	}
	var req createEventTypeRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	et, err := h.eventTypeUseCase.CreateEventType(c.Request().Context(), usecases.CreateEventTypeInput{
		OrgID: principal.OrgID, Name: req.Name, Description: req.Description, Schemas: req.Schemas,
	})
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, newEventTypeResponse(et))
}

// GetEventType returns one event type, archived ones included.
func (h *EventTypeHandler) GetEventType(c *echo.Context) error {
	principal, err := principal(c)
	if err != nil {
		return err
	}
	et, err := h.eventTypeUseCase.GetEventType(c.Request().Context(), principal.OrgID, c.Param("event_type_name"))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, newEventTypeResponse(et))
}

// UpdateEventType writes the mutable fields.
func (h *EventTypeHandler) UpdateEventType(c *echo.Context) error {
	principal, err := orgToken(c)
	if err != nil {
		return err
	}
	var req updateEventTypeRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	et, err := h.eventTypeUseCase.UpdateEventType(c.Request().Context(), principal.OrgID, c.Param("event_type_name"),
		usecases.UpdateEventTypeInput{Description: req.Description, Schemas: req.Schemas})
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, newEventTypeResponse(et))
}

// ArchiveEventType retires an event type. The row stays so historical
// messages referencing it still render.
func (h *EventTypeHandler) ArchiveEventType(c *echo.Context) error {
	principal, err := orgToken(c)
	if err != nil {
		return err
	}
	if err := h.eventTypeUseCase.ArchiveEventType(c.Request().Context(), principal.OrgID, c.Param("event_type_name")); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

// ListEventTypes returns a cursor-paginated page.
func (h *EventTypeHandler) ListEventTypes(c *echo.Context) error {
	principal, err := principal(c)
	if err != nil {
		return err
	}
	page := pagination(c)
	result, err := h.eventTypeUseCase.ListEventTypes(c.Request().Context(), usecases.ListEventTypesInput{
		OrgID:           principal.OrgID,
		IncludeArchived: boolQuery(c, "includeArchived"),
		Cursor:          page.Cursor,
		Limit:           page.Limit,
	})
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, CursorResponse[eventTypeResponse]{
		Data:         utils.Map(result.Items, newEventTypeResponse),
		Iterator:     result.NextCursor,
		PrevIterator: result.PrevCursor,
		Done:         !result.HasMore,
	})
}
