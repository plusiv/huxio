package handlers

import (
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/utils"
)

// EndpointHandler serves the endpoint CRUD, secret and header routes. Every
// route here is reachable by a portal token, so every one of them runs behind
// the app-scope assertion in middleware.
type EndpointHandler struct {
	endpointUseCase *usecases.EndpointUseCase
	appUseCase      *usecases.ApplicationUseCase
}

// NewEndpointHandler builds the handler.
func NewEndpointHandler(
	endpointUseCase *usecases.EndpointUseCase,
	appUseCase *usecases.ApplicationUseCase,
) *EndpointHandler {
	return &EndpointHandler{endpointUseCase: endpointUseCase, appUseCase: appUseCase}
}

type createEndpointRequest struct {
	URL         string            `json:"url"         validate:"required,url,max=2048"`
	Description string            `json:"description" validate:"max=2048"`
	UID         *string           `json:"uid"         validate:"omitempty,max=256"`
	Version     *int              `json:"version"     validate:"omitempty,min=1"`
	RateLimit   *int              `json:"rateLimit"   validate:"omitempty,min=1"`
	FilterTypes []string          `json:"filterTypes" validate:"omitempty,dive,max=256"`
	Channels    []string          `json:"channels"    validate:"omitempty,max=10,dive,max=128"`
	Headers     map[string]string `json:"headers"`
	Secret      string            `json:"secret"      validate:"omitempty,max=512"`
	SecretType  *string           `json:"secretType"  validate:"omitempty,oneof=hmac256 ed25519"`
}

type updateEndpointRequest struct {
	URL         *string  `json:"url"         validate:"omitempty,url,max=2048"`
	Description *string  `json:"description" validate:"omitempty,max=2048"`
	UID         *string  `json:"uid"         validate:"omitempty,max=256"`
	Version     *int     `json:"version"     validate:"omitempty,min=1"`
	RateLimit   *int     `json:"rateLimit"   validate:"omitempty,min=1"`
	FilterTypes []string `json:"filterTypes" validate:"omitempty,dive,max=256"`
	Channels    []string `json:"channels"    validate:"omitempty,max=10,dive,max=128"`
	Disabled    *bool    `json:"disabled"`
}

type rotateSecretRequest struct {
	Key string `json:"key" validate:"omitempty,max=512"`
}

type patchHeadersRequest struct {
	Headers map[string]string `json:"headers" validate:"required"`
}

type endpointResponse struct {
	ID          string    `json:"id"`
	URL         string    `json:"url"`
	Description string    `json:"description"`
	UID         *string   `json:"uid,omitempty"`
	Version     int       `json:"version"`
	RateLimit   *int      `json:"rateLimit,omitempty"`
	FilterTypes []string  `json:"filterTypes,omitempty"`
	Channels    []string  `json:"channels,omitempty"`
	Disabled    bool      `json:"disabled"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type endpointCreatedResponse struct {
	endpointResponse
	// Secret is returned once, at create time.
	Secret string `json:"secret"`
}

type secretResponse struct {
	Key string `json:"key"`
}

type headersResponse struct {
	Headers map[string]string `json:"headers"`
}

func newEndpointResponse(ep *entities.Endpoint) endpointResponse {
	return endpointResponse{
		ID:          ep.ID,
		URL:         ep.URL,
		Description: ep.Description,
		UID:         ep.UID,
		Version:     ep.Version,
		RateLimit:   ep.RateLimit,
		FilterTypes: ep.EventTypes,
		Channels:    ep.Channels,
		Disabled:    ep.Disabled(),
		CreatedAt:   ep.CreatedAt,
		UpdatedAt:   ep.UpdatedAt,
	}
}

// CreateEndpoint registers a destination URL.
func (h *EndpointHandler) CreateEndpoint(c *echo.Context) error {
	principal, err := principal(c)
	if err != nil {
		return err
	}
	app, err := h.appUseCase.GetApplication(c.Request().Context(), principal.OrgID, c.Param("app_id"))
	if err != nil {
		return err
	}

	var req createEndpointRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	var secretType *entities.SecretType
	if req.SecretType != nil {
		secretType = utils.Ptr(entities.SecretType(*req.SecretType))
	}

	ep, secret, err := h.endpointUseCase.CreateEndpoint(c.Request().Context(), usecases.CreateEndpointInput{
		OrgID:       app.OrgID,
		AppID:       app.ID,
		URL:         req.URL,
		Description: req.Description,
		UID:         req.UID,
		Version:     req.Version,
		RateLimit:   req.RateLimit,
		EventTypes:  req.FilterTypes,
		Channels:    req.Channels,
		Headers:     req.Headers,
		Secret:      req.Secret,
		SecretType:  secretType,
	})
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, endpointCreatedResponse{
		endpointResponse: newEndpointResponse(ep),
		Secret:           secret,
	})
}

// GetEndpoint returns one endpoint.
func (h *EndpointHandler) GetEndpoint(c *echo.Context) error {
	appID, err := h.resolveAppID(c)
	if err != nil {
		return err
	}
	ep, err := h.endpointUseCase.GetEndpoint(c.Request().Context(), appID, c.Param("endpoint_id"))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, newEndpointResponse(ep))
}

// ListEndpoints returns a cursor-paginated page.
func (h *EndpointHandler) ListEndpoints(c *echo.Context) error {
	appID, err := h.resolveAppID(c)
	if err != nil {
		return err
	}
	page := pagination(c)
	result, err := h.endpointUseCase.ListEndpoints(c.Request().Context(), usecases.ListEndpointsInput{
		AppID: appID, Cursor: page.Cursor, Limit: page.Limit,
	})
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, CursorResponse[endpointResponse]{
		Data:         utils.Map(result.Items, newEndpointResponse),
		Iterator:     result.NextCursor,
		PrevIterator: result.PrevCursor,
		Done:         !result.HasMore,
	})
}

// UpdateEndpoint applies the supplied fields.
func (h *EndpointHandler) UpdateEndpoint(c *echo.Context) error {
	appID, err := h.resolveAppID(c)
	if err != nil {
		return err
	}
	var req updateEndpointRequest
	if err := c.Bind(&req); err != nil {
		return err
	}

	ep, err := h.endpointUseCase.UpdateEndpoint(c.Request().Context(), appID, c.Param("endpoint_id"),
		usecases.UpdateEndpointInput{
			URL: req.URL, Description: req.Description, UID: req.UID,
			Version: req.Version, RateLimit: req.RateLimit,
			EventTypes: req.FilterTypes, Channels: req.Channels,
			Disabled: req.Disabled,
		})
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, newEndpointResponse(ep))
}

// DeleteEndpoint soft-deletes an endpoint.
func (h *EndpointHandler) DeleteEndpoint(c *echo.Context) error {
	appID, err := h.resolveAppID(c)
	if err != nil {
		return err
	}
	if err := h.endpointUseCase.DeleteEndpoint(c.Request().Context(), appID, c.Param("endpoint_id")); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

// GetSecret reveals the current signing secret.
func (h *EndpointHandler) GetSecret(c *echo.Context) error {
	appID, err := h.resolveAppID(c)
	if err != nil {
		return err
	}
	secret, err := h.endpointUseCase.RevealSecret(c.Request().Context(), appID, c.Param("endpoint_id"))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, secretResponse{Key: secret})
}

// RotateSecret installs a new secret, keeping the previous one signing for the
// overlap window.
func (h *EndpointHandler) RotateSecret(c *echo.Context) error {
	appID, err := h.resolveAppID(c)
	if err != nil {
		return err
	}
	var req rotateSecretRequest
	if err := c.Bind(&req); err != nil {
		return err
	}
	if _, err := h.endpointUseCase.RotateSecret(c.Request().Context(), appID, c.Param("endpoint_id"), req.Key); err != nil {
		return err
	}
	return c.NoContent(http.StatusNoContent)
}

// GetHeaders returns the endpoint's custom headers.
func (h *EndpointHandler) GetHeaders(c *echo.Context) error {
	appID, err := h.resolveAppID(c)
	if err != nil {
		return err
	}
	headers, err := h.endpointUseCase.GetHeaders(c.Request().Context(), appID, c.Param("endpoint_id"))
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, headersResponse{Headers: headers})
}

// PatchHeaders merges headers into the endpoint's set.
func (h *EndpointHandler) PatchHeaders(c *echo.Context) error {
	appID, err := h.resolveAppID(c)
	if err != nil {
		return err
	}
	var req patchHeadersRequest
	if err := c.Bind(&req); err != nil {
		return err
	}
	headers, err := h.endpointUseCase.PatchHeaders(c.Request().Context(), appID, c.Param("endpoint_id"), req.Headers)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, headersResponse{Headers: headers})
}

// resolveAppID maps the path's application segment onto an application id
// inside the caller's tenant, which is also what stops a token addressing
// another tenant's application by uid.
func (h *EndpointHandler) resolveAppID(c *echo.Context) (string, error) {
	principal, err := principal(c)
	if err != nil {
		return "", err
	}
	app, err := h.appUseCase.GetApplication(c.Request().Context(), principal.OrgID, c.Param("app_id"))
	if err != nil {
		return "", err
	}
	return app.ID, nil
}
