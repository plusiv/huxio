package portal

import (
	"context"
	"embed"
	"encoding/json"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/plusiv/huxio/internal/application/usecases"
	"github.com/plusiv/huxio/internal/domain/apperrors"
	"github.com/plusiv/huxio/internal/domain/entities"
	"github.com/plusiv/huxio/internal/domain/repositories"
	"github.com/plusiv/huxio/internal/infrastructure/logger"
	"github.com/plusiv/huxio/internal/utils"
	"github.com/rotisserie/eris"
)

// templateFS carries the portal's markup into the binary: no build step, no
// node, nothing to deploy alongside it.
//
//go:embed templates/*.html
var templateFS embed.FS

// pageTemplates holds the parsed templates, one set per page so each can
// define its own content block against the shared layout.
type pageTemplates struct {
	endpoints *template.Template
	endpoint  *template.Template
	log       *template.Template
	tail      *template.Template
}

func parsePages() (*pageTemplates, error) {
	parse := func(name string) (*template.Template, error) {
		return template.New(name).ParseFS(templateFS, "templates/layout.html", "templates/"+name+".html")
	}

	endpoints, err := parse("endpoints")
	if err != nil {
		return nil, eris.Wrap(err, "parse endpoints page")
	}
	// The endpoint page and the log page share the attempts table, so both
	// parse the endpoint template for it.
	endpoint, err := template.New("endpoint").ParseFS(templateFS,
		"templates/layout.html", "templates/endpoint.html")
	if err != nil {
		return nil, eris.Wrap(err, "parse endpoint page")
	}
	logPage, err := template.New("log").ParseFS(templateFS,
		"templates/layout.html", "templates/endpoint.html", "templates/log.html")
	if err != nil {
		return nil, eris.Wrap(err, "parse log page")
	}
	tail, err := parse("tail")
	if err != nil {
		return nil, eris.Wrap(err, "parse tail page")
	}

	return &pageTemplates{endpoints: endpoints, endpoint: endpoint, log: logPage, tail: tail}, nil
}

// Handler serves the portal.
type Handler struct {
	endpointUseCase  *usecases.EndpointUseCase
	attemptUseCase   *usecases.AttemptUseCase
	eventTypeUseCase *usecases.EventTypeUseCase
	ingestUseCase    *usecases.IngestUseCase
	templates        *pageTemplates
	// secure marks the session cookie Secure, which must be on wherever the
	// portal is served over HTTPS.
	secure bool
}

// New builds the portal handler.
func New(
	endpointUseCase *usecases.EndpointUseCase,
	attemptUseCase *usecases.AttemptUseCase,
	eventTypeUseCase *usecases.EventTypeUseCase,
	ingestUseCase *usecases.IngestUseCase,
	secure bool,
) (*Handler, error) {
	templates, err := parsePages()
	if err != nil {
		return nil, err
	}
	return &Handler{
		endpointUseCase:  endpointUseCase,
		attemptUseCase:   attemptUseCase,
		eventTypeUseCase: eventTypeUseCase,
		ingestUseCase:    ingestUseCase,
		templates:        templates,
		secure:           secure,
	}, nil
}

// view is the shape every template renders against.
type view struct {
	Title     string
	Flash     string
	FlashKind string
	Data      any
}

// endpointRow is one row of the endpoint list.
type endpointRow struct {
	ID          string
	URL         string
	Description string
	EventTypes  []string
	Disabled    bool
	Selected    bool
}

// eventTypeOption is one entry in the event-type picker.
type eventTypeOption struct {
	Name        string
	Description string
	Selected    bool
}

// attemptRow is one row of the delivery log.
type attemptRow struct {
	MsgID        string
	When         string
	StatusText   string
	StatusClass  string
	ResponseCode int
	Response     string
	DurationMS   int32
	Attempt      int16
}

// Endpoints renders the endpoint list and the add form.
func (h *Handler) Endpoints(c *echo.Context) error {
	session, err := sessionFrom(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()

	endpoints, err := h.endpointRows(ctx, session, "")
	if err != nil {
		return err
	}
	options, err := h.eventTypeOptions(ctx, session, nil)
	if err != nil {
		return err
	}

	return h.render(c, h.templates.endpoints, "endpoints", view{
		Title:     "Endpoints",
		Flash:     c.QueryParam("flash"),
		FlashKind: flashKind(c),
		Data: map[string]any{
			"Endpoints":     endpoints,
			"EventTypes":    options,
			"EventTypeRows": min(len(options), 8),
		},
	})
}

// CreateEndpoint adds an endpoint from the form.
func (h *Handler) CreateEndpoint(c *echo.Context) error {
	session, err := sessionFrom(c)
	if err != nil {
		return err
	}

	_, _, err = h.endpointUseCase.CreateEndpoint(c.Request().Context(), usecases.CreateEndpointInput{
		OrgID:       session.OrgID,
		AppID:       session.AppID,
		URL:         strings.TrimSpace(c.FormValue("url")),
		Description: strings.TrimSpace(c.FormValue("description")),
		EventTypes:  formEventTypes(c),
	})
	if err != nil {
		return h.redirectWithError(c, "/portal", err)
	}
	return h.redirect(c, "/portal", "Endpoint added. It will start receiving matching events immediately.")
}

// Endpoint renders one endpoint: settings, secret, test send and its log.
func (h *Handler) Endpoint(c *echo.Context) error {
	session, err := sessionFrom(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	endpointID := c.Param("endpoint_id")

	endpoint, err := h.endpointUseCase.GetEndpoint(ctx, session.AppID, endpointID)
	if err != nil {
		return err
	}

	stats, err := h.attemptUseCase.Stats(ctx, session.OrgID, session.AppID, endpoint.ID, nil)
	if err != nil {
		return err
	}
	attempts, next, err := h.attemptRows(ctx, session, endpoint.ID, "", c.QueryParam("cursor"))
	if err != nil {
		return err
	}
	options, err := h.eventTypeOptions(ctx, session, endpoint.EventTypes)
	if err != nil {
		return err
	}

	data := map[string]any{
		"Endpoint": endpointRow{
			ID: endpoint.ID, URL: endpoint.URL, Description: endpoint.Description,
			EventTypes: endpoint.EventTypes, Disabled: endpoint.Disabled(),
		},
		"Stats":         stats,
		"Attempts":      attempts,
		"EventTypes":    options,
		"EventTypeRows": min(len(options), 8),
		"EventTypesCSV": strings.Join(endpoint.EventTypes, ", "),
		"SamplePayload": samplePayload(),
		"NextCursor":    next,
		"NextPageURL":   "/portal/endpoints/" + endpoint.ID + "?cursor=" + next,
	}
	// The secret is only rendered when the viewer explicitly asked for it.
	if c.QueryParam("secret") == "1" {
		secret, err := h.endpointUseCase.RevealSecret(ctx, session.AppID, endpoint.ID)
		if err != nil {
			return err
		}
		data["Secret"] = secret
	}

	return h.render(c, h.templates.endpoint, "endpoint", view{
		Title:     "Endpoint",
		Flash:     c.QueryParam("flash"),
		FlashKind: flashKind(c),
		Data:      data,
	})
}

// UpdateEndpoint saves the settings form, including the enable and disable
// buttons.
func (h *Handler) UpdateEndpoint(c *echo.Context) error {
	session, err := sessionFrom(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	endpointID := c.Param("endpoint_id")
	target := "/portal/endpoints/" + endpointID

	switch c.FormValue("action") {
	case "disable":
		if _, err := h.endpointUseCase.SetDisabled(ctx, session.AppID, endpointID, true); err != nil {
			return h.redirectWithError(c, target, err)
		}
		return h.redirect(c, target, "Endpoint disabled. Nothing will be delivered to it until you enable it again.")

	case "enable":
		if _, err := h.endpointUseCase.SetDisabled(ctx, session.AppID, endpointID, false); err != nil {
			return h.redirectWithError(c, target, err)
		}
		return h.redirect(c, target, "Endpoint enabled.")
	}

	eventTypes := formEventTypes(c)
	_, err = h.endpointUseCase.UpdateEndpoint(ctx, session.AppID, endpointID, usecases.UpdateEndpointInput{
		URL:             utils.Ptr(strings.TrimSpace(c.FormValue("url"))),
		Description:     utils.Ptr(strings.TrimSpace(c.FormValue("description"))),
		EventTypes:      eventTypes,
		ClearEventTypes: len(eventTypes) == 0,
	})
	if err != nil {
		return h.redirectWithError(c, target, err)
	}
	return h.redirect(c, target, "Saved.")
}

// DeleteEndpoint removes an endpoint.
func (h *Handler) DeleteEndpoint(c *echo.Context) error {
	session, err := sessionFrom(c)
	if err != nil {
		return err
	}
	if err := h.endpointUseCase.DeleteEndpoint(c.Request().Context(), session.AppID, c.Param("endpoint_id")); err != nil {
		return h.redirectWithError(c, "/portal", err)
	}
	return h.redirect(c, "/portal", "Endpoint deleted.")
}

// Secret reveals or rotates the signing secret.
func (h *Handler) Secret(c *echo.Context) error {
	session, err := sessionFrom(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	endpointID := c.Param("endpoint_id")
	target := "/portal/endpoints/" + endpointID

	// Resolve inside the session's application before doing anything, so an
	// endpoint this session does not own is refused here rather than by the
	// page it would be redirected to.
	endpoint, err := h.endpointUseCase.GetEndpoint(ctx, session.AppID, endpointID)
	if err != nil {
		return err
	}

	if c.FormValue("action") == "rotate" {
		if _, err := h.endpointUseCase.RotateSecret(ctx, session.AppID, endpoint.ID, ""); err != nil {
			return h.redirectWithError(c, target, err)
		}
		return h.redirect(c, target+"?secret=1",
			"Secret rotated. The previous secret keeps working for 24 hours.")
	}

	// Revealing is a redirect with a flag rather than a rendered secret on a
	// POST response, so a refresh does not re-expose it.
	return c.Redirect(http.StatusSeeOther, target+"?secret=1")
}

// TestEvent sends a sample event to this application, which fans out to every
// matching endpoint including this one.
func (h *Handler) TestEvent(c *echo.Context) error {
	session, err := sessionFrom(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()
	endpointID := c.Param("endpoint_id")
	target := "/portal/endpoints/" + endpointID

	// The test event fans out to every matching endpoint, so the only reason
	// to resolve this one is to confirm the session owns the page it came
	// from.
	if _, err := h.endpointUseCase.GetEndpoint(ctx, session.AppID, endpointID); err != nil {
		return err
	}

	payload := strings.TrimSpace(c.FormValue("payload"))
	if payload == "" {
		payload = samplePayload()
	}
	if !json.Valid([]byte(payload)) {
		return h.redirectWithError(c, target, apperrors.NewValidationError("the payload must be valid JSON"))
	}

	eventType := strings.TrimSpace(c.FormValue("eventType"))
	if eventType == "" {
		eventType = "test.event"
	}

	_, err = h.ingestUseCase.Ingest(ctx, usecases.IngestInput{
		OrgID:      session.OrgID,
		AppIDOrUID: session.AppID,
		EventType:  eventType,
		Payload:    json.RawMessage(payload),
	})
	if err != nil {
		return h.redirectWithError(c, target, err)
	}
	return h.redirect(c, target, "Test event sent. Watch the live tail or refresh in a moment.")
}

// Resend queues one more delivery of a message to this endpoint.
func (h *Handler) Resend(c *echo.Context) error {
	session, err := sessionFrom(c)
	if err != nil {
		return err
	}
	endpointID := c.Param("endpoint_id")
	target := "/portal/endpoints/" + endpointID

	err = h.attemptUseCase.Resend(c.Request().Context(), usecases.ResendInput{
		OrgID:      session.OrgID,
		AppIDOrUID: session.AppID,
		MsgID:      strings.TrimSpace(c.FormValue("msgId")),
		EndpointID: endpointID,
	})
	if err != nil {
		return h.redirectWithError(c, target, err)
	}
	return h.redirect(c, target, "Queued for redelivery.")
}

// Recover replays everything that failed to this endpoint in a window.
func (h *Handler) Recover(c *echo.Context) error {
	session, err := sessionFrom(c)
	if err != nil {
		return err
	}
	endpointID := c.Param("endpoint_id")
	target := "/portal/endpoints/" + endpointID

	hours, err := strconv.Atoi(c.FormValue("hours"))
	if err != nil || hours <= 0 || hours > 24*30 {
		hours = 24
	}

	result, err := h.attemptUseCase.Recover(c.Request().Context(), usecases.RecoverInput{
		OrgID:      session.OrgID,
		AppIDOrUID: session.AppID,
		EndpointID: endpointID,
		Since:      time.Now().UTC().Add(-time.Duration(hours) * time.Hour),
	})
	if err != nil {
		return h.redirectWithError(c, target, err)
	}

	// Only deliveries whose most recent attempt failed for good are replayed:
	// one that still has a retry scheduled would be delivered twice.
	message := "Nothing needed replaying. Deliveries that are still retrying will be attempted again on their own."
	if result.Enqueued > 0 {
		message = "Queued " + strconv.Itoa(result.Enqueued) + " deliveries for replay."
	}
	if result.Truncated {
		message += " More remain; run it again to continue."
	}
	return h.redirect(c, target, message)
}

// Log renders the delivery log across endpoints.
func (h *Handler) Log(c *echo.Context) error {
	session, err := sessionFrom(c)
	if err != nil {
		return err
	}
	ctx := c.Request().Context()

	endpointID := c.QueryParam("endpointId")
	endpoints, err := h.endpointRows(ctx, session, endpointID)
	if err != nil {
		return err
	}
	attempts, next, err := h.attemptRows(ctx, session, endpointID, c.QueryParam("status"), c.QueryParam("cursor"))
	if err != nil {
		return err
	}

	nextURL := "/portal/log?cursor=" + next
	if endpointID != "" {
		nextURL += "&endpointId=" + endpointID
	}
	if status := c.QueryParam("status"); status != "" {
		nextURL += "&status=" + status
	}

	return h.render(c, h.templates.log, "log", view{
		Title:     "Delivery log",
		Flash:     c.QueryParam("flash"),
		FlashKind: flashKind(c),
		Data: map[string]any{
			"Endpoints":   endpoints,
			"Attempts":    attempts,
			"Status":      c.QueryParam("status"),
			"NextCursor":  next,
			"NextPageURL": nextURL,
			// The shared attempts table links resend at the endpoint it is rendered
			// under; the log view has no single endpoint, so the link target is the
			// attempt's own endpoint.
			"Endpoint": endpointRow{ID: endpointID},
		},
	})
}

// Tail renders the live tail page.
func (h *Handler) Tail(c *echo.Context) error {
	if _, err := sessionFrom(c); err != nil {
		return err
	}
	return h.render(c, h.templates.tail, "tail", view{Title: "Live tail"})
}

// SignOut clears the session cookie.
func (h *Handler) SignOut(c *echo.Context) error {
	clearSessionCookie(c, h.secure)
	return c.Redirect(http.StatusSeeOther, "/portal")
}

// render writes a page, refusing to send a half-rendered one: a template
// error must not produce a broken page with a 200 on it.
func (h *Handler) render(c *echo.Context, tmpl *template.Template, name string, data view) error {
	var buffer strings.Builder
	if err := tmpl.ExecuteTemplate(&buffer, name, data); err != nil {
		return apperrors.NewInternalError(eris.Wrap(err, "render portal page"))
	}
	return c.HTML(http.StatusOK, buffer.String())
}

// redirect completes a form post with a flash message, so a refresh does not
// repeat the action.
func (h *Handler) redirect(c *echo.Context, target, message string) error {
	return c.Redirect(http.StatusSeeOther, withFlash(target, message, "ok"))
}

// redirectWithError shows a client-safe message and logs anything internal.
func (h *Handler) redirectWithError(c *echo.Context, target string, err error) error {
	var appErr *apperrors.AppError
	if eris.As(err, &appErr) && appErr.Kind != apperrors.KindInternal {
		return c.Redirect(http.StatusSeeOther, withFlash(target, appErr.Message, "bad"))
	}
	logger.FromContext(c.Request().Context()).Error().Err(err).Msg("portal action failed")
	return c.Redirect(http.StatusSeeOther, withFlash(target, "Something went wrong. Please try again.", "bad"))
}

// urlEncode escapes a flash message for the query string.
func urlEncode(value string) string { return url.QueryEscape(value) }

func withFlash(target, message, kind string) string {
	separator := "?"
	if strings.Contains(target, "?") {
		separator = "&"
	}
	values := make([]string, 0, 2)
	values = append(values, "flash="+urlEncode(message))
	if kind != "" {
		values = append(values, "flashKind="+kind)
	}
	return target + separator + strings.Join(values, "&")
}

func flashKind(c *echo.Context) string {
	if kind := c.QueryParam("flashKind"); kind == "bad" {
		return "bad"
	}
	return "ok"
}

// endpointRows lists this application's endpoints for the list and the filter.
func (h *Handler) endpointRows(ctx context.Context, session session, selected string) ([]endpointRow, error) {
	result, err := h.endpointUseCase.ListEndpoints(ctx, usecases.ListEndpointsInput{
		AppID: session.AppID, Limit: repositories.MaxPageSize,
	})
	if err != nil {
		return nil, err
	}

	rows := make([]endpointRow, 0, len(result.Items))
	for _, endpoint := range result.Items {
		rows = append(rows, endpointRow{
			ID: endpoint.ID, URL: endpoint.URL, Description: endpoint.Description,
			EventTypes: endpoint.EventTypes, Disabled: endpoint.Disabled(),
			Selected: endpoint.ID == selected,
		})
	}
	return rows, nil
}

// attemptRows reads a page of the delivery log.
func (h *Handler) attemptRows(
	ctx context.Context,
	session session,
	endpointID, status, cursor string,
) ([]attemptRow, string, error) {
	filters := usecases.AttemptFilterInput{}
	if status != "" {
		parsed, err := parseStatus(status)
		if err != nil {
			return nil, "", err
		}
		filters.Status = &parsed
	}

	result, err := h.attemptUseCase.ListAttempts(ctx, usecases.ListAttemptsInput{
		OrgID:      session.OrgID,
		AppIDOrUID: session.AppID,
		EndpointID: endpointID,
		Filters:    filters,
		Cursor:     cursor,
		Limit:      25,
	})
	if err != nil {
		return nil, "", err
	}

	rows := make([]attemptRow, 0, len(result.Items))
	for _, attempt := range result.Items {
		rows = append(rows, attemptRow{
			MsgID:        attempt.MsgID,
			When:         attempt.CreatedAt.Local().Format("2006-01-02 15:04:05"),
			StatusText:   statusText(attempt.Status),
			StatusClass:  statusClass(attempt.Status),
			ResponseCode: int(attempt.ResponseStatusCode),
			Response:     utils.Truncate(attempt.ResponseBody, 160),
			DurationMS:   attempt.ResponseDurationMS,
			Attempt:      attempt.AttemptNumber,
		})
	}
	return rows, utils.Deref(result.NextCursor, ""), nil
}

// eventTypeOptions builds the subscription picker from the tenant's catalogue.
func (h *Handler) eventTypeOptions(ctx context.Context, session session, selected []string) ([]eventTypeOption, error) {
	result, err := h.eventTypeUseCase.ListEventTypes(ctx, usecases.ListEventTypesInput{
		OrgID: session.OrgID, Limit: repositories.MaxPageSize,
	})
	if err != nil {
		return nil, err
	}

	chosen := make(map[string]struct{}, len(selected))
	for _, name := range selected {
		chosen[name] = struct{}{}
	}

	options := make([]eventTypeOption, 0, len(result.Items))
	for _, eventType := range result.Items {
		_, isSelected := chosen[eventType.Name]
		options = append(options, eventTypeOption{
			Name:        eventType.Name,
			Description: eventType.Description,
			Selected:    isSelected,
		})
	}
	return options, nil
}

// formEventTypes reads the picker, accepting both a multi-select and a plain
// comma-separated field for tenants with no registered event types.
func formEventTypes(c *echo.Context) []string {
	values := c.Request().Form["eventTypes"]
	if len(values) == 1 && strings.Contains(values[0], ",") {
		values = strings.Split(values[0], ",")
	}

	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func parseStatus(raw string) (entities.AttemptStatus, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "success":
		return entities.AttemptSucceeded, nil
	case "pending":
		return entities.AttemptPendingRetry, nil
	case "fail":
		return entities.AttemptFailed, nil
	default:
		return 0, apperrors.NewValidationError("unknown status filter")
	}
}

func statusText(status entities.AttemptStatus) string {
	switch status {
	case entities.AttemptSucceeded:
		return "delivered"
	case entities.AttemptPendingRetry:
		return "pending retry"
	default:
		return "failed"
	}
}

func statusClass(status entities.AttemptStatus) string {
	switch status {
	case entities.AttemptSucceeded:
		return "ok"
	case entities.AttemptPendingRetry:
		return "warn"
	default:
		return "bad"
	}
}

func samplePayload() string {
	return "{\n  \"hello\": \"world\",\n  \"sentFrom\": \"the portal\"\n}"
}

// streamAttempt is the event payload the tail's JavaScript reads. It is
// deliberately the same shape the API's attempt listing returns, so the two
// views cannot drift.
type streamAttempt struct {
	ID                 string    `json:"id"`
	Timestamp          time.Time `json:"timestamp"`
	MsgID              string    `json:"msgId"`
	EndpointID         string    `json:"endpointId"`
	Status             int       `json:"status"`
	StatusText         string    `json:"statusText"`
	ResponseStatusCode int       `json:"responseStatusCode"`
	ResponseDurationMS int32     `json:"responseDurationMs"`
	AttemptNumber      int16     `json:"attemptNumber"`
}

// jsonEvent encodes one attempt for the event stream.
func jsonEvent(attempt *entities.DeliveryAttempt) ([]byte, error) {
	return json.Marshal(streamAttempt{
		ID:                 attempt.ID,
		Timestamp:          attempt.CreatedAt,
		MsgID:              attempt.MsgID,
		EndpointID:         attempt.EndpointID,
		Status:             int(attempt.Status),
		StatusText:         statusText(attempt.Status),
		ResponseStatusCode: int(attempt.ResponseStatusCode),
		ResponseDurationMS: attempt.ResponseDurationMS,
		AttemptNumber:      attempt.AttemptNumber,
	})
}
